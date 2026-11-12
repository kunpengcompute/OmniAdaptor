-- =====================================================================
-- 用例 3（回退对照）: SUM(1) OVER (PARTITION BY bidder ORDER BY proctime)
-- 常量参数 1 —— planner 会把它作为 constants(常量列) 传入 OverAggregate,
-- v1 白名单要求聚合参数来自输入列（非常量）—— 必须自动回退 Java 路径
--
-- 前置: 与用例 1 共用 /tmp/over_bid.csv
--
-- 为什么不用 AVG 做回退用例: planner 会把 AVG(x) 重写成
--   COUNT(x) + $SUM0(x) 两个聚合 + 外层 Calc 做除法,
-- 它的 OverAggregate 节点和 SUM 的完全相同(都在白名单内, 正确 native 化)。
-- 所以真正的回退用例改用常量参数 SUM(1)。
--
-- 验收点（与用例 1 相反的方向）:
--   1) 执行计划里 OverAggregate 节点照常出现（planner 层无差别）
--      —— 但日志中【不】出现 "OverAggregate native description injected"
--      （planner patch 判定 not eligible, 不注入 JSON）
--   2) 日志 grep "the operator OverAggregate" + "NOT SUITABLE"（整链回退 Java）
--   3) 输出仍然正确（走原生 Flink Java 算子）
-- 预期输出（每分区的行号前缀和, SUM(1) 累加 = 数行）:
--   (7,10,1) (8,5,1) (7,20,2) (8,15,2) (7,30,3) (8,35,3)
-- 这个用例证明: 白名单外的 OVER 不会被误 native 化, 回退后结果保持一致。
-- =====================================================================

SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default' = '1';
SET 'table.exec.mini-batch.enabled' = 'false';

CREATE TABLE bid (
  bidder BIGINT,
  price BIGINT,
  proctime AS PROCTIME()
) WITH (
  'connector' = 'filesystem',
  'path' = '/tmp/over_bid.csv',
  'format' = 'csv'
);

CREATE TABLE sink (
  bidder BIGINT,
  price BIGINT,
  running_sum BIGINT
) WITH (
  'connector' = 'print'
);

-- ===== 第一段: 执行计划测试（计划里 OverAggregate 节点照常出现）=====
-- EXPLAIN
-- INSERT INTO sink
-- SELECT
--   bidder,
--   price,
--   SUM(1) OVER (PARTITION BY bidder ORDER BY proctime) AS running_sum
-- FROM bid;

-- ===== 第二段: 计算结果测试（回退 Java, 输出仍须正确）=====
INSERT INTO sink
SELECT
  bidder,
  price,
  SUM(1) OVER (PARTITION BY bidder ORDER BY proctime) AS running_sum
FROM bid;
