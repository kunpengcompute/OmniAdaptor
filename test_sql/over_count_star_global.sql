-- =====================================================================
-- 用例 2: COUNT(*) OVER (ORDER BY proctime) —— 无 PARTITION BY 的全局窗口
-- 验证空 grouping = 单一全局分区（C++ 侧 grouping=[] 路径）
--
-- 前置: 与用例 1 共用 /tmp/over_bid.csv（无需另建数据）
--
-- 预期输出（全局累计行数, 不分 bidder）:
--   (7,10,1) (8,5,2) (7,20,3) (8,15,4) (7,30,5) (8,35,6)
-- 日志验收同用例 1: injected + SUITABLE 两行都要出现。
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
  row_count BIGINT
) WITH (
  'connector' = 'print'
);

-- ===== 第一段: 执行计划测试 =====
-- EXPLAIN
-- INSERT INTO sink
-- SELECT
--   bidder,
--   price,
--   COUNT(*) OVER (ORDER BY proctime) AS row_count
-- FROM bid;

-- ===== 第二段: 计算结果测试 =====
INSERT INTO sink
SELECT
  bidder,
  price,
  COUNT(*) OVER (ORDER BY proctime) AS row_count
FROM bid;
