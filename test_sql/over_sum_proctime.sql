-- =====================================================================
-- 用例 1（主验收）: SUM(price) OVER (PARTITION BY bidder ORDER BY proctime)
-- 前缀聚合 + 分区隔离 —— M2 path-A ProcTimeUnboundedPrecedingFunction
--
-- 前置: 把同目录 over_bid.csv 上传到执行机 /tmp/over_bid.csv
--   (6 行交错数据: bidder=7 价格 10/20/30, bidder=8 价格 5/15/35)
--
-- ---------- 第一段: 执行计划测试 ----------
-- 预期计划含 OverAggregate 算子, 且窗口形状必须是
--   RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
-- （proc-time + 不写 frame 的默认形状 = v1 白名单唯一支持的形状）
--
-- ---------- 第二段: 计算结果测试 ----------
-- 注释掉 EXPLAIN 后真跑, 日志验收三点:
--   1) grep "OverAggregate native description injected"  (planner patch 注入 JSON)
--   2) grep "the operator OverAggregate" + "SUITABLE"    (OmniGraphOverride 放行)
--   3) /tmp/flink_output.txt 与原生 Flink 同 SQL 同数据输出一致
-- 预期前缀和输出（按到达顺序交错）:
--   (7,10,10) (8,5,5) (7,20,30) (8,15,20) (7,30,60) (8,35,55)
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

-- ===== 第一段: 执行计划测试 (先跑这段, 确认计划含 OverAggregate) =====
-- EXPLAIN
-- INSERT INTO sink
-- SELECT
--   bidder,
--   price,
--   SUM(price) OVER (PARTITION BY bidder ORDER BY proctime) AS running_sum
-- FROM bid;

-- ===== 第二段: 计算结果测试 (注释掉上面的 EXPLAIN 后整文件重跑) =====
INSERT INTO sink
SELECT
  bidder,
  price,
  SUM(price) OVER (PARTITION BY bidder ORDER BY proctime) AS running_sum
FROM bid;
