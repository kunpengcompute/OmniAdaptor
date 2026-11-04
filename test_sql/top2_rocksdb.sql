-- =====================================================================
-- RocksDB 状态后端一致性验证
-- 对应 C++ 用例：DualBackendConsistency（Heap 与 RocksDB 两个状态后端
-- 的行为必须一致——RocksDB MapState 的 entries 缓存失效问题正是由此用例抓出）
--
-- 用法：与 top2_basic.sql 完全相同的 SQL，仅切换状态后端为 rocksdb。
-- 预期：输出与 top2_basic.sql（默认 Heap 后端）逐字节一致。
-- 注意：SQL 的 sink 表名不同，便于区分两份 flink_output。
-- =====================================================================

SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default' = '1';
SET 'table.exec.mini-batch.enabled' = 'false';
SET 'state.backend' = 'rocksdb';

CREATE FUNCTION top2 AS 'com.omnistream.udf.Top2';

CREATE TABLE top2_rocksdb_result (
    bidder BIGINT,
    `value`  BIGINT,
    `rank` BIGINT
) WITH (
  'connector' = 'print'
);

EXPLAIN
INSERT INTO top2_rocksdb_result
SELECT
    bidder,
    `value`,
    `rank`
FROM bid,
     LATERAL TABLE(top2(CAST(price AS BIGINT))) AS T(`value`, `rank`)
GROUP BY bidder;
