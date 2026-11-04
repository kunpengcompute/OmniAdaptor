-- =====================================================================
-- GroupTableAggregate 主验收场景（正向）
-- 对应 C++ 用例：MultiRowOutputCorrectness / FirstRowPath /
--               UpdateBeforeOrdering / NoUpdateBefore / NoUniqueKeyMatchesFlinkTop2
--
-- 用法（参考组内算子测试流程）：
--   1. 先带 EXPLAIN 跑：确认执行计划中出现 GroupTableAggregate 算子
--   2. 注释掉 EXPLAIN 行再跑：日志确认替换生效，print 输出与
--      原生 Flink 跑同一条 SQL 的输出逐字节比对
-- 注：bid 表由环境注册（同 group.sql），Nexmark 出价流；
--     price 为 DECIMAL(23,3)，v1 native 排序键仅支持 BIGINT，故 CAST。
-- =====================================================================

SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default' = '1';
SET 'table.exec.mini-batch.enabled' = 'false';

CREATE FUNCTION top2 AS 'com.omnistream.udf.Top2';

CREATE TABLE top2_result (
    bidder BIGINT,
    `value`  BIGINT,
    `rank` BIGINT
) WITH (
  'connector' = 'print'
);

EXPLAIN
INSERT INTO top2_result
SELECT
    bidder,
    `value`,
    `rank`
FROM bid,
     LATERAL TABLE(top2(CAST(price AS BIGINT))) AS T(`value`, `rank`)
GROUP BY bidder;
