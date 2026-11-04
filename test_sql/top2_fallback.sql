-- =====================================================================
-- 非白名单 UDF 自动回退验证（负向）
-- 对应任务书："仅针对特定UDF做适配，其他UDF回退"
--
-- 前置：把 Top2.java 复制改名为 Top3.java（逻辑完全不变）编进作业 jar。
-- 预期：
--   1. 计划/日志显示不替换：
--      "GroupTableAggregate falls back to Java: plan has no native processFunction"
--   2. 作业照常运行，输出与 top2_basic.sql 一致（Top3 逻辑与 Top2 相同）
-- =====================================================================

SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default' = '1';
SET 'table.exec.mini-batch.enabled' = 'false';

CREATE FUNCTION top3 AS 'com.omnistream.udf.Top3';

CREATE TABLE top2_fallback_result (
    bidder BIGINT,
    `value`  BIGINT,
    `rank` BIGINT
) WITH (
  'connector' = 'print'
);

EXPLAIN
INSERT INTO top2_fallback_result
SELECT
    bidder,
    `value`,
    `rank`
FROM bid,
     LATERAL TABLE(top3(CAST(price AS BIGINT))) AS T(`value`, `rank`)
GROUP BY bidder;
