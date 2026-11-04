-- =====================================================================
-- 无键模式"同值出现两次就排两次"验证
-- 对应 C++ 用例：NoUniqueKeyDuplicateValueRanksTwice
--
-- 思路：不构造特殊数据——bid 流里同一拍卖品(auction)天然会被同一出价人
-- 多次出价，因此 top2(auction) 的参数值必然重复出现。若重复值到达两次，
-- 无键模式下应占据榜单两个名次（与 Flink 原生 Top2 语义一致），
-- 双引擎输出仍须逐字节一致。
-- =====================================================================

SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default' = '1';
SET 'table.exec.mini-batch.enabled' = 'false';

CREATE FUNCTION top2 AS 'com.omnistream.udf.Top2';

CREATE TABLE top2_dup_result (
    bidder BIGINT,
    `value`  BIGINT,
    `rank` BIGINT
) WITH (
  'connector' = 'print'
);

EXPLAIN
INSERT INTO top2_dup_result
SELECT
    bidder,
    `value`,
    `rank`
FROM bid,
     LATERAL TABLE(top2(auction)) AS T(`value`, `rank`)
GROUP BY bidder;
