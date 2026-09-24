-- Q15-like: daily distinct bidders by auction category.
-- This is a self-contained smoke/performance case for Flink 1.16.3 + Omni.

SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default' = '2';
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

CREATE TABLE bids (
    auction_id BIGINT,
    bidder BIGINT,
    category BIGINT,
    bid_time AS PROCTIME()
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '100',
    'fields.auction_id.kind' = 'random',
    'fields.auction_id.min' = '1',
    'fields.auction_id.max' = '100000',
    'fields.bidder.kind' = 'random',
    'fields.bidder.min' = '1',
    'fields.bidder.max' = '10000',
    'fields.category.kind' = 'random',
    'fields.category.min' = '1',
    'fields.category.max' = '20'
);

CREATE TABLE q15_sink (
    category BIGINT,
    distinct_bidders BIGINT
) WITH (
    'connector' = 'print'
);

INSERT INTO q15_sink
SELECT
    category,
    COUNT(DISTINCT bidder) AS distinct_bidders
FROM TABLE(
    TUMBLE(TABLE bids, DESCRIPTOR(bid_time), INTERVAL '1' DAY)
)
GROUP BY window_start, window_end, category;
