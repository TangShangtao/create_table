
client.command(
'''
CREATE TABLE if not exists marketdata.stock_1minute_bars
(
    `timestamp` DateTime64 CODEC(Delta, ZSTD),
    `symbol` LowCardinality(String),
    `open` Float32 DEFAULT -1 CODEC(Delta, ZSTD),
    `high` Float32 DEFAULT -1 CODEC(Delta, ZSTD),
    `low` Float32 DEFAULT -1 CODEC(Delta, ZSTD),
    `close` Float32 DEFAULT -1 CODEC(Delta, ZSTD),
    `volume` Float64 DEFAULT -1,
    `turnover` Float64 DEFAULT -1,
)
ENGINE = MergeTree
ORDER BY (timestamp,symbol)
'''
)