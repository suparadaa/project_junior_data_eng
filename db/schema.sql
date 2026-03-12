CREATE TABLE clients (
    client_id VARCHAR PRIMARY KEY,
    client_name TEXT,
    country TEXT,
    kyc_status TEXT,
    created_at DATE
);

CREATE TABLE instruments (
    instrument_id VARCHAR PRIMARY KEY,
    symbol TEXT,
    asset_class TEXT,
    currency TEXT,
    exchange TEXT
);

CREATE TABLE trades (
    trade_id VARCHAR PRIMARY KEY,
    trade_time TIMESTAMP,
    client_id VARCHAR,
    instrument_id VARCHAR,
    side VARCHAR,
    quantity NUMERIC,
    price NUMERIC,
    fees NUMERIC,
    status VARCHAR
);