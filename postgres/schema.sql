-- postgres/schema.sql
-- -----------------------------------------------------------------------------
-- Run once against the `finance` database (local docker-compose Postgres, or
-- the Azure Database for PostgreSQL Flexible Server created by
-- azure/deploy_infra.sh).
--
--   psql -h localhost -U finance_user -d finance -f postgres/schema.sql
-- -----------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS staging;

-- Raw tick landing table (optional - useful for local testing without the
-- full Databricks medallion path; the load_to_postgres.py script can write
-- here directly from local Spark output).
CREATE TABLE IF NOT EXISTS staging.raw_ticks (
    id              BIGSERIAL PRIMARY KEY,
    symbol          VARCHAR(10)      NOT NULL,
    price           NUMERIC(12, 4)   NOT NULL,
    open_price      NUMERIC(12, 4),
    day_high        NUMERIC(12, 4),
    day_low         NUMERIC(12, 4),
    prev_close      NUMERIC(12, 4),
    volume          BIGINT,
    currency        VARCHAR(8),
    event_time      TIMESTAMPTZ      NOT NULL,
    source          VARCHAR(32),
    inserted_at     TIMESTAMPTZ      DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_raw_ticks_symbol_time ON staging.raw_ticks (symbol, event_time DESC);

-- Gold layer: one row per symbol per trading day. This is what ADF's Copy
-- activity upserts into, and what Power BI connects to directly.
CREATE TABLE IF NOT EXISTS public.gold_daily_metrics (
    symbol          VARCHAR(10)      NOT NULL,
    trade_date      DATE             NOT NULL,
    open_price      NUMERIC(12, 4),
    high_price      NUMERIC(12, 4),
    low_price       NUMERIC(12, 4),
    close_price     NUMERIC(12, 4),
    total_volume    BIGINT,
    avg_price       NUMERIC(12, 4),
    volatility      NUMERIC(12, 6),
    pct_change      NUMERIC(8, 4),
    rsi_14          NUMERIC(6, 2),
    loaded_at       TIMESTAMPTZ      DEFAULT now(),
    PRIMARY KEY (symbol, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_gold_symbol_date ON public.gold_daily_metrics (symbol, trade_date DESC);

-- Convenience view Power BI can use directly for a "top movers today" tile.
CREATE OR REPLACE VIEW public.vw_top_movers_latest AS
SELECT symbol, trade_date, close_price, pct_change, rsi_14
FROM public.gold_daily_metrics
WHERE trade_date = (SELECT MAX(trade_date) FROM public.gold_daily_metrics)
ORDER BY ABS(pct_change) DESC;

-- Application role used by ADF / the loader script. Tighten privileges as needed.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'finance_app') THEN
        CREATE ROLE finance_app LOGIN PASSWORD 'change_me_too';
    END IF;
END
$$;

GRANT USAGE ON SCHEMA staging, public TO finance_app;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA staging, public TO finance_app;
