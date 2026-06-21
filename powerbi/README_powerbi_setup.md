# Power BI Setup

This pipeline's gold layer lands in `public.gold_daily_metrics` in PostgreSQL,
which Power BI can connect to directly — no flat-file exports needed.

## 1. Connect Power BI Desktop to PostgreSQL

1. Get Data → PostgreSQL database
2. Server: `<your-postgres-server>.postgres.database.azure.com` (or `localhost`
   on port `5432` if running locally via docker-compose)
3. Database: `finance`
4. Authentication: Database, using the `finance_app` role created in `schema.sql`
5. Import (or DirectQuery if you want the dashboard to always reflect the
   latest nightly load without a manual refresh)

Select `public.gold_daily_metrics` and `public.vw_top_movers_latest`.

## 2. Suggested dashboard pages

**Page 1 — Market Overview**
- Card visuals: total symbols tracked, latest trade date, average market-wide % change
- Line chart: `close_price` over `trade_date`, one line per `symbol` (use a slicer for symbol selection)
- Bar chart: `vw_top_movers_latest`, sorted by `pct_change`, to show today's biggest movers

**Page 2 — Technical Indicators**
- Line chart: `rsi_14` over time per symbol, with reference lines at 30 and 70
  (oversold/overbought thresholds)
- Scatter plot: `volatility` (x) vs `pct_change` (y), bubble size = `total_volume`

**Page 3 — Symbol Deep Dive**
- Candlestick-style visual (use the "Stock Chart" custom visual from
  AppSource) fed by `open_price` / `high_price` / `low_price` / `close_price`
- Table of raw daily metrics for the selected symbol

## 3. Useful DAX measures

```dax
Daily Return % = AVERAGE(gold_daily_metrics[pct_change])

RSI Status =
VAR latestRSI = MAX(gold_daily_metrics[rsi_14])
RETURN
    SWITCH(
        TRUE(),
        latestRSI >= 70, "Overbought",
        latestRSI <= 30, "Oversold",
        "Neutral"
    )

7-Day Volatility (Avg) =
CALCULATE(
    AVERAGE(gold_daily_metrics[volatility]),
    DATESINPERIOD(gold_daily_metrics[trade_date], MAX(gold_daily_metrics[trade_date]), -7, DAY)
)
```

## 4. Scheduled refresh (cloud deployment)

If publishing to the Power BI Service:
1. Install/configure the On-premises Data Gateway only if Postgres isn't
   publicly reachable; Azure Database for PostgreSQL Flexible Server with a
   public endpoint doesn't need it.
2. In the Power BI Service dataset settings, set a scheduled refresh aligned
   to run **after** the ADF nightly pipeline finishes (e.g. ADF trigger at
   02:00 UTC → Power BI refresh at 02:30 UTC).
