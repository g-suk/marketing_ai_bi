/*=============================================================================
  Summit Gear Co. -- Marketing AI+BI Lab
  04_ml/01_forecast.sql

  Trains SNOWFLAKE.ML.FORECAST models and materializes results.

  Models:
    1. Single-series: Total daily revenue (executive overview)
    2. Multi-series: Revenue by product_category x channel (12 series)
    3. Multi-series: Marketing spend by sub_channel (6 series, excl distributor_event)

  Results stored in: FORECAST_RESULTS, FORECAST_FEATURE_IMPORTANCE
=============================================================================*/

USE DATABASE MARKETING_AI_BI;
USE SCHEMA MARKETING_ANALYTICS;
USE WAREHOUSE COMPUTE_WH;

----------------------------------------------------------------------
-- Result tables
----------------------------------------------------------------------
CREATE OR REPLACE TABLE FORECAST_RESULTS (
    model_name VARCHAR(50),
    series VARCHAR(50),
    ts TIMESTAMP_NTZ,
    forecast FLOAT,
    lower_bound FLOAT,
    upper_bound FLOAT
);

----------------------------------------------------------------------
-- Training / holdout split: train on first 18 months, forecast last 3
----------------------------------------------------------------------
CREATE OR REPLACE VIEW V_FORECAST_TRAINING AS
SELECT ds, series, y
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_FORECAST_INPUT
WHERE ds < '2026-01-01';

----------------------------------------------------------------------
-- 1. Single-series FORECAST: Total daily revenue
----------------------------------------------------------------------
CREATE OR REPLACE VIEW V_FORECAST_TOTAL_TRAINING AS
SELECT ds, SUM(y) AS y
FROM V_FORECAST_TRAINING
GROUP BY ds;

CREATE OR REPLACE SNOWFLAKE.ML.FORECAST FORECAST_TOTAL_REVENUE(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'V_FORECAST_TOTAL_TRAINING'),
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME => 'Y'
);

INSERT INTO FORECAST_RESULTS
SELECT 'total_revenue' AS model_name, 'Total' AS series, ts, forecast, lower_bound, upper_bound
FROM TABLE(FORECAST_TOTAL_REVENUE!FORECAST(
    FORECASTING_PERIODS => 90
));

----------------------------------------------------------------------
-- 2. Multi-series FORECAST: Revenue by product_category x channel
----------------------------------------------------------------------
CREATE OR REPLACE SNOWFLAKE.ML.FORECAST FORECAST_BY_PRODUCT_CHANNEL(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'V_FORECAST_TRAINING'),
    SERIES_COLNAME => 'SERIES',
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME => 'Y'
);

INSERT INTO FORECAST_RESULTS
SELECT
    'product_channel' AS model_name,
    series,
    ts,
    forecast,
    lower_bound,
    upper_bound
FROM TABLE(FORECAST_BY_PRODUCT_CHANNEL!FORECAST(
    FORECASTING_PERIODS => 90
));

----------------------------------------------------------------------
-- 3. Multi-series FORECAST: Marketing spend by sub_channel
----------------------------------------------------------------------
CREATE OR REPLACE VIEW V_SPEND_FORECAST_TRAINING AS
SELECT ds, series, y
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_SPEND_FORECAST_INPUT
WHERE ds < '2026-01-01';

CREATE OR REPLACE SNOWFLAKE.ML.FORECAST FORECAST_SPEND_BY_SUBCHANNEL(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'V_SPEND_FORECAST_TRAINING'),
    SERIES_COLNAME => 'SERIES',
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME => 'Y'
);

INSERT INTO FORECAST_RESULTS
SELECT
    'spend_subchannel' AS model_name,
    series,
    ts,
    forecast,
    lower_bound,
    upper_bound
FROM TABLE(FORECAST_SPEND_BY_SUBCHANNEL!FORECAST(
    FORECASTING_PERIODS => 90
));

----------------------------------------------------------------------
-- Feature importance (from product x channel model)
----------------------------------------------------------------------
CREATE OR REPLACE TABLE FORECAST_FEATURE_IMPORTANCE AS
SELECT * FROM TABLE(FORECAST_BY_PRODUCT_CHANNEL!EXPLAIN_FEATURE_IMPORTANCE());
