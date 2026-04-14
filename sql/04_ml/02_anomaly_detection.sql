/*=============================================================================
  Summit Gear Co. -- Marketing AI+BI Lab
  04_ml/02_anomaly_detection.sql

  Trains SNOWFLAKE.ML.ANOMALY_DETECTION models and materializes results.

  Pattern: Train on early data, detect anomalies on later data.

  Models:
    1. Ad Spend by sub_channel -- catches tactic-level overspend (multi-series)
    2. DTC Conversion Rate -- catches conversion crashes (single-series)
    3. Wholesale Orders by product_category -- catches product-level surges (multi-series)

  Seeded anomalies that should surface:
    - Nov 11-22 2024: Influencer overspend (3x) -- in training data
    - Feb 08-16 2025: DTC conversion crash (80% drop)
    - Sep 15-26 2025: Wholesale order surge (5x)
    - Mar 03-08 2026: DTC flash sale spike (3.5x)

  Results stored in: ANOMALY_DETECTION_RESULTS
=============================================================================*/

USE DATABASE MARKETING_AI_BI;
USE SCHEMA MARKETING_ANALYTICS;
USE WAREHOUSE COMPUTE_WH;

----------------------------------------------------------------------
-- Result table
----------------------------------------------------------------------
CREATE OR REPLACE TABLE ANOMALY_DETECTION_RESULTS (
    model_name VARCHAR(50),
    series VARCHAR(50),
    ts TIMESTAMP_NTZ,
    y FLOAT,
    forecast FLOAT,
    lower_bound FLOAT,
    upper_bound FLOAT,
    is_anomaly BOOLEAN,
    percentile FLOAT
);

----------------------------------------------------------------------
-- 1. Ad Spend by sub_channel (train: Jul 2024 - Jun 2025, detect: Jul 2025+)
--    Excludes distributor_event (too sparse for reliable model)
----------------------------------------------------------------------
CREATE OR REPLACE VIEW V_ANOMALY_SPEND_TRAINING AS
SELECT ds, sub_channel AS series, SUM(total_spend) AS y
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_SPEND_DAILY
WHERE ds < '2025-07-01'
  AND sub_channel != 'distributor_event'
GROUP BY ds, sub_channel;

CREATE OR REPLACE VIEW V_ANOMALY_SPEND_DETECT AS
SELECT ds, sub_channel AS series, SUM(total_spend) AS y
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_SPEND_DAILY
WHERE ds >= '2025-07-01'
  AND sub_channel != 'distributor_event'
GROUP BY ds, sub_channel;

CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION ANOMALY_SPEND_BY_SUBCHANNEL(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'V_ANOMALY_SPEND_TRAINING'),
    SERIES_COLNAME => 'SERIES',
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME => 'Y',
    LABEL_COLNAME => ''
);

INSERT INTO ANOMALY_DETECTION_RESULTS
SELECT
    'spend_subchannel' AS model_name,
    series,
    ts,
    y,
    forecast,
    lower_bound,
    upper_bound,
    is_anomaly,
    percentile
FROM TABLE(ANOMALY_SPEND_BY_SUBCHANNEL!DETECT_ANOMALIES(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'V_ANOMALY_SPEND_DETECT'),
    SERIES_COLNAME => 'SERIES',
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME => 'Y'
));

----------------------------------------------------------------------
-- 2. DTC Conversion Rate (single-series, train: Jul 2024 - Jun 2025)
----------------------------------------------------------------------
CREATE OR REPLACE VIEW V_ANOMALY_DTC_CVR_TRAINING AS
SELECT ds, SUM(total_conversions)::FLOAT / NULLIF(SUM(total_clicks), 0) * 100 AS y
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_SPEND_DAILY
WHERE channel = 'DTC'
  AND ds < '2025-07-01'
GROUP BY ds;

CREATE OR REPLACE VIEW V_ANOMALY_DTC_CVR_DETECT AS
SELECT ds, SUM(total_conversions)::FLOAT / NULLIF(SUM(total_clicks), 0) * 100 AS y
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_SPEND_DAILY
WHERE channel = 'DTC'
  AND ds >= '2025-07-01'
GROUP BY ds;

CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION ANOMALY_DTC_CONVERSION(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'V_ANOMALY_DTC_CVR_TRAINING'),
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME => 'Y',
    LABEL_COLNAME => ''
);

INSERT INTO ANOMALY_DETECTION_RESULTS
SELECT
    'dtc_conversion' AS model_name,
    'DTC Conversion Rate' AS series,
    ts,
    y,
    forecast,
    lower_bound,
    upper_bound,
    is_anomaly,
    percentile
FROM TABLE(ANOMALY_DTC_CONVERSION!DETECT_ANOMALIES(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'V_ANOMALY_DTC_CVR_DETECT'),
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME => 'Y'
));

----------------------------------------------------------------------
-- 3. Wholesale Orders by product_category (multi-series)
--    Train: Jul 2024 - May 2025, Detect: Jun 2025+
----------------------------------------------------------------------
CREATE OR REPLACE VIEW V_ANOMALY_WHOLESALE_ORDERS_TRAIN AS
SELECT
    order_date AS ds,
    product_category AS series,
    COUNT(*) AS y
FROM MARKETING_AI_BI.MARKETING_RAW.ORDERS
WHERE channel = 'wholesale'
  AND order_date < '2025-06-01'
GROUP BY order_date, product_category;

CREATE OR REPLACE VIEW V_ANOMALY_WHOLESALE_ORDERS_DETECT AS
SELECT
    order_date AS ds,
    product_category AS series,
    COUNT(*) AS y
FROM MARKETING_AI_BI.MARKETING_RAW.ORDERS
WHERE channel = 'wholesale'
  AND order_date >= '2025-06-01'
GROUP BY order_date, product_category;

CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION ANOMALY_WHOLESALE_BY_PRODUCT(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'V_ANOMALY_WHOLESALE_ORDERS_TRAIN'),
    SERIES_COLNAME => 'SERIES',
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME => 'Y',
    LABEL_COLNAME => ''
);

INSERT INTO ANOMALY_DETECTION_RESULTS
SELECT
    'wholesale_product' AS model_name,
    series,
    ts,
    y,
    forecast,
    lower_bound,
    upper_bound,
    is_anomaly,
    percentile
FROM TABLE(ANOMALY_WHOLESALE_BY_PRODUCT!DETECT_ANOMALIES(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'V_ANOMALY_WHOLESALE_ORDERS_DETECT'),
    SERIES_COLNAME => 'SERIES',
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME => 'Y'
));
