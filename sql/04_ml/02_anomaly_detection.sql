/*=============================================================================
  Summit Gear Co. -- Marketing AI+BI Lab
  04_ml/02_anomaly_detection.sql
  
  Trains SNOWFLAKE.ML.ANOMALY_DETECTION models and materializes results.
  
  Pattern: Train on early data, detect anomalies on later data.
  
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
-- Result table with correct column widths
----------------------------------------------------------------------
CREATE OR REPLACE TABLE ANOMALY_DETECTION_RESULTS (
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
-- 1. Ad Spend Anomalies (train: Jul 2024 - Jun 2025, detect: Jul 2025 - Mar 2026)
----------------------------------------------------------------------
CREATE OR REPLACE VIEW V_ANOMALY_SPEND_TRAINING AS
SELECT ds, SUM(total_spend) AS y
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_SPEND_DAILY WHERE ds < '2025-07-01' GROUP BY ds;

CREATE OR REPLACE VIEW V_ANOMALY_SPEND_DETECT AS
SELECT ds, SUM(total_spend) AS y
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_SPEND_DAILY WHERE ds >= '2025-07-01' GROUP BY ds;

CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION ANOMALY_SPEND(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'V_ANOMALY_SPEND_TRAINING'),
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME => 'Y',
    LABEL_COLNAME => ''
);

INSERT INTO ANOMALY_DETECTION_RESULTS
SELECT 'Ad Spend' AS series, ts, y, forecast, lower_bound, upper_bound, is_anomaly, percentile
FROM TABLE(ANOMALY_SPEND!DETECT_ANOMALIES(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'V_ANOMALY_SPEND_DETECT'),
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME => 'Y'
));

----------------------------------------------------------------------
-- 2. DTC Conversion Rate Anomalies (train: Jul 2024 - Jun 2025, detect: Jul 2025 - Mar 2026)
----------------------------------------------------------------------
CREATE OR REPLACE VIEW V_ANOMALY_DTC_CVR_TRAINING AS
SELECT ds, conversion_rate_pct AS y
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_SPEND_DAILY WHERE channel = 'DTC' AND ds < '2025-07-01';

CREATE OR REPLACE VIEW V_ANOMALY_DTC_CVR_DETECT AS
SELECT ds, conversion_rate_pct AS y
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_SPEND_DAILY WHERE channel = 'DTC' AND ds >= '2025-07-01';

CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION ANOMALY_DTC_CONVERSION(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'V_ANOMALY_DTC_CVR_TRAINING'),
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME => 'Y',
    LABEL_COLNAME => ''
);

INSERT INTO ANOMALY_DETECTION_RESULTS
SELECT 'DTC Conversion Rate' AS series, ts, y, forecast, lower_bound, upper_bound, is_anomaly, percentile
FROM TABLE(ANOMALY_DTC_CONVERSION!DETECT_ANOMALIES(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'V_ANOMALY_DTC_CVR_DETECT'),
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME => 'Y'
));

----------------------------------------------------------------------
-- 3. Wholesale Order Volume Anomalies (train: Jul 2024 - Jun 2025, detect: Jul 2025 - Mar 2026)
----------------------------------------------------------------------
CREATE OR REPLACE VIEW V_ANOMALY_WHOLESALE_ORDERS AS
SELECT order_date AS ds, COUNT(*) AS y
FROM MARKETING_AI_BI.MARKETING_RAW.ORDERS WHERE channel = 'wholesale' GROUP BY order_date;

CREATE OR REPLACE VIEW V_ANOMALY_WHOLESALE_ORDERS_TRAIN AS
SELECT ds, y FROM V_ANOMALY_WHOLESALE_ORDERS WHERE ds < '2025-06-01';

CREATE OR REPLACE VIEW V_ANOMALY_WHOLESALE_ORDERS_DETECT AS
SELECT ds, y FROM V_ANOMALY_WHOLESALE_ORDERS WHERE ds >= '2025-06-01';

CREATE OR REPLACE SNOWFLAKE.ML.ANOMALY_DETECTION ANOMALY_WHOLESALE_ORDERS(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'V_ANOMALY_WHOLESALE_ORDERS_TRAIN'),
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME => 'Y',
    LABEL_COLNAME => ''
);

INSERT INTO ANOMALY_DETECTION_RESULTS
SELECT 'Wholesale Orders' AS series, ts, y, forecast, lower_bound, upper_bound, is_anomaly, percentile
FROM TABLE(ANOMALY_WHOLESALE_ORDERS!DETECT_ANOMALIES(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'V_ANOMALY_WHOLESALE_ORDERS_DETECT'),
    TIMESTAMP_COLNAME => 'DS',
    TARGET_COLNAME => 'Y'
));
