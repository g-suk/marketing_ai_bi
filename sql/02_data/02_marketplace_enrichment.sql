/*=============================================================================
  Summit Gear Co. -- Marketing AI+BI Lab
  02_data/02_marketplace_enrichment.sql

  Materializes Marketplace shared data into snapshot tables.  Dynamic tables
  cannot track changes on secure views from shared databases (time-travel
  limitation), so we materialize directly into tables that downstream
  dynamic tables reference.

  Chain:  Marketplace DB  →  Snapshot table  →  Dynamic tables

  IMPORTANT: Before running this script, install the Marketplace listings and
  run the GRANT statements in 01_setup/setup.sql.

  Verified database/schema/table names (Apr 2026):
    Snowflake Public Data  -> SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA
    SMS CustomerConnect    -> CUSTOMERCONNECT360__SAMPLE.SMS_DEMO
    GWI Core               -> GWI_OPEN_DATA.EXAMPLE_GWI_CORE
    Weather Source         -> FROSTBYTE_WEATHERSOURCE.ONPOINT_ID
=============================================================================*/

USE DATABASE MARKETING_AI_BI;
USE SCHEMA MARKETING_RAW;

----------------------------------------------------------------------
-- 1. Snowflake Public Data -- Economic indicators
----------------------------------------------------------------------
CREATE OR REPLACE TABLE ECONOMIC_INDICATORS_SNAPSHOT AS
SELECT
    DATE_TRUNC('MONTH', t.DATE)::DATE AS indicator_month,
    MAX(CASE WHEN t.VARIABLE = 'CUSRSA0SA01982-84.M' THEN t.VALUE END) AS cpi,
    MAX(CASE WHEN t.VARIABLE = '451SM_SA' THEN t.VALUE END) AS retail_sales_sporting,
    MAX(CASE WHEN t.VARIABLE = 'the_index_of_consumer_sentiment' THEN t.VALUE END) AS consumer_confidence
FROM SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA.FINANCIAL_ECONOMIC_INDICATORS_TIMESERIES t
WHERE t.GEO_ID = 'country/USA'
  AND t.DATE >= '2024-01-01'
  AND t.VARIABLE IN (
      'CUSRSA0SA01982-84.M',
      '451SM_SA',
      'the_index_of_consumer_sentiment'
  )
GROUP BY 1;

----------------------------------------------------------------------
-- 2. SMS CustomerConnect 360 -- Consumer enrichment by zip code
----------------------------------------------------------------------
CREATE OR REPLACE TABLE CUSTOMER_ENRICHMENT_SNAPSHOT AS
SELECT
    TRIM(ZIP)::VARCHAR(5) AS zip_code,
    ESTIMATED_HOUSEHOLD_INCOME AS income_bracket,
    CASE
        WHEN LIFESTYLE_TRAVEL = 'Single' OR LIFESTYLE_TRAVEL_DOMESTIC = 'Single' THEN 'Travel Enthusiast'
        WHEN LIFESTYLE_HEALTH_FITNESS = 'Single' OR LIFESTYLE_EXERCISE_HEALTH_GROUPING = 'Single' THEN 'Health & Fitness'
        WHEN LIFESTYLE_OUTDOOR = 'Single' OR LIFESTYLE_OUTDOOR_SPORT_RECREATION = 'Single' THEN 'Outdoor Adventurer'
        WHEN LIFESTYLE_SPORTS_GROUPING = 'Single' OR LIFESTYLE_SPORTS_AND_LEISURE = 'Single' THEN 'Sports Fan'
        WHEN LIFESTYLE_HOME_AND_GARDEN = 'Single' OR LIFESTYLE_HOME_IMPROVEMENT = 'Single' THEN 'Home & Garden'
        ELSE 'General Consumer'
    END AS lifestyle_segment,
    CASE
        WHEN LIFESTYLE_CAMPING_HIKING = 'Single' THEN 'Camping/Hiking'
        WHEN LIFESTYLE_HUNTING_SHOOTING = 'Single' OR LIFESTYLE_FISHING = 'Single' THEN 'Hunting/Fishing'
        WHEN LIFESTYLE_SNOW_SKIING = 'Single' THEN 'Snow Sports'
        WHEN LIFESTYLE_OUTDOOR = 'Single' OR LIFESTYLE_OUTDOOR_SPORT_RECREATION = 'Single' THEN 'General Outdoor'
        ELSE 'None Identified'
    END AS outdoor_interest
FROM CUSTOMERCONNECT360__SAMPLE.SMS_DEMO.DEMO_SMS_CUSTOMERCONNECT_360;

----------------------------------------------------------------------
-- 3. GWI Core -- Consumer attitudes
----------------------------------------------------------------------
CREATE OR REPLACE TABLE CONSUMER_ATTITUDES_SNAPSHOT AS
SELECT
    r.RELEASE_YEAR AS survey_year,
    r.MARKET_CODE AS country,
    l.CATEGORY_1 AS category,
    l.QUESTION_NAME AS metric,
    l.DATAPOINT_NAME AS response,
    COUNT(DISTINCT r.RESPONDENT_CODE) AS respondent_count,
    ROUND(SUM(r.RESPONDENT_WEIGHT), 2) AS weighted_population
FROM GWI_OPEN_DATA.EXAMPLE_GWI_CORE.RLD r
JOIN GWI_OPEN_DATA.EXAMPLE_GWI_CORE.LABELS l
    ON r.NAMESPACE_CODE = l.NAMESPACE_CODE
    AND r.QUESTION_CODE = l.QUESTION_CODE
    AND r.DATAPOINT_CODE = l.DATAPOINT_CODE
WHERE r.MARKET_CODE = 'usa'
  AND r.RELEASE_YEAR >= 2021
  AND l.CATEGORY_2 IN ('Demographics', 'Media Consumption', 'Attitudes')
GROUP BY 1, 2, 3, 4, 5;

----------------------------------------------------------------------
-- 4. Weather Source -- Daily weather by zip code
----------------------------------------------------------------------
CREATE OR REPLACE TABLE DAILY_WEATHER_SNAPSHOT AS
SELECT
    DATE_VALID_STD::DATE           AS weather_date,
    POSTAL_CODE::VARCHAR(5)        AS zip_code,
    AVG_TEMPERATURE_AIR_2M_F::FLOAT AS avg_temp_f,
    TOT_PRECIPITATION_IN::FLOAT    AS precipitation_in,
    TOT_SNOWFALL_IN::FLOAT         AS snowfall_in
FROM FROSTBYTE_WEATHERSOURCE.ONPOINT_ID.HISTORY_DAY
WHERE DATE_VALID_STD >= '2025-01-01'
  AND COUNTRY = 'US';

----------------------------------------------------------------------
-- 5. Weather Source -- National daily averages
----------------------------------------------------------------------
CREATE OR REPLACE TABLE NATIONAL_WEATHER_SNAPSHOT AS
SELECT
    weather_date,
    ROUND(AVG(avg_temp_f), 1)      AS national_avg_temp_f,
    ROUND(SUM(precipitation_in), 2) AS national_precipitation_in,
    ROUND(SUM(snowfall_in), 2)      AS national_snowfall_in
FROM DAILY_WEATHER_SNAPSHOT
GROUP BY weather_date;
