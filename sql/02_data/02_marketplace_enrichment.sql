/*=============================================================================
  Summit Gear Co. -- Marketing AI+BI Lab
  02_data/02_marketplace_enrichment.sql
  
  Creates views that join synthetic data to Marketplace shared databases.
  
  IMPORTANT: Before running this script, install the Marketplace listings and
  run the GRANT statements in 01_setup/01_rbac_database_schema.sql.
  
  Verified database/schema/table names (Apr 2026):
    Snowflake Public Data  -> SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA
    SMS CustomerConnect    -> CUSTOMERCONNECT360__SAMPLE.SMS_DEMO
    GWI Core               -> GWI_OPEN_DATA.EXAMPLE_GWI_CORE
    Weather Source         -> FROSTBYTE_WEATHERSOURCE.ONPOINT_ID
=============================================================================*/

USE DATABASE MARKETING_AI_BI;
USE SCHEMA DEMO_DATA;

----------------------------------------------------------------------
-- Tier 1: Snowflake Public Data -- Economic indicators for FORECAST
----------------------------------------------------------------------
-- CPI monthly, retail sales (sporting goods), and consumer sentiment
-- as FORECAST exogenous features.
-- Schema: SNOWFLAKE_PUBLIC_DATA_PAID.PUBLIC_DATA
-- Table: FINANCIAL_ECONOMIC_INDICATORS_TIMESERIES (EAV model)
-- Key columns: GEO_ID, VARIABLE, VARIABLE_NAME, DATE, VALUE, UNIT

CREATE OR REPLACE VIEW V_ECONOMIC_INDICATORS AS
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
-- Tier 1: SMS CustomerConnect 360 -- Consumer enrichment
----------------------------------------------------------------------
-- Join on zip_code to enrich customer segments with lifestyle/income data.
-- Schema: CUSTOMERCONNECT360__SAMPLE.SMS_DEMO
-- View: DEMO_SMS_CUSTOMERCONNECT_360
-- Key columns: ZIP, ESTIMATED_HOUSEHOLD_INCOME, AGE, GENDER, LIFESTYLE_*

CREATE OR REPLACE VIEW V_CUSTOMER_ENRICHMENT AS
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.state,
    c.zip_code,
    c.age,
    c.channel_preference,
    c.lifetime_value,
    sms.estimated_household_income AS income_bracket,
    sms.lifestyle_segment,
    sms.outdoor_interest
FROM CUSTOMERS c
LEFT JOIN (
    SELECT
        TRIM(ZIP)::VARCHAR(5) AS zip_code,
        ESTIMATED_HOUSEHOLD_INCOME AS estimated_household_income,
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
    FROM CUSTOMERCONNECT360__SAMPLE.SMS_DEMO.DEMO_SMS_CUSTOMERCONNECT_360
) sms ON c.zip_code = sms.zip_code;

----------------------------------------------------------------------
-- Tier 2 (Bonus): GWI Core -- Consumer attitude validation
----------------------------------------------------------------------
-- GWI uses a survey-response model with LABELS (taxonomy) and RLD (responses).
-- Schema: GWI_OPEN_DATA.EXAMPLE_GWI_CORE
-- Tables: LABELS (question taxonomy), RLD (respondent-level data)
-- We aggregate respondent counts by question/datapoint for US market.

CREATE OR REPLACE VIEW V_CONSUMER_ATTITUDES AS
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
-- Tier 2 (Bonus): Weather Source -- Weather as FORECAST feature
----------------------------------------------------------------------
-- Daily weather by postal code for joining to revenue / spend data.
-- Schema: FROSTBYTE_WEATHERSOURCE.ONPOINT_ID
-- View: HISTORY_DAY

CREATE OR REPLACE VIEW V_DAILY_WEATHER AS
SELECT
    DATE_VALID_STD::DATE  AS weather_date,
    POSTAL_CODE::VARCHAR(5) AS zip_code,
    AVG_TEMPERATURE_AIR_2M_F::FLOAT AS avg_temp_f,
    TOT_PRECIPITATION_IN::FLOAT     AS precipitation_in,
    TOT_SNOWFALL_IN::FLOAT          AS snowfall_in
FROM FROSTBYTE_WEATHERSOURCE.ONPOINT_ID.HISTORY_DAY
WHERE DATE_VALID_STD >= '2024-07-01'
  AND COUNTRY = 'US';

-- Aggregated to national daily average for FORECAST feature
CREATE OR REPLACE VIEW V_NATIONAL_WEATHER AS
SELECT
    weather_date,
    ROUND(AVG(avg_temp_f), 1)      AS national_avg_temp_f,
    ROUND(SUM(precipitation_in), 2) AS national_precipitation_in,
    ROUND(SUM(snowfall_in), 2)      AS national_snowfall_in
FROM V_DAILY_WEATHER
GROUP BY weather_date;
