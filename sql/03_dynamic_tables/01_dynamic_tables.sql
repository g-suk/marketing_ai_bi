/*=============================================================================
  Summit Gear Co. -- Marketing AI+BI Lab
  03_dynamic_tables/01_dynamic_tables.sql

  Creates 12 dynamic tables in the MARKETING_ANALYTICS schema that transform
  raw source data from MARKETING_RAW into analytics-ready tables.

  Original (1-7):  Core revenue, campaigns, partners, customers, forecast,
                   spend, product metrics.
  Marketplace (8-11): Customer enrichment, geo-targeting, weather-revenue,
                      and marketing-mix-model daily — all fed by snapshot
                      tables created in 02_marketplace_enrichment.sql.

  Participants can inspect the lineage graph in Snowsight:
  Data > Databases > MARKETING_AI_BI > MARKETING_ANALYTICS > Dynamic Tables
=============================================================================*/

USE DATABASE MARKETING_AI_BI;
USE SCHEMA MARKETING_ANALYTICS;
USE WAREHOUSE COMPUTE_WH;

----------------------------------------------------------------------
-- 1. DT_DAILY_REVENUE -- Daily revenue by channel
----------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE DT_DAILY_REVENUE
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    order_date,
    channel,
    COUNT(*)                       AS order_count,
    SUM(quantity)                   AS total_units,
    ROUND(SUM(revenue), 2)         AS total_revenue,
    ROUND(AVG(revenue), 2)         AS avg_order_value
FROM MARKETING_AI_BI.MARKETING_RAW.ORDERS
GROUP BY order_date, channel;

----------------------------------------------------------------------
-- 2. DT_CAMPAIGN_METRICS -- Campaign-level KPIs
----------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE DT_CAMPAIGN_METRICS
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    c.campaign_id,
    c.campaign_name,
    c.channel,
    c.sub_channel,
    c.start_date,
    c.end_date,
    c.budget,
    c.target_conversions,
    COALESCE(s.total_spend, 0)       AS total_spend,
    COALESCE(s.total_impressions, 0) AS total_impressions,
    COALESCE(s.total_clicks, 0)      AS total_clicks,
    COALESCE(s.total_conversions, 0) AS total_conversions,
    COALESCE(o.campaign_revenue, 0)  AS campaign_revenue,
    CASE WHEN COALESCE(s.total_conversions, 0) > 0
         THEN ROUND(COALESCE(s.total_spend, 0) / s.total_conversions, 2)
         ELSE NULL
    END AS cpa,
    CASE WHEN COALESCE(s.total_spend, 0) > 0
         THEN ROUND(COALESCE(o.campaign_revenue, 0) / s.total_spend, 2)
         ELSE NULL
    END AS roas,
    CASE WHEN COALESCE(s.total_impressions, 0) > 0
         THEN ROUND(COALESCE(s.total_clicks, 0)::FLOAT / s.total_impressions * 100, 3)
         ELSE NULL
    END AS ctr_pct
FROM MARKETING_AI_BI.MARKETING_RAW.CAMPAIGNS c
LEFT JOIN (
    SELECT
        campaign_id,
        SUM(amount)      AS total_spend,
        SUM(impressions)  AS total_impressions,
        SUM(clicks)       AS total_clicks,
        SUM(conversions)  AS total_conversions
    FROM MARKETING_AI_BI.MARKETING_RAW.MARKETING_SPEND
    GROUP BY campaign_id
) s ON c.campaign_id = s.campaign_id
LEFT JOIN (
    SELECT
        campaign_id,
        SUM(revenue) AS campaign_revenue
    FROM MARKETING_AI_BI.MARKETING_RAW.ORDERS
    GROUP BY campaign_id
) o ON c.campaign_id = o.campaign_id;

----------------------------------------------------------------------
-- 3. DT_PARTNER_PERFORMANCE -- Wholesale partner metrics
----------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE DT_PARTNER_PERFORMANCE
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    wp.partner_id,
    wp.partner_name,
    wp.region,
    wp.tier,
    wp.avg_sell_through_rate,
    wp.annual_volume AS target_annual_volume,
    COUNT(o.order_id)           AS total_orders,
    SUM(o.quantity)             AS total_units_sold,
    ROUND(SUM(o.revenue), 2)   AS total_revenue,
    ROUND(AVG(o.revenue), 2)   AS avg_order_value,
    ROUND(SUM(o.revenue) / NULLIF(wp.annual_volume, 0) * 100, 1) AS revenue_attainment_pct
FROM MARKETING_AI_BI.MARKETING_RAW.WHOLESALE_PARTNERS wp
LEFT JOIN MARKETING_AI_BI.MARKETING_RAW.ORDERS o ON wp.partner_id = o.wholesale_partner_id
GROUP BY wp.partner_id, wp.partner_name, wp.region, wp.tier,
         wp.avg_sell_through_rate, wp.annual_volume;

----------------------------------------------------------------------
-- 4. DT_CUSTOMER_ENRICHED -- Customer profiles with marketplace enrichment
--    Supersedes the original DT_CUSTOMER_SEGMENTS by adding income,
--    lifestyle, outdoor interest, and purchase-behavior columns.
--    Depends on: CUSTOMER_ENRICHMENT_SNAPSHOT (02_marketplace_enrichment.sql)
----------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE DT_CUSTOMER_ENRICHED
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    c.customer_id,
    c.state,
    c.zip_code,
    c.age,
    c.gender,
    c.channel_preference,
    c.lifetime_value,
    c.signup_date,
    CASE
        WHEN c.age BETWEEN 18 AND 24 THEN '18-24'
        WHEN c.age BETWEEN 25 AND 34 THEN '25-34'
        WHEN c.age BETWEEN 35 AND 44 THEN '35-44'
        WHEN c.age BETWEEN 45 AND 54 THEN '45-54'
        ELSE '55+'
    END AS age_group,
    CASE
        WHEN c.lifetime_value >= 2000 THEN 'high_value'
        WHEN c.lifetime_value >= 500  THEN 'mid_value'
        ELSE 'low_value'
    END AS value_tier,
    sms.income_bracket,
    sms.lifestyle_segment,
    sms.outdoor_interest,
    o.total_orders,
    o.total_revenue,
    o.first_order_date,
    o.last_order_date,
    o.distinct_categories,
    DATEDIFF('day', c.signup_date, o.last_order_date) AS customer_tenure_days,
    DATEDIFF('day', o.last_order_date, CURRENT_DATE()) AS days_since_last_order,
    ROUND(o.total_revenue / NULLIF(o.total_orders, 0), 2) AS avg_order_value,
    ROUND(o.total_orders / NULLIF(DATEDIFF('month', o.first_order_date, o.last_order_date), 0), 2) AS orders_per_month
FROM MARKETING_AI_BI.MARKETING_RAW.CUSTOMERS c
LEFT JOIN MARKETING_AI_BI.MARKETING_RAW.CUSTOMER_ENRICHMENT_SNAPSHOT sms
    ON c.zip_code = sms.zip_code
LEFT JOIN (
    SELECT
        customer_id,
        COUNT(*) AS total_orders,
        ROUND(SUM(revenue), 2) AS total_revenue,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date,
        COUNT(DISTINCT product_category) AS distinct_categories
    FROM MARKETING_AI_BI.MARKETING_RAW.ORDERS
    GROUP BY customer_id
) o ON c.customer_id = o.customer_id;

----------------------------------------------------------------------
-- 5. DT_FORECAST_INPUT -- Revenue forecast input by product_category x channel
----------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE DT_FORECAST_INPUT
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    order_date AS ds,
    product_category || '_' || channel AS series,
    ROUND(SUM(revenue), 2) AS y,
    CASE WHEN DAYOFWEEK(order_date) IN (0, 6) THEN 1 ELSE 0 END AS is_weekend,
    CASE WHEN order_date IN (
        '2024-07-04','2024-09-02','2024-11-28','2024-11-29','2024-12-25',
        '2025-01-01','2025-01-20','2025-02-17','2025-05-26','2025-07-04',
        '2025-09-01','2025-11-27','2025-11-28','2025-12-25',
        '2026-01-01','2026-01-19','2026-02-16'
    ) THEN 1 ELSE 0 END AS is_holiday
FROM MARKETING_AI_BI.MARKETING_RAW.ORDERS
GROUP BY order_date, product_category, channel;

----------------------------------------------------------------------
-- 6. DT_SPEND_DAILY -- Daily spend by sub_channel for anomaly detection
----------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE DT_SPEND_DAILY
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    spend_date                    AS ds,
    channel,
    sub_channel,
    ROUND(SUM(amount), 2)         AS total_spend,
    SUM(impressions)              AS total_impressions,
    SUM(clicks)                   AS total_clicks,
    SUM(conversions)              AS total_conversions,
    CASE WHEN SUM(clicks) > 0
         THEN ROUND(SUM(conversions)::FLOAT / SUM(clicks) * 100, 3)
         ELSE 0
    END AS conversion_rate_pct
FROM MARKETING_AI_BI.MARKETING_RAW.MARKETING_SPEND
GROUP BY spend_date, channel, sub_channel;

----------------------------------------------------------------------
-- 6b. DT_SPEND_FORECAST_INPUT -- Spend forecast input by sub_channel
----------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE DT_SPEND_FORECAST_INPUT
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    ds,
    sub_channel AS series,
    SUM(total_spend) AS y,
    CASE WHEN DAYOFWEEK(ds) IN (0, 6) THEN 1 ELSE 0 END AS is_weekend
FROM DT_SPEND_DAILY
WHERE sub_channel != 'distributor_event'
GROUP BY ds, sub_channel;

----------------------------------------------------------------------
-- 7. DT_PRODUCT_REVENUE -- Revenue by product category
----------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE DT_PRODUCT_REVENUE
    TARGET_LAG = '1 hour'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    product_category,
    product_name,
    channel,
    COUNT(*)                       AS order_count,
    SUM(quantity)                   AS total_units,
    ROUND(SUM(revenue), 2)         AS total_revenue,
    ROUND(AVG(revenue), 2)         AS avg_order_value
FROM MARKETING_AI_BI.MARKETING_RAW.ORDERS
GROUP BY product_category, product_name, channel;

----------------------------------------------------------------------
-- 8. DT_WEATHER_REVENUE -- Daily revenue joined with national weather
--    Depends on: NATIONAL_WEATHER_SNAPSHOT (02_marketplace_enrichment.sql)
----------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE DT_WEATHER_REVENUE
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    r.order_date,
    r.channel,
    r.order_count,
    r.total_units,
    r.total_revenue,
    r.avg_order_value,
    w.national_avg_temp_f,
    w.national_precipitation_in,
    w.national_snowfall_in,
    CASE
        WHEN w.national_avg_temp_f < 32 THEN 'freezing'
        WHEN w.national_avg_temp_f < 50 THEN 'cold'
        WHEN w.national_avg_temp_f < 70 THEN 'mild'
        WHEN w.national_avg_temp_f < 85 THEN 'warm'
        ELSE 'hot'
    END AS temp_band,
    CASE WHEN w.national_precipitation_in > 0.5 THEN TRUE ELSE FALSE END AS rainy_day,
    CASE WHEN w.national_snowfall_in > 1.0 THEN TRUE ELSE FALSE END AS snow_day
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_DAILY_REVENUE r
LEFT JOIN MARKETING_AI_BI.MARKETING_RAW.NATIONAL_WEATHER_SNAPSHOT w
    ON r.order_date = w.weather_date;

----------------------------------------------------------------------
-- 9. DT_GEO_TARGETING -- Zip-level customer, revenue, and weather profile
--    Depends on: CUSTOMER_ENRICHMENT_SNAPSHOT, DAILY_WEATHER_SNAPSHOT
----------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE DT_GEO_TARGETING
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    c.state,
    c.zip_code,
    COUNT(DISTINCT c.customer_id) AS customer_count,
    ROUND(AVG(c.lifetime_value), 2) AS avg_ltv,
    ROUND(COALESCE(SUM(o.revenue), 0), 2) AS total_revenue,
    MODE(sms.income_bracket) AS dominant_income_bracket,
    MODE(sms.lifestyle_segment) AS dominant_lifestyle,
    MODE(sms.outdoor_interest) AS dominant_outdoor_interest,
    ROUND(AVG(w.avg_temp_f), 1) AS recent_avg_temp,
    ROUND(SUM(w.precipitation_in), 2) AS recent_total_precip,
    ROUND(SUM(w.snowfall_in), 2) AS recent_total_snowfall
FROM MARKETING_AI_BI.MARKETING_RAW.CUSTOMERS c
LEFT JOIN MARKETING_AI_BI.MARKETING_RAW.CUSTOMER_ENRICHMENT_SNAPSHOT sms
    ON c.zip_code = sms.zip_code
LEFT JOIN MARKETING_AI_BI.MARKETING_RAW.ORDERS o
    ON c.customer_id = o.customer_id
LEFT JOIN MARKETING_AI_BI.MARKETING_RAW.DAILY_WEATHER_SNAPSHOT w
    ON c.zip_code = w.zip_code
    AND w.weather_date >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY c.state, c.zip_code;

----------------------------------------------------------------------
-- 10. DT_MMM_DAILY -- Marketing mix model daily feature table
--     Joins spend, revenue, economic indicators, and weather.
--     Depends on: ECONOMIC_INDICATORS_SNAPSHOT, NATIONAL_WEATHER_SNAPSHOT
----------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE DT_MMM_DAILY
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    d.ds AS dt,
    d.channel,
    d.sub_channel,
    d.total_spend,
    d.total_impressions,
    d.total_clicks,
    d.total_conversions,
    d.conversion_rate_pct,
    rev.daily_revenue,
    ei.cpi,
    ei.retail_sales_sporting,
    ei.consumer_confidence,
    w.national_avg_temp_f,
    w.national_precipitation_in,
    w.national_snowfall_in,
    DAYOFWEEK(d.ds) AS dow,
    MONTH(d.ds) AS month_num,
    CASE WHEN DAYOFWEEK(d.ds) IN (0, 6) THEN 1 ELSE 0 END AS is_weekend
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_SPEND_DAILY d
LEFT JOIN (
    SELECT order_date, SUM(total_revenue) AS daily_revenue
    FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_DAILY_REVENUE
    GROUP BY order_date
) rev ON d.ds = rev.order_date
LEFT JOIN MARKETING_AI_BI.MARKETING_RAW.ECONOMIC_INDICATORS_SNAPSHOT ei
    ON DATE_TRUNC('MONTH', d.ds)::DATE = ei.indicator_month
LEFT JOIN MARKETING_AI_BI.MARKETING_RAW.NATIONAL_WEATHER_SNAPSHOT w
    ON d.ds = w.weather_date;
