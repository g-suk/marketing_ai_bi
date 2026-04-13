/*=============================================================================
  Summit Gear Co. -- Marketing AI+BI Lab
  03_dynamic_tables/01_dynamic_tables.sql
  
  Creates 6 dynamic tables that transform raw source data into analytics-ready
  tables. These replace traditional CTAS or scheduled tasks with a declarative
  transformation layer that Snowflake keeps fresh automatically.
  
  Participants can inspect the lineage graph in Snowsight:
  Data > Databases > MARKETING_AI_BI > DEMO_DATA > Dynamic Tables
=============================================================================*/

USE DATABASE MARKETING_AI_BI;
USE SCHEMA DEMO_DATA;
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
FROM ORDERS
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
FROM CAMPAIGNS c
LEFT JOIN (
    SELECT
        campaign_id,
        SUM(amount)      AS total_spend,
        SUM(impressions)  AS total_impressions,
        SUM(clicks)       AS total_clicks,
        SUM(conversions)  AS total_conversions
    FROM MARKETING_SPEND
    GROUP BY campaign_id
) s ON c.campaign_id = s.campaign_id
LEFT JOIN (
    SELECT
        campaign_id,
        SUM(revenue) AS campaign_revenue
    FROM ORDERS
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
FROM WHOLESALE_PARTNERS wp
LEFT JOIN ORDERS o ON wp.partner_id = o.wholesale_partner_id
GROUP BY wp.partner_id, wp.partner_name, wp.region, wp.tier,
         wp.avg_sell_through_rate, wp.annual_volume;

----------------------------------------------------------------------
-- 4. DT_CUSTOMER_SEGMENTS -- Enriched customer segments
--    (Falls back gracefully if Marketplace data not yet installed)
----------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE DT_CUSTOMER_SEGMENTS
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
    o.total_orders,
    o.total_revenue
FROM CUSTOMERS c
LEFT JOIN (
    SELECT customer_id, COUNT(*) AS total_orders, SUM(revenue) AS total_revenue
    FROM ORDERS
    GROUP BY customer_id
) o ON c.customer_id = o.customer_id;

----------------------------------------------------------------------
-- 5. DT_FORECAST_INPUT -- FORECAST training data with exogenous features
----------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE DT_FORECAST_INPUT
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    dr.order_date AS ds,
    dr.channel    AS series,
    dr.total_revenue AS y,
    CASE WHEN DAYOFWEEK(dr.order_date) IN (0, 6) THEN 1 ELSE 0 END AS is_weekend,
    CASE WHEN dr.order_date IN (
        '2024-07-04','2024-09-02','2024-11-28','2024-11-29','2024-12-25',
        '2025-01-01','2025-01-20','2025-02-17','2025-05-26','2025-07-04',
        '2025-09-01','2025-11-27','2025-11-28','2025-12-25'
    ) THEN 1 ELSE 0 END AS is_holiday
FROM DT_DAILY_REVENUE dr;

----------------------------------------------------------------------
-- 6. DT_SPEND_DAILY -- Daily spend totals for ANOMALY_DETECTION
----------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE DT_SPEND_DAILY
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    spend_date                    AS ds,
    channel,
    ROUND(SUM(amount), 2)         AS total_spend,
    SUM(impressions)              AS total_impressions,
    SUM(clicks)                   AS total_clicks,
    SUM(conversions)              AS total_conversions,
    CASE WHEN SUM(clicks) > 0
         THEN ROUND(SUM(conversions)::FLOAT / SUM(clicks) * 100, 3)
         ELSE 0
    END AS conversion_rate_pct
FROM MARKETING_SPEND
GROUP BY spend_date, channel;
