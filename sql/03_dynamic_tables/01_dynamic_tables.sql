/*=============================================================================
  Summit Gear Co. -- Marketing AI+BI Lab
  03_dynamic_tables/01_dynamic_tables.sql

  Creates dynamic tables in the MARKETING_ANALYTICS schema that transform
  raw source data from MARKETING_RAW into analytics-ready tables, plus
  materialized derived tables for geo-targeting, MMM, CLV, and attribution.

  Original (1-7):  Core revenue, campaigns, partners, customers, forecast,
                   spend, product metrics.
  Marketplace (8-11): Customer enrichment, geo-targeting, weather-revenue,
                      and marketing-mix-model daily — all fed by snapshot
                      tables created in 02_marketplace_enrichment.sql.
  Derived (12-13): GEO_TARGETING_PROFILES and GEO_WEATHER_TRIGGERS —
                   scored/classified views of DT_GEO_TARGETING.
  MMM (14-16): Marketing Mix Model channel contributions, weekly
               decomposition, and AI insights.
  CLV (17): Customer lifetime value tiers and churn risk classification.
  MTA (18-19): Multi-touch attribution touchpoints and journey summaries.
  Attribution Views (20-25): 5 rule-based models + unified summary view.

  Participants can inspect the lineage graph in Snowsight:
  Data > Databases > MARKETING_AI_BI > MARKETING_ANALYTICS > Dynamic Tables
=============================================================================*/

USE ROLE MARKETING_LAB_ROLE;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE MARKETING_AI_BI;
USE SCHEMA MARKETING_ANALYTICS;

----------------------------------------------------------------------
-- 1. DT_DAILY_REVENUE -- Daily revenue by channel
----------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE DT_DAILY_REVENUE
    TARGET_LAG = '1 day'
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
    TARGET_LAG = '1 day'
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
    TARGET_LAG = '1 day'
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
    TARGET_LAG = '1 day'
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

----------------------------------------------------------------------
-- 11. GEO_TARGETING_PROFILES -- Zip-level profiles with targeting score
--     Materialized from DT_GEO_TARGETING with computed score & categories
----------------------------------------------------------------------
CREATE OR REPLACE TABLE GEO_TARGETING_PROFILES AS
SELECT
    g.*,
    ROUND(
        (COALESCE(g.avg_ltv, 0) / 3000.0) * 0.4
        + (LEAST(g.customer_count, 100) / 100.0) * 0.3
        + (COALESCE(g.total_revenue, 0)
           / (SELECT MAX(total_revenue) FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_GEO_TARGETING)) * 0.3
    , 3) AS targeting_score,
    CASE
        WHEN COALESCE(g.recent_total_snowfall, 0) > 5 THEN 'winter_sports'
        WHEN COALESCE(g.recent_avg_temp, 60) > 75     THEN 'camping'
        WHEN COALESCE(g.recent_avg_temp, 60) < 40      THEN 'outerwear'
        WHEN COALESCE(g.recent_total_precip, 0) > 3    THEN 'footwear'
        ELSE 'accessories'
    END AS weather_recommended_category,
    CASE
        WHEN g.customer_count >= 10          THEN 'social'
        WHEN COALESCE(g.avg_ltv, 0) > 1500  THEN 'email'
        ELSE 'search'
    END AS recommended_channel
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_GEO_TARGETING g;

----------------------------------------------------------------------
-- 14. MMM_CHANNEL_CONTRIBUTIONS -- Quarterly channel-level MMM results
--     Distributes total daily revenue across sub-channels proportional
--     to each channel's share of weekly spend, weighted by conversion
--     efficiency. ROI = attributed_revenue / total_spend.
----------------------------------------------------------------------
CREATE OR REPLACE TABLE MMM_CHANNEL_CONTRIBUTIONS AS
WITH weekly_spend AS (
    SELECT
        DATE_TRUNC('week', ds) AS week_start,
        sub_channel,
        SUM(total_spend) AS weekly_spend,
        SUM(total_conversions) AS weekly_conversions
    FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_SPEND_DAILY
    GROUP BY DATE_TRUNC('week', ds), sub_channel
),
weekly_revenue AS (
    SELECT
        DATE_TRUNC('week', order_date) AS week_start,
        SUM(total_revenue) AS weekly_revenue
    FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_DAILY_REVENUE
    GROUP BY DATE_TRUNC('week', order_date)
),
spend_share AS (
    SELECT
        ws.week_start,
        ws.sub_channel,
        ws.weekly_spend,
        ws.weekly_conversions,
        wr.weekly_revenue,
        ws.weekly_spend / NULLIF(SUM(ws.weekly_spend) OVER (PARTITION BY ws.week_start), 0) AS spend_share,
        CASE WHEN ws.weekly_spend > 0
             THEN ws.weekly_conversions::FLOAT / ws.weekly_spend
             ELSE 0
        END AS conversion_efficiency
    FROM weekly_spend ws
    JOIN weekly_revenue wr ON ws.week_start = wr.week_start
),
weighted AS (
    SELECT *,
        spend_share * (1 + conversion_efficiency) AS raw_weight,
        SUM(spend_share * (1 + conversion_efficiency)) OVER (PARTITION BY week_start) AS total_weight
    FROM spend_share
),
attributed AS (
    SELECT
        week_start,
        sub_channel,
        weekly_spend,
        weekly_revenue,
        ROUND(weekly_revenue * raw_weight / NULLIF(total_weight, 0), 2) AS attributed_revenue,
        weekly_conversions
    FROM weighted
)
SELECT
    sub_channel,
    'Q' || QUARTER(week_start) || '_' || YEAR(week_start) AS period,
    ROUND(SUM(weekly_spend), 2) AS total_spend,
    ROUND(SUM(attributed_revenue), 2) AS attributed_revenue,
    ROUND(SUM(attributed_revenue) / NULLIF(SUM(weekly_spend), 0), 2) AS roi,
    ROUND(SUM(weekly_spend) / NULLIF(SUM(SUM(weekly_spend)) OVER (PARTITION BY 'Q' || QUARTER(week_start) || '_' || YEAR(week_start)), 0) * 100, 1) AS share_of_spend,
    ROUND(SUM(weekly_spend) / NULLIF(SUM(weekly_conversions), 0), 2) AS cost_per_conversion
FROM attributed
GROUP BY sub_channel, 'Q' || QUARTER(week_start) || '_' || YEAR(week_start);

----------------------------------------------------------------------
-- 15. MMM_WEEKLY_DECOMPOSITION -- Weekly spend vs. attributed revenue
----------------------------------------------------------------------
CREATE OR REPLACE TABLE MMM_WEEKLY_DECOMPOSITION AS
WITH weekly_spend AS (
    SELECT
        DATE_TRUNC('week', ds) AS week_start,
        sub_channel,
        SUM(total_spend) AS spend,
        SUM(total_conversions) AS conversions
    FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_SPEND_DAILY
    GROUP BY DATE_TRUNC('week', ds), sub_channel
),
weekly_revenue AS (
    SELECT DATE_TRUNC('week', order_date) AS week_start, SUM(total_revenue) AS weekly_revenue
    FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_DAILY_REVENUE
    GROUP BY DATE_TRUNC('week', order_date)
),
spend_share AS (
    SELECT ws.*, wr.weekly_revenue,
        ws.spend / NULLIF(SUM(ws.spend) OVER (PARTITION BY ws.week_start), 0) AS s_share,
        CASE WHEN ws.spend > 0 THEN ws.conversions::FLOAT / ws.spend ELSE 0 END AS conv_eff
    FROM weekly_spend ws
    JOIN weekly_revenue wr ON ws.week_start = wr.week_start
),
weighted AS (
    SELECT *,
        s_share * (1 + conv_eff) AS rw,
        SUM(s_share * (1 + conv_eff)) OVER (PARTITION BY week_start) AS tw
    FROM spend_share
)
SELECT
    week_start,
    sub_channel,
    ROUND(spend, 2) AS spend,
    ROUND(weekly_revenue * rw / NULLIF(tw, 0), 2) AS attributed_revenue,
    ROUND(weekly_revenue * rw / NULLIF(tw, 0) - spend, 2) AS incremental_revenue,
    ROUND(weekly_revenue * rw / NULLIF(tw, 0) / NULLIF(spend, 0), 3) AS efficiency_index
FROM weighted
ORDER BY week_start, sub_channel;

----------------------------------------------------------------------
-- 16. MMM_AI_INSIGHTS -- LLM-generated strategic MMM summary
----------------------------------------------------------------------
CREATE OR REPLACE TABLE MMM_AI_INSIGHTS AS
WITH channel_summary AS (
    SELECT
        sub_channel,
        ROUND(SUM(total_spend), 0) AS total_spend,
        ROUND(SUM(attributed_revenue), 0) AS total_attributed,
        ROUND(SUM(attributed_revenue) / NULLIF(SUM(total_spend), 0), 2) AS overall_roi
    FROM MARKETING_AI_BI.MARKETING_ANALYTICS.MMM_CHANNEL_CONTRIBUTIONS
    GROUP BY sub_channel
    ORDER BY overall_roi DESC
)
SELECT
    SNOWFLAKE.CORTEX.COMPLETE(
        'snowflake-llama-3.3-70b',
        'You are a senior marketing analytics strategist for Summit Gear Co., an outdoor recreation retailer. '
        || 'Analyze this Marketing Mix Model output and provide a strategic summary with specific budget reallocation recommendations. '
        || 'Channel performance: ' || (SELECT LISTAGG(sub_channel || ': spend=$' || total_spend || ', attributed_rev=$' || total_attributed || ', ROI=' || overall_roi, '; ') FROM channel_summary)
        || '. Provide 3-4 concise bullet points covering: top performing channels, underperforming channels, and specific reallocation recommendations with percentages.'
    ) AS insight_text;

----------------------------------------------------------------------
-- 17. CLV_RISK_CLASSIFICATION -- Customer lifetime value tiers + churn
----------------------------------------------------------------------
CREATE OR REPLACE TABLE CLV_RISK_CLASSIFICATION AS
WITH base AS (
    SELECT
        customer_id,
        state,
        age_group,
        value_tier,
        lifetime_value,
        total_orders,
        total_revenue,
        days_since_last_order,
        orders_per_month,
        lifestyle_segment,
        outdoor_interest,
        LEAST(1.0, GREATEST(0.0,
            0.6 * LEAST(1.0, COALESCE(days_since_last_order, 365) / 365.0)
            + 0.4 * (1.0 - LEAST(1.0, COALESCE(orders_per_month, 0) / 2.0))
        )) AS churn_risk_score
    FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_CUSTOMER_ENRICHED
)
SELECT
    customer_id,
    CASE
        WHEN lifetime_value >= 1500
             AND COALESCE(days_since_last_order, 999) <= 60
             AND churn_risk_score <= 0.20
            THEN 'Loyal High-Value'
        WHEN (lifetime_value >= 500 OR total_orders >= 3)
             AND COALESCE(days_since_last_order, 999) <= 120
            THEN 'Growth Potential'
        WHEN total_orders <= 2
             AND COALESCE(days_since_last_order, 0) <= 120
            THEN 'New Customer'
        WHEN COALESCE(days_since_last_order, 999) > 300
             OR churn_risk_score > 0.85
            THEN 'Lapsed'
        WHEN COALESCE(days_since_last_order, 999) BETWEEN 121 AND 300
             OR churn_risk_score BETWEEN 0.40 AND 0.75
            THEN 'At-Risk'
        ELSE 'Growth Potential'
    END AS clv_tier,
    lifetime_value,
    ROUND(churn_risk_score, 3) AS churn_risk_score,
    total_orders,
    total_revenue,
    days_since_last_order,
    state,
    age_group,
    value_tier,
    lifestyle_segment,
    outdoor_interest
FROM base;

----------------------------------------------------------------------
-- 18. DT_MTA_TOUCHPOINTS -- Multi-touch attribution touchpoints
--     For each conversion (order), identifies all sub-channels the
--     customer was exposed to within 30 days (one touch per sub-channel).
----------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE DT_MTA_TOUCHPOINTS
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = COMPUTE_WH
AS
WITH channel_touches AS (
    SELECT
        o.customer_id,
        o.order_id AS conversion_id,
        o.order_date AS conversion_date,
        o.revenue,
        c.sub_channel,
        MIN(s.spend_date) AS first_touch_date,
        MAX(s.spend_date) AS last_touch_date,
        SUM(s.amount) AS channel_spend
    FROM MARKETING_AI_BI.MARKETING_RAW.ORDERS o
    JOIN MARKETING_AI_BI.MARKETING_RAW.MARKETING_SPEND s
        ON s.spend_date BETWEEN DATEADD('day', -30, o.order_date) AND o.order_date
        AND s.conversions > 0
    JOIN MARKETING_AI_BI.MARKETING_RAW.CAMPAIGNS c
        ON s.campaign_id = c.campaign_id
    GROUP BY o.customer_id, o.order_id, o.order_date, o.revenue, c.sub_channel
)
SELECT
    customer_id,
    conversion_id,
    conversion_date,
    revenue,
    sub_channel,
    first_touch_date,
    last_touch_date,
    channel_spend,
    DATEDIFF('day', first_touch_date, conversion_date) AS days_to_conversion,
    ROW_NUMBER() OVER (
        PARTITION BY conversion_id ORDER BY first_touch_date, sub_channel
    ) AS touchpoint_sequence,
    COUNT(*) OVER (PARTITION BY conversion_id) AS total_touchpoints
FROM channel_touches;

----------------------------------------------------------------------
-- 19. DT_MTA_JOURNEY_SUMMARY -- Aggregated journey-level stats
----------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE DT_MTA_JOURNEY_SUMMARY
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = COMPUTE_WH
AS
SELECT
    customer_id,
    conversion_id,
    revenue,
    MIN(first_touch_date) AS first_touch_date,
    MAX(last_touch_date) AS last_touch_date,
    conversion_date,
    MAX(total_touchpoints) AS total_touchpoints,
    DATEDIFF('day', MIN(first_touch_date), conversion_date) AS journey_duration_days,
    LISTAGG(DISTINCT sub_channel, ' > ') WITHIN GROUP (ORDER BY sub_channel) AS channel_path
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_MTA_TOUCHPOINTS
GROUP BY customer_id, conversion_id, revenue, conversion_date;

----------------------------------------------------------------------
-- 20. V_FIRST_TOUCH_ATTRIBUTION -- 100% credit to first interaction
----------------------------------------------------------------------
CREATE OR REPLACE VIEW V_FIRST_TOUCH_ATTRIBUTION AS
SELECT
    conversion_id,
    customer_id,
    sub_channel,
    touchpoint_sequence,
    total_touchpoints,
    revenue,
    days_to_conversion,
    CASE WHEN touchpoint_sequence = 1 THEN revenue ELSE 0 END AS attributed_revenue,
    CASE WHEN touchpoint_sequence = 1 THEN 1.0 ELSE 0.0 END AS attribution_weight,
    'first_touch' AS model_name
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_MTA_TOUCHPOINTS;

----------------------------------------------------------------------
-- 21. V_LAST_TOUCH_ATTRIBUTION -- 100% credit to last interaction
----------------------------------------------------------------------
CREATE OR REPLACE VIEW V_LAST_TOUCH_ATTRIBUTION AS
SELECT
    conversion_id,
    customer_id,
    sub_channel,
    touchpoint_sequence,
    total_touchpoints,
    revenue,
    days_to_conversion,
    CASE WHEN touchpoint_sequence = total_touchpoints THEN revenue ELSE 0 END AS attributed_revenue,
    CASE WHEN touchpoint_sequence = total_touchpoints THEN 1.0 ELSE 0.0 END AS attribution_weight,
    'last_touch' AS model_name
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_MTA_TOUCHPOINTS;

----------------------------------------------------------------------
-- 22. V_LINEAR_ATTRIBUTION -- Equal credit across all touchpoints
----------------------------------------------------------------------
CREATE OR REPLACE VIEW V_LINEAR_ATTRIBUTION AS
SELECT
    conversion_id,
    customer_id,
    sub_channel,
    touchpoint_sequence,
    total_touchpoints,
    revenue,
    days_to_conversion,
    ROUND(revenue / total_touchpoints, 2) AS attributed_revenue,
    ROUND(1.0 / total_touchpoints, 4) AS attribution_weight,
    'linear' AS model_name
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_MTA_TOUCHPOINTS;

----------------------------------------------------------------------
-- 23. V_TIME_DECAY_ATTRIBUTION -- More credit to recent touchpoints
----------------------------------------------------------------------
CREATE OR REPLACE VIEW V_TIME_DECAY_ATTRIBUTION AS
WITH raw_weights AS (
    SELECT *,
        1.0 / (days_to_conversion + 1) AS raw_weight,
        SUM(1.0 / (days_to_conversion + 1)) OVER (PARTITION BY conversion_id) AS total_weight
    FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_MTA_TOUCHPOINTS
)
SELECT
    conversion_id,
    customer_id,
    sub_channel,
    touchpoint_sequence,
    total_touchpoints,
    revenue,
    days_to_conversion,
    ROUND(revenue * raw_weight / NULLIF(total_weight, 0), 2) AS attributed_revenue,
    ROUND(raw_weight / NULLIF(total_weight, 0), 4) AS attribution_weight,
    'time_decay' AS model_name
FROM raw_weights;

----------------------------------------------------------------------
-- 24. V_POSITION_BASED_ATTRIBUTION -- 40% first, 40% last, 20% middle
----------------------------------------------------------------------
CREATE OR REPLACE VIEW V_POSITION_BASED_ATTRIBUTION AS
SELECT
    conversion_id,
    customer_id,
    sub_channel,
    touchpoint_sequence,
    total_touchpoints,
    revenue,
    days_to_conversion,
    ROUND(CASE
        WHEN total_touchpoints = 1 THEN revenue
        WHEN total_touchpoints = 2 THEN revenue * 0.5
        WHEN touchpoint_sequence = 1 THEN revenue * 0.4
        WHEN touchpoint_sequence = total_touchpoints THEN revenue * 0.4
        ELSE revenue * 0.2 / (total_touchpoints - 2)
    END, 2) AS attributed_revenue,
    ROUND(CASE
        WHEN total_touchpoints = 1 THEN 1.0
        WHEN total_touchpoints = 2 THEN 0.5
        WHEN touchpoint_sequence = 1 THEN 0.4
        WHEN touchpoint_sequence = total_touchpoints THEN 0.4
        ELSE 0.2 / (total_touchpoints - 2)
    END, 4) AS attribution_weight,
    'position_based' AS model_name
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_MTA_TOUCHPOINTS;

----------------------------------------------------------------------
-- 25. V_CHANNEL_ATTRIBUTION_SUMMARY -- Unified view across all 5 models
----------------------------------------------------------------------
CREATE OR REPLACE VIEW V_CHANNEL_ATTRIBUTION_SUMMARY AS
SELECT
    sub_channel,
    model_name,
    ROUND(SUM(attributed_revenue), 2) AS total_attributed_revenue,
    COUNT(DISTINCT conversion_id) AS conversions_attributed
FROM (
    SELECT sub_channel, model_name, attributed_revenue, conversion_id FROM MARKETING_AI_BI.MARKETING_ANALYTICS.V_FIRST_TOUCH_ATTRIBUTION WHERE attributed_revenue > 0
    UNION ALL
    SELECT sub_channel, model_name, attributed_revenue, conversion_id FROM MARKETING_AI_BI.MARKETING_ANALYTICS.V_LAST_TOUCH_ATTRIBUTION WHERE attributed_revenue > 0
    UNION ALL
    SELECT sub_channel, model_name, attributed_revenue, conversion_id FROM MARKETING_AI_BI.MARKETING_ANALYTICS.V_LINEAR_ATTRIBUTION
    UNION ALL
    SELECT sub_channel, model_name, attributed_revenue, conversion_id FROM MARKETING_AI_BI.MARKETING_ANALYTICS.V_TIME_DECAY_ATTRIBUTION
    UNION ALL
    SELECT sub_channel, model_name, attributed_revenue, conversion_id FROM MARKETING_AI_BI.MARKETING_ANALYTICS.V_POSITION_BASED_ATTRIBUTION
)
GROUP BY sub_channel, model_name;

----------------------------------------------------------------------
-- 12. GEO_WEATHER_TRIGGERS -- Weather-based campaign trigger actions
--     Materialized from DT_GEO_TARGETING with trigger classification
----------------------------------------------------------------------
CREATE OR REPLACE TABLE GEO_WEATHER_TRIGGERS AS
SELECT
    g.state,
    g.zip_code,
    g.customer_count,
    g.recent_avg_temp,
    g.recent_total_precip,
    g.recent_total_snowfall,
    CASE
        WHEN COALESCE(g.recent_total_snowfall, 0) > 10
            THEN 'BLIZZARD ALERT — Push winter gear promos'
        WHEN COALESCE(g.recent_total_snowfall, 0) > 5
            THEN 'SNOW ADVISORY — Promote ski & outerwear'
        WHEN COALESCE(g.recent_total_precip, 0) > 5
            THEN 'HEAVY RAIN — Promote waterproof footwear'
        WHEN COALESCE(g.recent_total_precip, 0) > 2
            THEN 'RAIN ALERT — Promote rain gear & footwear'
        WHEN COALESCE(g.recent_avg_temp, 60) > 90
            THEN 'HEAT WAVE — Promote cooling & hydration gear'
        WHEN COALESCE(g.recent_avg_temp, 60) > 80
            THEN 'HOT WEATHER — Promote summer & camping gear'
        WHEN COALESCE(g.recent_avg_temp, 60) < 20
            THEN 'EXTREME COLD — Promote insulated outerwear'
        WHEN COALESCE(g.recent_avg_temp, 60) < 35
            THEN 'COLD SNAP — Promote layering & winter accessories'
        ELSE 'NO TRIGGER'
    END AS trigger_action
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_GEO_TARGETING g;
