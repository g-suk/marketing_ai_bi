/*=============================================================================
  Summit Gear Co. -- Marketing AI+BI Lab
  06_semantic/01_semantic_view.sql

  Creates four semantic views that collectively cover all dashboard data:
    1. SV_SUMMIT_GEAR_MARKETING  -- core transactional model + daily/product aggregates + campaign classification + AI summaries
    2. SV_SUMMIT_GEAR_ML         -- forecast and anomaly detection results
    3. SV_SUMMIT_GEAR_REVIEWS    -- AI-powered review analysis
    4. SV_SUMMIT_GEAR_ADVANCED   -- MMM, geo-targeting, CLV, weather impact

  These views back the Cortex Agent for natural language Q&A.

  Requires: All source tables, dynamic tables, ML result tables,
            and AI result tables to exist.
=============================================================================*/

USE DATABASE MARKETING_AI_BI;
USE SCHEMA MARKETING_ANALYTICS;
USE WAREHOUSE COMPUTE_WH;

----------------------------------------------------------------------
-- 1. SV_SUMMIT_GEAR_MARKETING
--    Core transactional model: campaigns, orders, customers, spend,
--    partners, plus pre-aggregated daily revenue, product revenue,
--    and AI campaign classification.
----------------------------------------------------------------------
CREATE OR REPLACE SEMANTIC VIEW SV_SUMMIT_GEAR_MARKETING
  TABLES (
    campaigns AS MARKETING_AI_BI.MARKETING_RAW.CAMPAIGNS
      PRIMARY KEY (campaign_id)
      COMMENT = 'Marketing campaigns across DTC and wholesale channels',
    orders AS MARKETING_AI_BI.MARKETING_RAW.ORDERS
      PRIMARY KEY (order_id)
      COMMENT = 'Customer orders across DTC and wholesale channels',
    customers AS MARKETING_AI_BI.MARKETING_RAW.CUSTOMERS
      PRIMARY KEY (customer_id)
      COMMENT = 'Customer profiles with demographics',
    spend AS MARKETING_AI_BI.MARKETING_RAW.MARKETING_SPEND
      PRIMARY KEY (spend_id)
      COMMENT = 'Daily marketing spend by campaign',
    partners AS MARKETING_AI_BI.MARKETING_RAW.WHOLESALE_PARTNERS
      PRIMARY KEY (partner_id)
      COMMENT = 'Wholesale retail partner profiles',
    campaign_metrics AS MARKETING_AI_BI.MARKETING_ANALYTICS.DT_CAMPAIGN_METRICS
      PRIMARY KEY (campaign_id)
      COMMENT = 'Campaign-level KPIs including spend, conversions, CPA, ROAS',
    partner_perf AS MARKETING_AI_BI.MARKETING_ANALYTICS.DT_PARTNER_PERFORMANCE
      PRIMARY KEY (partner_id)
      COMMENT = 'Wholesale partner performance metrics',
    daily_revenue AS MARKETING_AI_BI.MARKETING_ANALYTICS.DT_DAILY_REVENUE
      COMMENT = 'Daily revenue aggregated by channel',
    product_revenue AS MARKETING_AI_BI.MARKETING_ANALYTICS.DT_PRODUCT_REVENUE
      COMMENT = 'Revenue aggregated by product category and channel',
    campaign_tiers AS MARKETING_AI_BI.MARKETING_ANALYTICS.AI_CLASSIFY_RESULTS
      PRIMARY KEY (campaign_id)
      COMMENT = 'AI-classified campaign performance tiers',
    campaign_summaries AS MARKETING_AI_BI.MARKETING_ANALYTICS.AI_COMPLETE_RESULTS
      PRIMARY KEY (campaign_id)
      COMMENT = 'AI-generated executive summaries per campaign'
  )
  RELATIONSHIPS (
    orders (campaign_id) REFERENCES campaigns (campaign_id),
    orders (customer_id) REFERENCES customers (customer_id),
    orders (wholesale_partner_id) REFERENCES partners (partner_id),
    spend (campaign_id) REFERENCES campaigns (campaign_id),
    campaign_metrics (campaign_id) REFERENCES campaigns (campaign_id),
    partner_perf (partner_id) REFERENCES partners (partner_id),
    campaign_tiers (campaign_id) REFERENCES campaigns (campaign_id),
    campaign_summaries (campaign_id) REFERENCES campaigns (campaign_id)
  )
  FACTS (
    orders.revenue AS orders.revenue COMMENT = 'Order revenue in USD',
    orders.quantity AS orders.quantity COMMENT = 'Units ordered',
    spend.amount AS spend.amount COMMENT = 'Daily spend amount in USD',
    spend.impressions AS spend.impressions,
    spend.clicks AS spend.clicks,
    spend.conversions AS spend.conversions,
    campaigns.budget AS campaigns.budget COMMENT = 'Campaign budget in USD',
    campaign_metrics.total_spend AS campaign_metrics.total_spend COMMENT = 'Total campaign spend',
    campaign_metrics.campaign_revenue AS campaign_metrics.campaign_revenue COMMENT = 'Revenue attributed to campaign',
    campaign_metrics.cpa AS campaign_metrics.cpa COMMENT = 'Cost per acquisition',
    campaign_metrics.roas AS campaign_metrics.roas COMMENT = 'Return on ad spend',
    partner_perf.total_revenue AS partner_perf.total_revenue COMMENT = 'Total partner revenue',
    partner_perf.total_orders AS partner_perf.total_orders COMMENT = 'Total partner orders',
    partner_perf.total_units_sold AS partner_perf.total_units_sold COMMENT = 'Total units sold by partner',
    partner_perf.avg_sell_through_rate AS partner_perf.avg_sell_through_rate COMMENT = 'Partner sell-through rate',
    customers.lifetime_value AS customers.lifetime_value COMMENT = 'Customer lifetime value',
    daily_revenue.order_count AS daily_revenue.order_count COMMENT = 'Daily order count',
    daily_revenue.total_units AS daily_revenue.total_units COMMENT = 'Daily units sold',
    daily_revenue.total_revenue AS daily_revenue.total_revenue COMMENT = 'Daily total revenue',
    daily_revenue.avg_order_value AS daily_revenue.avg_order_value COMMENT = 'Daily average order value',
    product_revenue.order_count AS product_revenue.order_count COMMENT = 'Product order count',
    product_revenue.total_units AS product_revenue.total_units COMMENT = 'Product units sold',
    product_revenue.total_revenue AS product_revenue.total_revenue COMMENT = 'Product total revenue',
    product_revenue.avg_order_value AS product_revenue.avg_order_value COMMENT = 'Product average order value',
    campaign_tiers.roas AS campaign_tiers.roas COMMENT = 'ROAS used for tier classification'
  )
  DIMENSIONS (
    orders.order_date AS orders.order_date COMMENT = 'Date the order was placed',
    orders.channel AS orders.channel COMMENT = 'DTC or wholesale',
    orders.product_category AS orders.product_category COMMENT = 'outerwear, footwear, camping, climbing, winter_sports, or accessories',
    orders.product_name AS orders.product_name COMMENT = 'Name of the product',
    campaigns.campaign_name AS campaigns.campaign_name COMMENT = 'Campaign name',
    campaigns.channel AS campaigns.channel COMMENT = 'DTC or wholesale',
    campaigns.sub_channel AS campaigns.sub_channel COMMENT = 'email, social, search, influencer, trade_promo, co_op_ad, distributor_event',
    customers.state AS customers.state COMMENT = 'US state abbreviation',
    customers.age AS customers.age,
    customers.gender AS customers.gender,
    customers.channel_preference AS customers.channel_preference COMMENT = 'DTC, wholesale, or both',
    partners.partner_name AS partners.partner_name COMMENT = 'Retail partner name',
    partners.region AS partners.region COMMENT = 'US region',
    partners.tier AS partners.tier COMMENT = 'gold, silver, or bronze',
    spend.spend_date AS spend.spend_date COMMENT = 'Date of the spend',
    daily_revenue.order_date AS daily_revenue.order_date COMMENT = 'Date for daily revenue',
    daily_revenue.channel AS daily_revenue.channel COMMENT = 'DTC or wholesale',
    product_revenue.product_category AS product_revenue.product_category COMMENT = 'Product category',
    product_revenue.product_name AS product_revenue.product_name COMMENT = 'Product name',
    product_revenue.channel AS product_revenue.channel COMMENT = 'DTC or wholesale',
    campaign_tiers.campaign_name AS campaign_tiers.campaign_name COMMENT = 'Campaign name from classification',
    campaign_tiers.channel AS campaign_tiers.channel COMMENT = 'Campaign channel',
    campaign_tiers.sub_channel AS campaign_tiers.sub_channel COMMENT = 'Campaign sub-channel',
    campaign_tiers.performance_tier AS campaign_tiers.performance_tier COMMENT = 'AI-assigned tier: high, medium, or low',
    campaign_summaries.campaign_name AS campaign_summaries.campaign_name COMMENT = 'Campaign name for executive summary',
    campaign_summaries.channel AS campaign_summaries.channel COMMENT = 'Campaign channel',
    campaign_summaries.sub_channel AS campaign_summaries.sub_channel COMMENT = 'Campaign sub-channel',
    campaign_summaries.executive_summary AS campaign_summaries.executive_summary COMMENT = 'AI-generated 2-sentence executive summary of campaign performance'
  )
  METRICS (
    orders.total_revenue AS SUM(orders.revenue) COMMENT = 'Sum of order revenue',
    orders.total_orders AS COUNT(orders.order_id) COMMENT = 'Count of orders',
    orders.avg_order_value AS AVG(orders.revenue) COMMENT = 'Average order value',
    spend.total_spend AS SUM(spend.amount) COMMENT = 'Sum of daily spend',
    spend.total_conversions AS SUM(spend.conversions) COMMENT = 'Sum of conversions',
    spend.ctr AS DIV0(SUM(spend.clicks), SUM(spend.impressions)) * 100 COMMENT = 'Click-through rate pct'
  )
  COMMENT = 'Core marketing analytics model for Summit Gear Co.';

----------------------------------------------------------------------
-- 2. SV_SUMMIT_GEAR_ML
--    ML results: revenue forecasts, anomaly detection, feature importance.
----------------------------------------------------------------------
CREATE OR REPLACE SEMANTIC VIEW SV_SUMMIT_GEAR_ML
  TABLES (
    forecasts AS MARKETING_AI_BI.MARKETING_ANALYTICS.FORECAST_RESULTS
      COMMENT = 'Revenue and spend forecast results by model (total_revenue, product_channel, spend_subchannel)',
    anomalies AS MARKETING_AI_BI.MARKETING_ANALYTICS.ANOMALY_DETECTION_RESULTS
      COMMENT = 'Anomaly detection results by model (spend_subchannel, dtc_conversion, wholesale_product)',
    feature_importance AS MARKETING_AI_BI.MARKETING_ANALYTICS.FORECAST_FEATURE_IMPORTANCE
      COMMENT = 'Feature importance scores from the product x channel forecast model'
  )
  FACTS (
    forecasts.forecast AS forecasts.forecast COMMENT = 'Forecasted value',
    forecasts.lower_bound AS forecasts.lower_bound COMMENT = 'Lower bound of forecast confidence interval',
    forecasts.upper_bound AS forecasts.upper_bound COMMENT = 'Upper bound of forecast confidence interval',
    anomalies.y AS anomalies.y COMMENT = 'Actual observed value',
    anomalies.forecast AS anomalies.forecast COMMENT = 'Expected value from anomaly model',
    anomalies.lower_bound AS anomalies.lower_bound COMMENT = 'Lower bound of expected range',
    anomalies.upper_bound AS anomalies.upper_bound COMMENT = 'Upper bound of expected range',
    anomalies.percentile AS anomalies.percentile COMMENT = 'Percentile of observed value',
    feature_importance.score AS feature_importance.score COMMENT = 'Feature importance score',
    feature_importance.rank AS feature_importance.rank COMMENT = 'Feature importance rank'
  )
  DIMENSIONS (
    forecasts.model_name AS forecasts.model_name COMMENT = 'Forecast model: total_revenue, product_channel, or spend_subchannel',
    forecasts.series AS forecasts.series COMMENT = 'Forecast series: Total, {product_category}_{channel}, or sub_channel name',
    forecasts.ts AS forecasts.ts COMMENT = 'Forecast timestamp',
    anomalies.model_name AS anomalies.model_name COMMENT = 'Anomaly model: spend_subchannel, dtc_conversion, or wholesale_product',
    anomalies.series AS anomalies.series COMMENT = 'Anomaly series: sub_channel name, DTC Conversion Rate, or product_category',
    anomalies.ts AS anomalies.ts COMMENT = 'Anomaly detection timestamp',
    anomalies.is_anomaly AS anomalies.is_anomaly COMMENT = 'True if the data point is an anomaly',
    feature_importance.feature_name AS feature_importance.feature_name COMMENT = 'Name of the feature',
    feature_importance.feature_type AS feature_importance.feature_type COMMENT = 'Type of the feature'
  )
  METRICS (
    anomalies.anomaly_count AS COUNT_IF(anomalies.is_anomaly = TRUE) COMMENT = 'Total number of anomalies detected'
  )
  COMMENT = 'ML forecast and anomaly detection results for Summit Gear Co.';

----------------------------------------------------------------------
-- 3. SV_SUMMIT_GEAR_REVIEWS
--    AI-powered review analysis: sentiment, extraction, and themes.
----------------------------------------------------------------------
CREATE OR REPLACE SEMANTIC VIEW SV_SUMMIT_GEAR_REVIEWS
  TABLES (
    sentiment AS MARKETING_AI_BI.MARKETING_ANALYTICS.AI_SENTIMENT_RESULTS
      PRIMARY KEY (review_id)
      COMMENT = 'Sentiment scores for product reviews',
    extracts AS MARKETING_AI_BI.MARKETING_ANALYTICS.AI_EXTRACT_RESULTS
      PRIMARY KEY (review_id)
      COMMENT = 'AI-extracted product feedback, competitor mentions, and recommendations',
    themes AS MARKETING_AI_BI.MARKETING_ANALYTICS.AI_AGG_RESULTS
      COMMENT = 'Aggregated review theme summaries by channel'
  )
  RELATIONSHIPS (
    extracts (review_id) REFERENCES sentiment (review_id)
  )
  FACTS (
    sentiment.sentiment_score AS sentiment.sentiment_score COMMENT = 'Sentiment score from -1 (negative) to 1 (positive)',
    sentiment.rating AS sentiment.rating COMMENT = 'Customer star rating',
    extracts.rating AS extracts.rating COMMENT = 'Customer star rating from extract source'
  )
  DIMENSIONS (
    sentiment.review_text AS sentiment.review_text COMMENT = 'Full review text',
    sentiment.channel AS sentiment.channel COMMENT = 'DTC or wholesale',
    sentiment.product_category AS sentiment.product_category COMMENT = 'Product category',
    extracts.product_category AS extracts.product_category COMMENT = 'Product category',
    extracts.product_name AS extracts.product_name COMMENT = 'Product name',
    extracts.channel AS extracts.channel COMMENT = 'DTC or wholesale',
    extracts.review_text AS extracts.review_text COMMENT = 'Full review text',
    extracts.product_feedback AS extracts.product_feedback COMMENT = 'AI-extracted product feedback',
    extracts.competitor_mention AS extracts.competitor_mention COMMENT = 'AI-extracted competitor mentions',
    extracts.recommendation AS extracts.recommendation COMMENT = 'AI-extracted recommendation',
    themes.channel AS themes.channel COMMENT = 'DTC or wholesale',
    themes.themes AS themes.themes COMMENT = 'AI-generated theme summary for the channel'
  )
  METRICS (
    sentiment.avg_sentiment AS AVG(sentiment.sentiment_score) COMMENT = 'Average sentiment score',
    sentiment.review_count AS COUNT(sentiment.review_id) COMMENT = 'Number of reviews',
    sentiment.avg_rating AS AVG(sentiment.rating) COMMENT = 'Average star rating'
  )
  COMMENT = 'AI-powered review analysis for Summit Gear Co.';

----------------------------------------------------------------------
-- 4. SV_SUMMIT_GEAR_ADVANCED
--    Advanced analytics: MMM, geo-targeting, CLV, weather impact.
----------------------------------------------------------------------
CREATE OR REPLACE SEMANTIC VIEW SV_SUMMIT_GEAR_ADVANCED
  TABLES (
    mmm_contributions AS MARKETING_AI_BI.MARKETING_ANALYTICS.MMM_CHANNEL_CONTRIBUTIONS
      COMMENT = 'Quarterly marketing mix model channel contributions and ROI',
    mmm_weekly AS MARKETING_AI_BI.MARKETING_ANALYTICS.MMM_WEEKLY_DECOMPOSITION
      COMMENT = 'Weekly marketing mix decomposition with spend and attributed revenue',
    mmm_insights AS MARKETING_AI_BI.MARKETING_ANALYTICS.MMM_AI_INSIGHTS
      COMMENT = 'AI-generated marketing mix model insights',
    geo_profiles AS MARKETING_AI_BI.MARKETING_ANALYTICS.GEO_TARGETING_PROFILES
      COMMENT = 'Zip-code level geo-targeting profiles with composite scoring',
    geo_triggers AS MARKETING_AI_BI.MARKETING_ANALYTICS.GEO_WEATHER_TRIGGERS
      COMMENT = 'Weather-triggered campaign rules by zip code',
    geo_recommendations AS MARKETING_AI_BI.MARKETING_ANALYTICS.GEO_AI_RECOMMENDATIONS
      COMMENT = 'AI-generated per-state targeting recommendations',
    clv_risk AS MARKETING_AI_BI.MARKETING_ANALYTICS.CLV_RISK_CLASSIFICATION
      PRIMARY KEY (customer_id)
      COMMENT = 'Customer lifetime value tiers and churn risk scores',
    weather_revenue AS MARKETING_AI_BI.MARKETING_ANALYTICS.DT_WEATHER_REVENUE
      COMMENT = 'Daily revenue with weather conditions and temperature bands',
    customer_enriched AS MARKETING_AI_BI.MARKETING_ANALYTICS.DT_CUSTOMER_ENRICHED
      PRIMARY KEY (customer_id)
      COMMENT = 'Enriched customer profiles with marketplace data, purchase behavior, and demographics'
  )
  RELATIONSHIPS (
    clv_risk (customer_id) REFERENCES customer_enriched (customer_id)
  )
  FACTS (
    mmm_contributions.total_spend AS mmm_contributions.total_spend COMMENT = 'Quarterly channel spend',
    mmm_contributions.attributed_revenue AS mmm_contributions.attributed_revenue COMMENT = 'Revenue attributed to channel',
    mmm_contributions.roi AS mmm_contributions.roi COMMENT = 'Channel return on investment',
    mmm_contributions.share_of_spend AS mmm_contributions.share_of_spend COMMENT = 'Channel share of total spend pct',
    mmm_contributions.cost_per_conversion AS mmm_contributions.cost_per_conversion COMMENT = 'Cost per conversion for channel',
    mmm_weekly.spend AS mmm_weekly.spend COMMENT = 'Weekly channel spend',
    mmm_weekly.attributed_revenue AS mmm_weekly.attributed_revenue COMMENT = 'Weekly attributed revenue',
    mmm_weekly.incremental_revenue AS mmm_weekly.incremental_revenue COMMENT = 'Weekly incremental (non-organic) revenue',
    mmm_weekly.efficiency_index AS mmm_weekly.efficiency_index COMMENT = 'Revenue efficiency index for spend',
    geo_profiles.customer_count AS geo_profiles.customer_count COMMENT = 'Customers in zip code',
    geo_profiles.avg_ltv AS geo_profiles.avg_ltv COMMENT = 'Average LTV for zip code',
    geo_profiles.total_revenue AS geo_profiles.total_revenue COMMENT = 'Total revenue for zip code',
    geo_profiles.targeting_score AS geo_profiles.targeting_score COMMENT = 'Composite targeting score 0-1',
    clv_risk.lifetime_value AS clv_risk.lifetime_value COMMENT = 'Customer lifetime value',
    clv_risk.total_orders AS clv_risk.total_orders COMMENT = 'Total orders placed',
    clv_risk.total_revenue AS clv_risk.total_revenue COMMENT = 'Total revenue from customer',
    clv_risk.churn_risk_score AS clv_risk.churn_risk_score COMMENT = 'Churn risk score 0-1',
    clv_risk.days_since_last_order AS clv_risk.days_since_last_order COMMENT = 'Days since most recent order',
    weather_revenue.total_revenue AS weather_revenue.total_revenue COMMENT = 'Daily revenue',
    weather_revenue.national_avg_temp_f AS weather_revenue.national_avg_temp_f COMMENT = 'National average temperature F',
    customer_enriched.lifetime_value AS customer_enriched.lifetime_value COMMENT = 'Customer lifetime value',
    customer_enriched.avg_order_value AS customer_enriched.avg_order_value COMMENT = 'Average order value',
    customer_enriched.orders_per_month AS customer_enriched.orders_per_month COMMENT = 'Order frequency per month'
  )
  DIMENSIONS (
    mmm_contributions.sub_channel AS mmm_contributions.sub_channel COMMENT = 'Marketing sub-channel',
    mmm_contributions.period AS mmm_contributions.period COMMENT = 'Quarter period like Q1_2025',
    mmm_weekly.week_start AS mmm_weekly.week_start COMMENT = 'Week start date',
    mmm_weekly.sub_channel AS mmm_weekly.sub_channel COMMENT = 'Marketing sub-channel',
    geo_profiles.state AS geo_profiles.state COMMENT = 'US state abbreviation',
    geo_profiles.zip_code AS geo_profiles.zip_code COMMENT = 'ZIP code',
    geo_profiles.dominant_lifestyle AS geo_profiles.dominant_lifestyle COMMENT = 'Most common lifestyle segment in zip',
    geo_profiles.dominant_outdoor_interest AS geo_profiles.dominant_outdoor_interest COMMENT = 'Most common outdoor interest in zip',
    geo_profiles.weather_recommended_category AS geo_profiles.weather_recommended_category COMMENT = 'Product category recommended based on weather',
    geo_profiles.recommended_channel AS geo_profiles.recommended_channel COMMENT = 'Recommended marketing channel for zip',
    geo_triggers.trigger_action AS geo_triggers.trigger_action COMMENT = 'Weather-triggered campaign action',
    geo_triggers.recommended_product_category AS geo_triggers.recommended_product_category COMMENT = 'Product category triggered by weather',
    geo_recommendations.state AS geo_recommendations.state COMMENT = 'State for AI recommendation',
    geo_recommendations.recommendation AS geo_recommendations.recommendation COMMENT = 'AI-generated state targeting recommendation',
    clv_risk.clv_tier AS clv_risk.clv_tier COMMENT = 'CLV tier: Loyal High-Value, Growth Potential, At-Risk, New Customer, or Lapsed',
    clv_risk.state AS clv_risk.state COMMENT = 'Customer state',
    clv_risk.age_group AS clv_risk.age_group COMMENT = 'Customer age group',
    clv_risk.value_tier AS clv_risk.value_tier COMMENT = 'high_value, mid_value, or low_value',
    clv_risk.lifestyle_segment AS clv_risk.lifestyle_segment COMMENT = 'Customer lifestyle segment from marketplace',
    clv_risk.outdoor_interest AS clv_risk.outdoor_interest COMMENT = 'Customer outdoor interest from marketplace',
    weather_revenue.order_date AS weather_revenue.order_date COMMENT = 'Order date',
    weather_revenue.channel AS weather_revenue.channel COMMENT = 'DTC or wholesale',
    weather_revenue.temp_band AS weather_revenue.temp_band COMMENT = 'Temperature band: freezing, cold, mild, warm, hot',
    weather_revenue.rainy_day AS weather_revenue.rainy_day COMMENT = 'Was it a rainy day',
    weather_revenue.snow_day AS weather_revenue.snow_day COMMENT = 'Was it a snow day',
    customer_enriched.state AS customer_enriched.state COMMENT = 'Customer state',
    customer_enriched.age_group AS customer_enriched.age_group COMMENT = 'Customer age group',
    customer_enriched.value_tier AS customer_enriched.value_tier COMMENT = 'Customer value tier',
    customer_enriched.lifestyle_segment AS customer_enriched.lifestyle_segment COMMENT = 'Customer lifestyle from marketplace',
    customer_enriched.outdoor_interest AS customer_enriched.outdoor_interest COMMENT = 'Customer outdoor interest from marketplace'
  )
  METRICS (
    mmm_contributions.avg_roi AS AVG(mmm_contributions.roi) COMMENT = 'Average ROI across channels',
    mmm_contributions.total_attributed_revenue AS SUM(mmm_contributions.attributed_revenue) COMMENT = 'Sum of attributed revenue',
    geo_profiles.avg_targeting_score AS AVG(geo_profiles.targeting_score) COMMENT = 'Average targeting score',
    clv_risk.at_risk_count AS COUNT_IF(clv_risk.churn_risk_score >= 0.7) COMMENT = 'Number of at-risk or lapsed customers',
    clv_risk.avg_churn_risk AS AVG(clv_risk.churn_risk_score) COMMENT = 'Average churn risk score',
    weather_revenue.avg_daily_revenue AS AVG(weather_revenue.total_revenue) COMMENT = 'Average daily revenue'
  )
  COMMENT = 'Advanced analytics: Marketing Mix Modeling, Geo-Targeting, CLV Risk, and Weather Impact for Summit Gear Co.';
