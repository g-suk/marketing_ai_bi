/*=============================================================================
  Summit Gear Co. -- Marketing AI+BI Lab
  06_semantic/01_semantic_view.sql

  Creates three semantic views that collectively cover all dashboard data:
    1. SV_SUMMIT_GEAR_MARKETING  -- core transactional model + daily/product aggregates + campaign classification
    2. SV_SUMMIT_GEAR_ML         -- forecast and anomaly detection results
    3. SV_SUMMIT_GEAR_REVIEWS    -- AI-powered review analysis

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
      COMMENT = 'AI-classified campaign performance tiers'
  )
  RELATIONSHIPS (
    orders (campaign_id) REFERENCES campaigns (campaign_id),
    orders (customer_id) REFERENCES customers (customer_id),
    orders (wholesale_partner_id) REFERENCES partners (partner_id),
    spend (campaign_id) REFERENCES campaigns (campaign_id),
    campaign_metrics (campaign_id) REFERENCES campaigns (campaign_id),
    partner_perf (partner_id) REFERENCES partners (partner_id),
    campaign_tiers (campaign_id) REFERENCES campaigns (campaign_id)
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
    campaign_tiers.performance_tier AS campaign_tiers.performance_tier COMMENT = 'AI-assigned tier: high, medium, or low'
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
