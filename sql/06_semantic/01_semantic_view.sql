/*=============================================================================
  Summit Gear Co. -- Marketing AI+BI Lab
  06_semantic/01_semantic_view.sql
  
  Creates a semantic view over the marketing data model.
  This view backs the Cortex Agent for natural language Q&A.
  
  Requires: All source tables, dynamic tables, and AI result tables to exist.
=============================================================================*/

USE DATABASE MARKETING_AI_BI;
USE SCHEMA DEMO_DATA;
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE SEMANTIC VIEW SV_SUMMIT_GEAR_MARKETING
  TABLES (
    campaigns AS MARKETING_AI_BI.DEMO_DATA.CAMPAIGNS
      PRIMARY KEY (campaign_id)
      COMMENT = 'Marketing campaigns across DTC and wholesale channels',
    orders AS MARKETING_AI_BI.DEMO_DATA.ORDERS
      PRIMARY KEY (order_id)
      COMMENT = 'Customer orders across DTC and wholesale channels',
    customers AS MARKETING_AI_BI.DEMO_DATA.CUSTOMERS
      PRIMARY KEY (customer_id)
      COMMENT = 'Customer profiles with demographics',
    spend AS MARKETING_AI_BI.DEMO_DATA.MARKETING_SPEND
      PRIMARY KEY (spend_id)
      COMMENT = 'Daily marketing spend by campaign',
    partners AS MARKETING_AI_BI.DEMO_DATA.WHOLESALE_PARTNERS
      PRIMARY KEY (partner_id)
      COMMENT = 'Wholesale retail partner profiles',
    campaign_metrics AS MARKETING_AI_BI.DEMO_DATA.DT_CAMPAIGN_METRICS
      PRIMARY KEY (campaign_id)
      COMMENT = 'Campaign-level KPIs including spend, conversions, CPA, ROAS',
    partner_perf AS MARKETING_AI_BI.DEMO_DATA.DT_PARTNER_PERFORMANCE
      PRIMARY KEY (partner_id)
      COMMENT = 'Wholesale partner performance metrics'
  )
  RELATIONSHIPS (
    orders (campaign_id) REFERENCES campaigns (campaign_id),
    orders (customer_id) REFERENCES customers (customer_id),
    orders (wholesale_partner_id) REFERENCES partners (partner_id),
    spend (campaign_id) REFERENCES campaigns (campaign_id),
    campaign_metrics (campaign_id) REFERENCES campaigns (campaign_id),
    partner_perf (partner_id) REFERENCES partners (partner_id)
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
    customers.lifetime_value AS customers.lifetime_value COMMENT = 'Customer lifetime value'
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
    spend.spend_date AS spend.spend_date COMMENT = 'Date of the spend'
  )
  METRICS (
    orders.total_revenue AS SUM(orders.revenue) COMMENT = 'Sum of order revenue',
    orders.total_orders AS COUNT(orders.order_id) COMMENT = 'Count of orders',
    orders.avg_order_value AS AVG(orders.revenue) COMMENT = 'Average order value',
    spend.total_spend AS SUM(spend.amount) COMMENT = 'Sum of daily spend',
    spend.total_conversions AS SUM(spend.conversions) COMMENT = 'Sum of conversions',
    spend.ctr AS DIV0(SUM(spend.clicks), SUM(spend.impressions)) * 100 COMMENT = 'Click-through rate pct'
  )
  COMMENT = 'Semantic model for Summit Gear Co. marketing analytics';
