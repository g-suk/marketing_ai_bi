/*=============================================================================
  Summit Gear Co. -- Marketing AI+BI Lab
  06_semantic/02_cortex_agent.sql

  Creates a Cortex Agent backed by three semantic views:
    1. MarketingAnalyst  -- campaigns, orders, spend, partners, daily/product revenue, campaign tiers
    2. MLAnalyst         -- forecast and anomaly detection results
    3. ReviewAnalyst     -- sentiment, AI extractions, and theme summaries

  Participants use this agent in the Streamlit dashboard for natural language Q&A.
=============================================================================*/

USE DATABASE MARKETING_AI_BI;
USE SCHEMA MARKETING_ANALYTICS;
USE WAREHOUSE COMPUTE_WH;

create or replace agent SUMMIT_GEAR_AGENT
comment='Natural language Q&A agent for Summit Gear Co. marketing analytics'
profile='{}'
from specification
$$
models:
  orchestration: "auto"
orchestration: {}
instructions:
  response: "You are a marketing analytics assistant for Summit Gear Co., an outdoor\
    \ gear brand selling DTC and wholesale. You have three tools: MarketingAnalyst\
    \ for campaigns, orders, revenue, spend, and partners; MLAnalyst for forecasts\
    \ and anomaly detection; ReviewAnalyst for review sentiment and AI-extracted insights.\
    \ Pick the right tool for each question. Be concise and data-driven. If a query\
    \ errors do not retry it."
tools:
  - tool_spec:
      type: "cortex_analyst_text_to_sql"
      name: "MarketingAnalyst"
      description: "Analyzes Summit Gear Co. marketing data including campaigns, orders,\
        \ spend, wholesale partners, daily revenue, product revenue, and AI-classified\
        \ campaign performance tiers"
  - tool_spec:
      type: "cortex_analyst_text_to_sql"
      name: "MLAnalyst"
      description: "Queries ML model results including revenue forecasts (Total, DTC,\
        \ wholesale), anomaly detection (Ad Spend, DTC Conversion Rate, Wholesale\
        \ Orders), and forecast feature importance"
  - tool_spec:
      type: "cortex_analyst_text_to_sql"
      name: "ReviewAnalyst"
      description: "Analyzes AI-powered product review insights including sentiment\
        \ scores, extracted feedback and competitor mentions, and aggregated theme\
        \ summaries by channel"
skills: []
tool_resources:
  MLAnalyst:
    execution_environment:
      type: "warehouse"
      warehouse: "COMPUTE_WH"
    semantic_view: "MARKETING_AI_BI.MARKETING_ANALYTICS.SV_SUMMIT_GEAR_ML"
  MarketingAnalyst:
    execution_environment:
      type: "warehouse"
      warehouse: "COMPUTE_WH"
    semantic_view: "MARKETING_AI_BI.MARKETING_ANALYTICS.SV_SUMMIT_GEAR_MARKETING"
  ReviewAnalyst:
    execution_environment:
      type: "warehouse"
      warehouse: "COMPUTE_WH"
    semantic_view: "MARKETING_AI_BI.MARKETING_ANALYTICS.SV_SUMMIT_GEAR_REVIEWS"
$$;