/*=============================================================================
  Summit Gear Co. -- Marketing AI+BI Lab
  06_semantic/02_cortex_agent.sql
  
  Creates a Cortex Agent backed by the semantic view.
  Participants use this agent in the Streamlit dashboard for natural language Q&A.
=============================================================================*/

USE DATABASE MARKETING_AI_BI;
USE SCHEMA MARKETING_ANALYTICS;
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE AGENT SUMMIT_GEAR_AGENT
  COMMENT = 'Natural language Q&A agent for Summit Gear Co. marketing analytics'
  FROM SPECIFICATION
  $$
  tools:
    - tool_spec:
        type: "cortex_analyst_text_to_sql"
        name: "MarketingAnalyst"
        description: "Analyzes Summit Gear Co. marketing data including campaigns, orders, spend, and wholesale partners"

  tool_resources:
    MarketingAnalyst:
      semantic_view: "MARKETING_AI_BI.MARKETING_ANALYTICS.SV_SUMMIT_GEAR_MARKETING"

  instructions:
    response: "You are a marketing analytics assistant for Summit Gear Co., an outdoor gear brand. Answer questions about campaign performance, revenue trends, channel metrics, and partner data. Be concise and data-driven."
  $$;
