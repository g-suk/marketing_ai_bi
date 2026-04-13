/*=============================================================================
  Summit Gear Co. -- Marketing AI+BI Lab
  05_ai/01_ai_functions.sql
  
  Applies Cortex AI functions and materializes results into tables.
  The Streamlit dashboard reads from these tables to avoid repeat AI calls.
  
  Functions:
    1. AI_SENTIMENT on product reviews
    2. AI_CLASSIFY on campaign performance
    3. AI_EXTRACT on review text
    4. AI_COMPLETE for campaign executive summaries
    5. AI_AGG for theme aggregation by channel
=============================================================================*/

USE DATABASE MARKETING_AI_BI;
USE SCHEMA MARKETING_RAW;
USE WAREHOUSE COMPUTE_WH;

----------------------------------------------------------------------
-- 1. AI_SENTIMENT -- Review sentiment scoring
----------------------------------------------------------------------
CREATE OR REPLACE TABLE AI_SENTIMENT_RESULTS AS
SELECT
    review_id,
    review_text,
    channel,
    product_category,
    product_name,
    rating,
    SNOWFLAKE.CORTEX.SENTIMENT(review_text) AS sentiment_score
FROM PRODUCT_REVIEWS;

----------------------------------------------------------------------
-- 2. AI_CLASSIFY -- Campaign performance tiers
----------------------------------------------------------------------
CREATE OR REPLACE TABLE AI_CLASSIFY_RESULTS AS
SELECT
    cm.campaign_id,
    cm.campaign_name,
    cm.channel,
    cm.sub_channel,
    cm.roas,
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        'Campaign "' || cm.campaign_name || '" achieved ROAS of ' || COALESCE(cm.roas::VARCHAR, 'N/A')
        || ', CPA of $' || COALESCE(cm.cpa::VARCHAR, 'N/A')
        || ', and ' || cm.total_conversions || ' conversions on a budget of $' || cm.budget || '.',
        ['Top Performer', 'Solid Performer', 'Average', 'Underperformer', 'Needs Review']
    ):label::VARCHAR AS performance_tier
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_CAMPAIGN_METRICS cm;

----------------------------------------------------------------------
-- 3. AI_EXTRACT -- Structured fields from review text
----------------------------------------------------------------------
CREATE OR REPLACE TABLE AI_EXTRACT_RESULTS AS
SELECT
    review_id,
    product_category,
    product_name,
    channel,
    rating,
    review_text,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
        review_text,
        'What specific product issue or praise is mentioned?'
    ) AS product_feedback,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
        review_text,
        'Does the reviewer mention any competitor brand or product?'
    ) AS competitor_mention,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
        review_text,
        'Would the reviewer recommend this product?'
    ) AS recommendation
FROM PRODUCT_REVIEWS;

----------------------------------------------------------------------
-- 4. AI_COMPLETE -- Executive campaign summaries
----------------------------------------------------------------------
CREATE OR REPLACE TABLE AI_COMPLETE_RESULTS AS
SELECT
    cm.campaign_id,
    cm.campaign_name,
    cm.channel,
    cm.sub_channel,
    SNOWFLAKE.CORTEX.COMPLETE(
        'snowflake-llama-3.3-70b',
        'Write a concise 2-sentence executive summary of this marketing campaign performance. '
        || 'Campaign: ' || cm.campaign_name
        || '. Channel: ' || cm.channel || ' (' || cm.sub_channel || ')'
        || '. Budget: $' || cm.budget::VARCHAR
        || '. Spend: $' || COALESCE(cm.total_spend::VARCHAR, '0')
        || '. Conversions: ' || COALESCE(cm.total_conversions::VARCHAR, '0')
        || '. Revenue: $' || COALESCE(cm.campaign_revenue::VARCHAR, '0')
        || '. ROAS: ' || COALESCE(cm.roas::VARCHAR, 'N/A')
        || '. CPA: $' || COALESCE(cm.cpa::VARCHAR, 'N/A') || '.'
    ) AS executive_summary
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_CAMPAIGN_METRICS cm;

----------------------------------------------------------------------
-- 5. AI_AGG -- Theme aggregation by channel
----------------------------------------------------------------------
CREATE OR REPLACE TABLE AI_AGG_RESULTS AS
SELECT
    channel,
    AI_SUMMARIZE_AGG(review_text) AS themes AS themes
FROM PRODUCT_REVIEWS
GROUP BY channel;
