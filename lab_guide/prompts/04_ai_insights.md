# Prompt 4: AI Insights

Paste this into Cortex Code:

---

Add a fourth page called "AI Insights" to my Streamlit app.

- Show a sentiment distribution chart (histogram or violin plot) comparing DTC reviews vs wholesale channel reviews. Query from AI_SENTIMENT_RESULTS (columns: review_id, review_text, channel, product_category, product_name, rating, sentiment_score).
- Display the AI_AGG theme summaries for DTC and wholesale side by side in two columns. Query from AI_AGG_RESULTS (columns: channel, themes).
- Show a pie or donut chart of campaign performance tiers from AI_CLASSIFY_RESULTS (columns: campaign_id, campaign_name, channel, sub_channel, roas, performance_tier).
- Show a sample of AI_EXTRACT results in an expandable table -- the structured fields extracted from reviews.

Use tables in MARKETING_AI_BI.MARKETING_RAW for AI results and MARKETING_AI_BI.MARKETING_ANALYTICS for dynamic tables.
