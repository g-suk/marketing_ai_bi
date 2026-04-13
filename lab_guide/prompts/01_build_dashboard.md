# Build the Dashboard

Paste this single prompt into Cortex Code:

---

Build a Streamlit app for "Summit Gear Co." -- an outdoor gear brand selling DTC and wholesale. Use `get_active_session()` to connect. All data lives in `MARKETING_AI_BI.MARKETING_ANALYTICS`:

**Dynamic tables:**
- DT_DAILY_REVENUE (order_date, channel, order_count, total_units, total_revenue, avg_order_value)
- DT_CAMPAIGN_METRICS (campaign_id, campaign_name, channel, sub_channel, budget, total_spend, total_impressions, total_clicks, total_conversions, campaign_revenue, cpa, roas, ctr_pct)
- DT_PARTNER_PERFORMANCE (partner_id, partner_name, region, tier, avg_sell_through_rate, total_orders, total_units_sold, total_revenue, avg_order_value)
- DT_PRODUCT_REVENUE (product_category, product_name, channel, order_count, total_units, total_revenue, avg_order_value)

**ML result tables:**
- FORECAST_RESULTS (series, ts, forecast, lower_bound, upper_bound)
- FORECAST_FEATURE_IMPORTANCE
- ANOMALY_DETECTION_RESULTS (series, ts, y, forecast, lower_bound, upper_bound, is_anomaly, percentile)

**AI result tables:**
- AI_SENTIMENT_RESULTS (review_id, review_text, channel, product_category, rating, sentiment_score)
- AI_CLASSIFY_RESULTS (campaign_id, campaign_name, channel, sub_channel, roas, performance_tier)
- AI_AGG_RESULTS (channel, themes)
- AI_EXTRACT_RESULTS (review_id, product_category, product_name, channel, rating, review_text, product_feedback, competitor_mention, recommendation)

**Cortex Agent:** `SUMMIT_GEAR_AGENT` in `MARKETING_AI_BI.MARKETING_ANALYTICS` -- backed by 3 semantic views covering all tables above. Call it with `SNOWFLAKE.CORTEX.DATA_AGENT_RUN('MARKETING_AI_BI.MARKETING_ANALYTICS.SUMMIT_GEAR_AGENT', question)`.

Create a sidebar-navigated app with these 5 pages:

1. **KPI Overview** -- metric cards (total revenue, DTC revenue, wholesale revenue, total orders, avg order value, ROAS from DT_CAMPAIGN_METRICS), daily revenue line chart by channel with date range filter from DT_DAILY_REVENUE, revenue by product category bar chart from DT_PRODUCT_REVENUE.

2. **Channel Deep Dive** -- DTC: scatter of spend vs conversions by sub-channel sized by ROAS, campaign metrics table from DT_CAMPAIGN_METRICS. Wholesale: top 15 partner sell-through bar chart from DT_PARTNER_PERFORMANCE, trade promo spend vs revenue comparison.

3. **Forecasting & Anomalies** -- Forecast: plot actuals from DT_DAILY_REVENUE with forecast overlay and confidence interval band from FORECAST_RESULTS, selectbox for series (Total/DTC/wholesale). Anomaly: plot time series with red markers on anomaly points from ANOMALY_DETECTION_RESULTS, selectbox for series (Ad Spend/DTC Conversion Rate/Wholesale Orders).

4. **AI Insights** -- sentiment histogram by channel from AI_SENTIMENT_RESULTS, theme summaries side-by-side from AI_AGG_RESULTS, campaign performance tier donut chart from AI_CLASSIFY_RESULTS, expandable AI extract table from AI_EXTRACT_RESULTS.

5. **Marketing Agent** -- chat interface calling `SNOWFLAKE.CORTEX.DATA_AGENT_RUN('MARKETING_AI_BI.MARKETING_ANALYTICS.SUMMIT_GEAR_AGENT', question)`. The agent has 3 semantic views: MarketingAnalyst (campaigns, orders, spend, partners, daily/product revenue, campaign tiers), MLAnalyst (forecasts, anomalies), ReviewAnalyst (sentiment, extractions, themes). Show suggested starter questions as clickable buttons and display the agent's SQL in an expander.

Use plotly for charts. Keep the layout clean with wide mode.
