# Build the Dashboard

Paste this single prompt into Cortex Code:

---

Build a Streamlit app for "Summit Gear Co." -- an outdoor gear brand selling DTC and wholesale. Use `get_active_session()` to connect. Use these schemas:

- **Raw data:** `MARKETING_AI_BI.MARKETING_RAW` -- tables: ORDERS (order_id, customer_id, order_date, channel, product_category, product_name, quantity, revenue, wholesale_partner_id), MARKETING_SPEND (spend_id, campaign_id, spend_date, channel, sub_channel, amount, impressions, clicks, conversions), FORECAST_RESULTS (series, ts, forecast, lower_bound, upper_bound), ANOMALY_DETECTION_RESULTS (series, ts, y, forecast, lower_bound, upper_bound, is_anomaly, percentile), AI_SENTIMENT_RESULTS (review_id, review_text, channel, product_category, rating, sentiment_score), AI_CLASSIFY_RESULTS (campaign_id, campaign_name, channel, sub_channel, roas, performance_tier), AI_AGG_RESULTS (channel, themes)
- **Analytics:** `MARKETING_AI_BI.MARKETING_ANALYTICS` -- dynamic tables: DT_DAILY_REVENUE (order_date, channel, total_revenue), DT_CAMPAIGN_METRICS (campaign_id, campaign_name, channel, sub_channel, budget, total_spend, total_conversions, campaign_revenue, roas, cpa), DT_PARTNER_PERFORMANCE (partner_name, tier, avg_sell_through_rate, total_revenue)

Create a sidebar-navigated app with these 5 pages:

1. **KPI Overview** -- metric cards (total revenue, DTC revenue, wholesale revenue, total orders, avg order value, ROAS), daily revenue line chart by channel with date range filter, revenue by product category bar chart.

2. **Channel Deep Dive** -- DTC: scatter of spend vs conversions by sub-channel sized by ROAS, campaign metrics table. Wholesale: top 15 partner sell-through bar chart, trade promo spend vs revenue comparison.

3. **Forecasting & Anomalies** -- Forecast: plot actuals from DT_DAILY_REVENUE with forecast overlay and confidence interval band, selectbox for series (Total/DTC/wholesale). Anomaly: plot time series with red markers on anomaly points, selectbox for series (Ad Spend/DTC Conversion Rate/Wholesale Orders).

4. **AI Insights** -- sentiment histogram by channel, AI_AGG theme summaries side-by-side, campaign performance tier donut chart, expandable AI extract table.

5. **Marketing Agent** -- chat interface using `SNOWFLAKE.CORTEX.AGENT('MARKETING_AI_BI.MARKETING_ANALYTICS.SUMMIT_GEAR_AGENT', question)`. Show suggested starter questions as clickable buttons and display the agent's SQL in an expander.

Use plotly for charts. Keep the layout clean with wide mode.
