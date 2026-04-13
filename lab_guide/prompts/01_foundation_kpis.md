# Prompt 1: Foundation + KPI Overview

Paste this into Cortex Code:

---

Create a Streamlit app that connects to my Snowflake account and displays a marketing dashboard for "Summit Gear Co." -- an outdoor gear brand that sells DTC (direct to consumer) and wholesale.

On the first page, show:
- A row of KPI metric cards: total revenue, DTC revenue, wholesale revenue, total orders, average order value, and ROAS (return on ad spend). Use st.metric with delta values comparing current month to prior month.
- A time series line chart showing daily revenue with separate lines for DTC and wholesale channels. Let the user pick a date range.
- A horizontal bar chart showing revenue by product category.

Query data from MARKETING_AI_BI.MARKETING_RAW. The main tables are ORDERS (with columns: order_id, customer_id, order_date, channel, product_category, quantity, revenue, wholesale_partner_id) and MARKETING_SPEND (campaign_id, spend_date, channel, amount, impressions, clicks, conversions). Also use the dynamic table MARKETING_AI_BI.MARKETING_ANALYTICS.DT_DAILY_REVENUE for pre-aggregated daily revenue by channel and MARKETING_AI_BI.MARKETING_ANALYTICS.DT_CAMPAIGN_METRICS for campaign-level KPIs.
