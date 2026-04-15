# Enhancement Ideas

Once you have the base dashboard running, try pasting any of these follow-up prompts into Cortex Code to level it up.

---

## Visual Polish

> Add a dark-themed color palette using Plotly templates. Use a consistent brand color scheme: navy (#1B2A4A), teal (#2EC4B6), coral (#FF6B6B), gold (#FFD166), and slate gray (#6C757D). Apply it to all charts.

---

## Executive Summary with AI

> Add a collapsible "AI Executive Summary" section at the top of the KPI Overview page. When the user clicks "Generate Summary", call SNOWFLAKE.CORTEX.COMPLETE('snowflake-llama-3.3-70b', ...) with the current KPI values and top/bottom campaigns to produce a 3-sentence executive briefing.

---

## Customer Segmentation Drilldown

> Add a new page called "Customer Segments" that queries DT_CUSTOMER_ENRICHED from MARKETING_AI_BI.MARKETING_ANALYTICS (columns: customer_id, state, zip_code, age, age_group, value_tier, income_bracket, lifestyle_segment, outdoor_interest, total_orders, total_revenue, avg_order_value, orders_per_month, days_since_last_order). Show a bar chart of customer count by value_tier, a scatter of avg_order_value vs orders_per_month colored by lifestyle_segment, and a detail table filterable by state and outdoor_interest.

---

## Time Period Comparison

> On the KPI Overview page, add a toggle that lets the user compare two time periods side-by-side (e.g., Q4 2024 vs Q4 2025). Show the KPI metrics for each period with delta values showing the change.

---

## Anomaly Drill-Through

> When a user clicks an anomaly point on the Forecasting & Anomalies page, show a detail card below the chart with the date, actual vs expected value, the deviation magnitude, and a one-sentence AI-generated explanation of what likely caused it (using CORTEX.COMPLETE with context about the anomaly date and metric).

---

## Product Review Explorer

> Add an interactive review explorer that lets users filter by product category, rating range, and sentiment score range. Show the matching reviews in a scrollable card layout with the rating as stars, sentiment as a colored badge, and the review text. Include a word cloud or bar chart of the most common terms.

---

## Funnel Visualization

> Add a marketing funnel chart to the Channel Deep Dive page showing impressions > clicks > conversions > orders for each DTC sub-channel. Use a Plotly funnel chart and let the user toggle between sub-channels.

---

## Chat History & Pinning

> Enhance the Marketing Agent page with persistent chat history in st.session_state. Let users "pin" useful agent responses to a sidebar panel so they can reference them while exploring other pages.

---

## Exportable Report

> Add a "Download Report" button to each page that generates a PDF or HTML summary of the current page's charts and metrics using st.download_button. Include the chart images as base64-encoded PNGs and key metrics as formatted text.
