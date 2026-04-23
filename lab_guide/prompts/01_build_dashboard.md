# Build the Dashboard

Paste this single prompt into Cortex Code:

---

**IMPORTANT: Before writing any code, read the file `helper_streamlit_skill/SKILL.md` in this project.** It contains critical SiS runtime constraints, the complete Cortex Agent response parser, Altair charting patterns, and deployment instructions. Follow it exactly for all Streamlit-in-Snowflake and Cortex Agent work.

Build a Streamlit-in-Snowflake app for "Summit Gear Co." Wide layout, sidebar navigation with 5 pages. Store the `streamlit_app.py` and `environment.yml` in the stage `@MARKETING_AI_BI.MARKETING_ANALYTICS.STREAMLIT_STAGE/streamlit/`.

## SiS Environment Rules (also in SKILL.md)

- Use `get_active_session()` to connect — NOT `st.connection("snowflake")`.
- Use Altair (`import altair as alt`) for all charts (SVG rendering). Do NOT use plotly or any WebGL-based charting library.
- When layering Altair charts with `+`, ALL layers MUST have identical `.properties(height=...)` values or SiS throws `ValueError: inconsistent values for height`.
- Do NOT use `use_column_width` (deprecated).
- Do NOT use `nonlocal` in nested functions — causes `SyntaxError` in SiS. Use module-level functions with mutable list arguments instead.
- The `environment.yml` should list: streamlit, snowflake-snowpark-python, altair, pandas, pydeck (snowflake conda channel only).
- Snowflake returns UPPERCASE column names — normalize with `df.columns = [c.upper() for c in df.columns]`.
- Convert date columns with `pd.to_datetime()` before filtering.

## How to query data — SEMANTIC_VIEW syntax (CRITICAL)

All pages 1-4 MUST query data through semantic views, NOT direct table queries. The syntax is:

```sql
SELECT * FROM SEMANTIC_VIEW(
    MARKETING_AI_BI.MARKETING_ANALYTICS.SV_SUMMIT_GEAR_MARKETING
    DIMENSIONS entity.column, entity.column
    METRICS entity.metric_name
    WHERE entity.column = 'value'
) ORDER BY column
```

Rules: DIMENSIONS are grouping columns. METRICS are aggregates (SUM, COUNT, AVG — cross-entity joins allowed). FACTS are raw values (same-entity columns only, no cross-entity joins). All column references use `entity.column` format. Store view names as constants: `SV_MARKETING`, `SV_ML`, `SV_REVIEWS`, `SV_ADVANCED`.

Create a helper function for semantic view queries:

```python
def run_sv(sv, dimensions="", metrics="", facts="", where="", order="", limit=""):
    parts = [f"SELECT * FROM SEMANTIC_VIEW(\n    {sv}"]
    if dimensions:
        parts.append(f"    DIMENSIONS {dimensions}")
    if metrics:
        parts.append(f"    METRICS {metrics}")
    if facts:
        parts.append(f"    FACTS {facts}")
    if where:
        parts.append(f"    WHERE {where}")
    q = "\n".join(parts) + "\n)"
    if order:
        q += f" ORDER BY {order}"
    if limit:
        q += f" LIMIT {limit}"
    return session.sql(q).to_pandas()
```

## Semantic views and their entities

**SV_SUMMIT_GEAR_MARKETING** — entities: `campaigns` (campaign_name, channel, sub_channel, budget), `orders` (order_date, channel, product_category, product_name, revenue, quantity), `customers` (state, age, gender), `spend` (spend_date, amount, impressions, clicks, conversions), `partners` (partner_name, region, tier), `daily_revenue` (order_date, channel, total_revenue, order_count, avg_order_value), `product_revenue` (product_category, product_name, channel, total_revenue, order_count), `campaign_tiers` (campaign_name, channel, sub_channel, performance_tier, roas), `campaign_summaries` (campaign_name, channel, sub_channel, executive_summary). Metrics: `orders.total_revenue`, `orders.total_orders`, `orders.avg_order_value`, `spend.total_spend`, `spend.total_conversions`.

**SV_SUMMIT_GEAR_ML** — entities: `forecasts` (model_name, series, ts, forecast, lower_bound, upper_bound), `anomalies` (model_name, series, ts, y, forecast, lower_bound, upper_bound, is_anomaly, percentile), `feature_importance` (feature_name, feature_type, score, rank). Forecast/anomaly columns are FACTS not METRICS.

**SV_SUMMIT_GEAR_REVIEWS** — entities: `sentiment` (review_text, channel, product_category, sentiment_score, rating), `extracts` (product_category, product_name, channel, review_text, product_feedback, competitor_mention, recommendation, rating), `themes` (channel, themes).

**SV_SUMMIT_GEAR_ADVANCED** — entities: `mmm_contributions` (sub_channel, period, total_spend, attributed_revenue, roi, share_of_spend, cost_per_conversion), `mmm_weekly` (week_start, sub_channel, spend, attributed_revenue, incremental_revenue, efficiency_index), `mmm_insights` (insight_text), `geo_profiles` (state, zip_code, customer_count, avg_ltv, total_revenue, targeting_score, dominant_lifestyle, dominant_outdoor_interest, weather_recommended_category, recommended_channel), `geo_triggers` (trigger_action, recommended_product_category), `geo_recommendations` (state, recommendation, customer_count, avg_ltv, total_revenue), `clv_risk` (customer_id, clv_tier, lifetime_value, churn_risk_score, total_orders, state, age_group, value_tier, lifestyle_segment, outdoor_interest), `weather_revenue` (order_date, channel, total_revenue, temp_band, rainy_day, snow_day, national_avg_temp_f), `customer_enriched` (customer_id, state, age_group, value_tier, lifetime_value, avg_order_value, orders_per_month, lifestyle_segment, outdoor_interest). Note: The Advanced Analytics page (page 2) uses direct SQL queries against the analytics tables, not SEMANTIC_VIEW syntax.

## Pages 1-4 (use SEMANTIC_VIEW queries)

1. **KPI Overview** — metric cards (total revenue, DTC revenue, wholesale revenue, total orders, avg order value, ROAS) using `DIMENSIONS campaigns.channel METRICS orders.total_revenue, orders.total_orders, orders.avg_order_value, spend.total_spend`. Daily revenue line chart by channel using `DIMENSIONS daily_revenue.order_date, daily_revenue.channel FACTS daily_revenue.total_revenue` with date range filter. Revenue by product category bar chart using `DIMENSIONS product_revenue.product_category FACTS product_revenue.total_revenue`.

2. **Advanced Analytics** — Three tabs: "Marketing Mix Model", "Geo-Targeting", "CLV & Churn". Use direct SQL queries (not SEMANTIC_VIEW) against these tables. Each tab should include a collapsible `st.expander()` at the top explaining the methodology (how the score/model works, what the data pipeline is). Keep expanders concise — a short paragraph plus a few bullet points max. Avoid tables or lengthy breakdowns inside expanders; they should be scannable in 5 seconds.

   **Tab 1 - Marketing Mix Model:** Quarter selector. Bar chart of attributed revenue by sub_channel colored by ROI from `MMM_CHANNEL_CONTRIBUTIONS` (columns: sub_channel, period, total_spend, attributed_revenue, roi, share_of_spend). Weekly line chart of spend (dashed) vs attributed revenue (solid) by channel from `MMM_WEEKLY_DECOMPOSITION` (columns: week_start, sub_channel, spend, attributed_revenue, efficiency_index) with multi-select channel filter. AI insights text from `MMM_AI_INSIGHTS` (column: insight_text).

   **Tab 2 - Geo-Targeting:** Title this section "Geo-Targeting: Market Opportunity Index". The targeting score is a *Market Opportunity Index* — it measures where marketing dollars will generate the highest return based on proven customer behavior (40% avg LTV, 30% customer density, 30% revenue share). It is NOT propensity to buy or product fit. Use `st.caption()` for a simple one-line legend below the map: "Bubble size = total revenue | Color = Market Opportunity Index (Low → High)". Do NOT build an Altair color legend chart — keep it clean with just the caption. Pydeck scatter map of US states using `st.pydeck_chart()` with `pdk.Layer("ScatterplotLayer")`. Query `GEO_TARGETING_PROFILES` grouped by state (SUM customer_count, AVG avg_ltv, SUM total_revenue, AVG targeting_score). Map state abbreviations to lat/lon centroids using a dict:
   ```python
   STATE_COORDS = {
       'CA': (36.78, -119.42), 'TX': (31.97, -99.90), 'NY': (42.17, -74.95),
       'FL': (27.66, -81.52), 'CO': (39.55, -105.78), 'WA': (47.75, -120.74),
       'OR': (43.80, -120.55), 'UT': (39.32, -111.09), 'MT': (46.88, -110.36),
       'ID': (44.07, -114.74), 'AZ': (34.05, -111.09), 'NC': (35.76, -79.02),
       'VA': (37.43, -78.66), 'GA': (32.16, -82.90), 'PA': (41.20, -77.19),
       'IL': (40.63, -89.40), 'OH': (40.42, -82.91), 'MI': (44.31, -84.71),
       'MN': (46.73, -94.69), 'WI': (43.78, -88.79), 'MA': (42.41, -71.38),
       'NJ': (40.06, -74.41), 'CT': (41.60, -72.76), 'NH': (43.19, -71.57),
       'VT': (44.56, -72.58), 'ME': (45.25, -69.45), 'NM': (34.52, -105.87),
       'NV': (38.80, -116.42), 'TN': (35.52, -86.58), 'SC': (33.84, -81.16),
   }
   ```
   Add LAT/LON columns via `.map()`. Compute RADIUS proportional to total_revenue (scaled to max ~120000). Color by targeting_score (red-green gradient). Set initial view to US center (lat=39.83, lon=-98.58, zoom=3.2). Enable tooltips showing state, revenue, customers, score. Below the map, show the same state-level aggregated bar chart (top 20 by revenue, colored by targeting score). Weather trigger summary bar chart from `GEO_WEATHER_TRIGGERS` (columns: trigger_action — group by trigger_action excluding 'NO TRIGGER'). AI state recommendation cards from `GEO_AI_RECOMMENDATIONS` (columns: state, customer_count, avg_ltv, total_revenue, recommendation) with state selector.

   **Tab 3 - CLV & Churn:** CLV tier donut chart and summary metrics from `CLV_RISK_CLASSIFICATION` (columns: customer_id, clv_tier, lifetime_value, churn_risk_score, total_orders, lifestyle_segment, outdoor_interest). Normalized stacked bar of CLV tier mix by lifestyle_segment. Weather impact on revenue bar chart by temp_band from `DT_WEATHER_REVENUE` (columns: temp_band, total_revenue — group and average).

3. **Forecasting & Anomalies** — Forecast: actuals from SV_MARKETING (`daily_revenue` entity) overlaid with forecast from SV_ML (`DIMENSIONS forecasts.series, forecasts.ts FACTS forecasts.forecast, forecasts.lower_bound, forecasts.upper_bound WHERE forecasts.series = '{series}'`), selectbox for Total/DTC/wholesale, confidence interval band. Anomaly: SV_ML (`DIMENSIONS anomalies.series, anomalies.ts, anomalies.is_anomaly FACTS anomalies.y, anomalies.forecast, anomalies.lower_bound, anomalies.upper_bound, anomalies.percentile WHERE anomalies.series = '{series}'`), selectbox for "Ad Spend"/"DTC Conversion Rate"/"Wholesale Orders", red markers on anomaly points.

4. **AI Insights** — Sentiment histogram by channel from SV_REVIEWS (`DIMENSIONS sentiment.channel, sentiment.review_text FACTS sentiment.sentiment_score`). Theme summaries side-by-side (`DIMENSIONS themes.channel, themes.themes`). Campaign tier donut from SV_MARKETING (`DIMENSIONS campaign_tiers.performance_tier METRICS orders.total_orders`). Campaign executive summaries from SV_MARKETING (`DIMENSIONS campaign_summaries.campaign_name, campaign_summaries.channel, campaign_summaries.sub_channel, campaign_summaries.executive_summary`) — show as expandable cards grouped by channel. Expandable extract table (`DIMENSIONS extracts.product_category, extracts.product_name, extracts.channel, extracts.review_text, extracts.product_feedback, extracts.competitor_mention, extracts.recommendation FACTS extracts.rating LIMIT 50`).

## Page 5 — Marketing Agent (REST API — MUST follow SKILL.md exactly)

Call the agent via REST API using `_snowflake.send_snow_api_request()`. There is NO SQL function for agents.

**You MUST copy the complete agent parsing code from `helper_streamlit_skill/SKILL.md` sections 3 and 4.** This includes:

1. **`call_agent()`** — calls the REST API with retry logic (retries with `{"stream": True}` on non-200)
2. **`_extract_result_set()`** — converts result_set JSON to DataFrame
3. **`_extract_json_content()`** — extracts sql, text, and result_set from JSON content
4. **`_walk_for_content()`** — recursive fallback walker for unknown formats
5. **`process_sse_response()`** — handles ALL three response formats (single message dict, SSE event list, flat content dict)
6. **`render_agent_response()`** — renders text as markdown, SQL in expanders, DataFrames as tables, with debug fallback

All of these MUST be module-level functions (not nested inside `if`/`elif` blocks). They use mutable list arguments — never `nonlocal`.

```python
AGENT_ENDPOINT = "/api/v2/databases/MARKETING_AI_BI/schemas/MARKETING_ANALYTICS/agents/SUMMIT_GEAR_AGENT:run"
AGENT_TIMEOUT = 60000
```

The agent has 4 tools: MarketingAnalyst, MLAnalyst, ReviewAnalyst, and AdvancedAnalyst.

Show suggested question buttons (6 suggestions in 3 columns), use `st.chat_message` for conversation history, `st.chat_input` for user input, and `st.spinner("Thinking...")` while waiting. Store chat history in `st.session_state.agent_messages`.

### Why this matters

The Cortex Agent API returns responses in multiple formats depending on internal routing. `json.loads(resp["content"])` can return either a dict or a list. A naive parser that assumes one format (e.g., `response.get("message")`) will crash with `'list' object has no attribute 'get'`. The `process_sse_response()` function in the SKILL.md handles all known formats with a recursive fallback.
