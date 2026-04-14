# Build the Dashboard

Paste this single prompt into Cortex Code:

---

Build a Streamlit-in-Snowflake app for "Summit Gear Co." Use `get_active_session()` to connect. Use Altair (`import altair as alt`) for all charts (SVG rendering). Do NOT use plotly or any WebGL-based charting library. Do NOT use `use_column_width` (deprecated) on `st.sidebar.image()`. Wide layout, sidebar navigation with 5 pages.

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

Rules: DIMENSIONS are grouping columns. METRICS are aggregates (SUM, COUNT, AVG — cross-entity joins allowed). FACTS are raw values (same-entity columns only, no cross-entity joins). All column references use `entity.column` format. Store view names as constants: `SV_MARKETING`, `SV_ML`, `SV_REVIEWS`.

## Three semantic views and their entities

**SV_SUMMIT_GEAR_MARKETING** — entities: `campaigns` (campaign_name, channel, sub_channel, budget), `orders` (order_date, channel, product_category, product_name, revenue, quantity), `customers` (state, age, gender), `spend` (spend_date, amount, impressions, clicks, conversions), `partners` (partner_name, region, tier), `daily_revenue` (order_date, channel, total_revenue, order_count, avg_order_value), `product_revenue` (product_category, product_name, channel, total_revenue, order_count), `campaign_tiers` (campaign_name, channel, sub_channel, performance_tier, roas). Metrics: `orders.total_revenue`, `orders.total_orders`, `orders.avg_order_value`, `spend.total_spend`, `spend.total_conversions`.

**SV_SUMMIT_GEAR_ML** — entities: `forecasts` (model_name, series, ts, forecast, lower_bound, upper_bound), `anomalies` (model_name, series, ts, y, forecast, lower_bound, upper_bound, is_anomaly, percentile), `feature_importance` (feature_name, feature_type, score, rank). Forecast/anomaly columns are FACTS not METRICS.

**SV_SUMMIT_GEAR_REVIEWS** — entities: `sentiment` (review_text, channel, product_category, sentiment_score, rating), `extracts` (product_category, product_name, channel, review_text, product_feedback, competitor_mention, recommendation, rating), `themes` (channel, themes).

## Pages 1-4 (use SEMANTIC_VIEW queries)

1. **KPI Overview** — metric cards (total revenue, DTC revenue, wholesale revenue, total orders, avg order value, ROAS) using `DIMENSIONS campaigns.channel METRICS orders.total_revenue, orders.total_orders, orders.avg_order_value, spend.total_spend`. Daily revenue line chart by channel using `DIMENSIONS daily_revenue.order_date, daily_revenue.channel FACTS daily_revenue.total_revenue` with date range filter. Revenue by product category bar chart using `DIMENSIONS product_revenue.product_category FACTS product_revenue.total_revenue`.

2. **Channel Deep Dive** — DTC scatter (spend vs conversions by sub_channel sized by ROAS) and campaign table using `DIMENSIONS campaigns.campaign_name, campaigns.sub_channel METRICS orders.total_revenue, spend.total_spend, spend.total_conversions WHERE campaigns.channel = 'DTC'`. Wholesale top 15 partners using `DIMENSIONS partners.partner_name, partners.tier METRICS orders.total_revenue WHERE partners.partner_name IS NOT NULL`. Trade promo comparison using `WHERE campaigns.sub_channel = 'trade_promo'`.

3. **Forecasting & Anomalies** — Forecast: actuals from SV_MARKETING (`daily_revenue` entity) overlaid with forecast from SV_ML (`DIMENSIONS forecasts.series, forecasts.ts FACTS forecasts.forecast, forecasts.lower_bound, forecasts.upper_bound WHERE forecasts.series = '{series}'`), selectbox for Total/DTC/wholesale, confidence interval band. Anomaly: SV_ML (`DIMENSIONS anomalies.series, anomalies.ts, anomalies.is_anomaly FACTS anomalies.y, anomalies.forecast, anomalies.lower_bound, anomalies.upper_bound, anomalies.percentile WHERE anomalies.series = '{series}'`), selectbox for "Ad Spend"/"DTC Conversion Rate"/"Wholesale Orders", red markers on anomaly points.

4. **AI Insights** — Sentiment histogram by channel from SV_REVIEWS (`DIMENSIONS sentiment.channel, sentiment.review_text FACTS sentiment.sentiment_score`). Theme summaries side-by-side (`DIMENSIONS themes.channel, themes.themes`). Campaign tier donut from SV_MARKETING (`DIMENSIONS campaign_tiers.performance_tier METRICS orders.total_orders`). Expandable extract table (`DIMENSIONS extracts.product_category, extracts.product_name, extracts.channel, extracts.review_text, extracts.product_feedback, extracts.competitor_mention, extracts.recommendation FACTS extracts.rating LIMIT 50`).

## Page 5 — Marketing Agent (REST API, not SQL)

Call the agent via REST API using `_snowflake.send_snow_api_request()`. There is NO SQL function for agents.

```python
import _snowflake, json

AGENT_ENDPOINT = "/api/v2/databases/MARKETING_AI_BI/schemas/MARKETING_ANALYTICS/agents/SUMMIT_GEAR_AGENT:run"
AGENT_TIMEOUT = 60000

def call_agent(question):
    messages = [{"role": "user", "content": [{"type": "text", "text": question}]}]
    payload = {"messages": messages}
    resp = _snowflake.send_snow_api_request("POST", AGENT_ENDPOINT, {}, {}, payload, None, AGENT_TIMEOUT)
    if resp["status"] != 200:
        raise Exception(f"Agent error HTTP {resp['status']}: {resp.get('content','')}")
    return json.loads(resp["content"])
```

The non-streaming response format is: `{"message": {"content": [{"type": "text", "text": "..."}, {"type": "tool_results", "tool_results": {"content": [{"type": "json", "json": {"sql": "...", "text": "...", "result_set": {...}}}]}}]}}`. Parse `message.content[]` — extract text from `type: text` items, extract SQL/result_set from `type: tool_results` items. For result_set: columns are in `resultSetMetaData.rowType[].name`, rows in `data[]`. Build a recursive fallback walker in case the format varies. Show suggested questions as buttons, render text as markdown, SQL in expanders, result_set as `st.dataframe()`.
