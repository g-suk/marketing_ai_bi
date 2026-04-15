import streamlit as st
from snowflake.snowpark.context import get_active_session
import altair as alt
import pandas as pd
import json
import _snowflake

st.set_page_config(page_title="Summit Gear Co. Marketing Dashboard", layout="wide")

session = get_active_session()

SV_MARKETING = "MARKETING_AI_BI.MARKETING_ANALYTICS.SV_SUMMIT_GEAR_MARKETING"
SV_ML = "MARKETING_AI_BI.MARKETING_ANALYTICS.SV_SUMMIT_GEAR_ML"
SV_REVIEWS = "MARKETING_AI_BI.MARKETING_ANALYTICS.SV_SUMMIT_GEAR_REVIEWS"
SV_ADVANCED = "MARKETING_AI_BI.MARKETING_ANALYTICS.SV_SUMMIT_GEAR_ADVANCED"


def run_query(sql):
    return session.sql(sql).to_pandas()


def page_kpi_overview():
    st.title("Summit Gear Co. -- KPI Overview")

    kpis = run_query(f"""
        SELECT * FROM SEMANTIC_VIEW(
            {SV_MARKETING}
            DIMENSIONS campaigns.channel
            METRICS orders.total_revenue, orders.total_orders, orders.avg_order_value, spend.total_spend
        )
    """)

    total_rev = kpis["TOTAL_REVENUE"].sum()
    dtc_rev = kpis.loc[kpis["CHANNEL"] == "DTC", "TOTAL_REVENUE"].sum()
    ws_rev = kpis.loc[kpis["CHANNEL"] == "wholesale", "TOTAL_REVENUE"].sum()
    total_orders = kpis["TOTAL_ORDERS"].sum()
    avg_ov = total_rev / max(total_orders, 1)
    total_spend = kpis["TOTAL_SPEND"].sum()
    roas_val = round(total_rev / max(total_spend, 1), 2)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Revenue", f"${total_rev:,.0f}")
    c2.metric("DTC Revenue", f"${dtc_rev:,.0f}")
    c3.metric("Wholesale Revenue", f"${ws_rev:,.0f}")
    c4.metric("Total Orders", f"{total_orders:,}")
    c5.metric("Avg Order Value", f"${avg_ov:,.2f}")
    c6.metric("ROAS", f"{roas_val}x")

    st.subheader("Daily Revenue by Channel")

    daily = run_query(f"""
        SELECT * FROM SEMANTIC_VIEW(
            {SV_MARKETING}
            DIMENSIONS daily_revenue.order_date, daily_revenue.channel
            FACTS daily_revenue.total_revenue
        ) ORDER BY order_date
    """)

    daily["ORDER_DATE"] = pd.to_datetime(daily["ORDER_DATE"])
    col1, col2 = st.columns(2)
    min_date = daily["ORDER_DATE"].min().date()
    max_date = daily["ORDER_DATE"].max().date()
    with col1:
        start_date = st.date_input("Start Date", min_date, min_value=min_date, max_value=max_date)
    with col2:
        end_date = st.date_input("End Date", max_date, min_value=min_date, max_value=max_date)

    daily_filtered = daily[
        (daily["ORDER_DATE"].dt.date >= start_date) &
        (daily["ORDER_DATE"].dt.date <= end_date)
    ]
    chart = alt.Chart(daily_filtered).mark_line().encode(
        x=alt.X("ORDER_DATE:T", title="Date"),
        y=alt.Y("TOTAL_REVENUE:Q", title="Revenue"),
        color="CHANNEL:N",
        tooltip=["ORDER_DATE:T", "CHANNEL:N", alt.Tooltip("TOTAL_REVENUE:Q", format=",.0f")]
    ).properties(title="Daily Revenue", height=400)
    st.altair_chart(chart, use_container_width=True)

    st.subheader("Revenue by Product Category")
    cat = run_query(f"""
        SELECT * FROM SEMANTIC_VIEW(
            {SV_MARKETING}
            DIMENSIONS product_revenue.product_category
            FACTS product_revenue.total_revenue
        ) ORDER BY total_revenue DESC
    """)
    chart2 = alt.Chart(cat).mark_bar().encode(
        x=alt.X("TOTAL_REVENUE:Q", title="Revenue"),
        y=alt.Y("PRODUCT_CATEGORY:N", sort="-x", title="Category"),
        tooltip=["PRODUCT_CATEGORY:N", alt.Tooltip("TOTAL_REVENUE:Q", format=",.0f")]
    ).properties(title="Revenue by Category", height=300)
    st.altair_chart(chart2, use_container_width=True)


def page_advanced_analytics():
    st.title("Advanced Analytics")
    tab1, tab2, tab3 = st.tabs(["Marketing Mix Model", "Geo-Targeting", "CLV & Churn"])

    with tab1:
        st.subheader("Channel ROI by Quarter")
        mmm = run_query("""
            SELECT sub_channel, period, total_spend, attributed_revenue, roi, share_of_spend
            FROM MARKETING_AI_BI.MARKETING_ANALYTICS.MMM_CHANNEL_CONTRIBUTIONS
            ORDER BY period, total_spend DESC
        """)
        if not mmm.empty:
            period_sel = st.selectbox("Quarter", sorted(mmm["PERIOD"].unique()), key="mmm_period")
            mmm_f = mmm[mmm["PERIOD"] == period_sel]
            chart = alt.Chart(mmm_f).mark_bar().encode(
                x=alt.X("ATTRIBUTED_REVENUE:Q", title="Attributed Revenue"),
                y=alt.Y("SUB_CHANNEL:N", sort="-x", title="Channel"),
                color=alt.Color("ROI:Q", scale=alt.Scale(scheme="redyellowgreen")),
                tooltip=["SUB_CHANNEL:N",
                         alt.Tooltip("TOTAL_SPEND:Q", format=",.0f", title="Spend"),
                         alt.Tooltip("ATTRIBUTED_REVENUE:Q", format=",.0f", title="Attributed Rev"),
                         alt.Tooltip("ROI:Q", format=".2f"),
                         alt.Tooltip("SHARE_OF_SPEND:Q", format=".1f", title="Spend Share %")]
            ).properties(title=f"Channel Attribution — {period_sel}", height=350)
            st.altair_chart(chart, use_container_width=True)

        st.subheader("Weekly Spend vs Attributed Revenue")
        weekly = run_query("""
            SELECT week_start, sub_channel, spend, attributed_revenue, efficiency_index
            FROM MARKETING_AI_BI.MARKETING_ANALYTICS.MMM_WEEKLY_DECOMPOSITION
            ORDER BY week_start
        """)
        if not weekly.empty:
            channel_sel = st.multiselect("Channels", weekly["SUB_CHANNEL"].unique().tolist(),
                                         default=weekly["SUB_CHANNEL"].unique().tolist()[:3], key="mmm_channels")
            wf = weekly[weekly["SUB_CHANNEL"].isin(channel_sel)]
            spend_line = alt.Chart(wf).mark_line(strokeDash=[4, 4]).encode(
                x=alt.X("WEEK_START:T", title="Week"),
                y=alt.Y("SPEND:Q", title="Amount ($)"),
                color="SUB_CHANNEL:N",
                strokeDash=alt.value([4, 4]),
                tooltip=["SUB_CHANNEL:N", "WEEK_START:T", alt.Tooltip("SPEND:Q", format=",.0f")]
            )
            rev_line = alt.Chart(wf).mark_line().encode(
                x="WEEK_START:T",
                y=alt.Y("ATTRIBUTED_REVENUE:Q"),
                color="SUB_CHANNEL:N",
                tooltip=["SUB_CHANNEL:N", "WEEK_START:T", alt.Tooltip("ATTRIBUTED_REVENUE:Q", format=",.0f")]
            )
            chart2 = alt.layer(spend_line, rev_line).properties(
                title="Spend (dashed) vs Attributed Revenue (solid)", height=400
            )
            st.altair_chart(chart2, use_container_width=True)

        st.subheader("AI Marketing Mix Insights")
        insights = run_query("SELECT insight_text FROM MARKETING_AI_BI.MARKETING_ANALYTICS.MMM_AI_INSIGHTS LIMIT 1")
        if not insights.empty:
            st.markdown(insights.iloc[0]["INSIGHT_TEXT"])

    with tab2:
        st.subheader("Geo-Targeting: State Performance")
        geo = run_query("""
            SELECT state, SUM(customer_count) AS customers, ROUND(AVG(avg_ltv), 2) AS avg_ltv,
                   ROUND(SUM(total_revenue), 0) AS total_revenue, ROUND(AVG(targeting_score), 3) AS avg_score,
                   MODE(weather_recommended_category) AS top_category, MODE(recommended_channel) AS top_channel
            FROM MARKETING_AI_BI.MARKETING_ANALYTICS.GEO_TARGETING_PROFILES
            GROUP BY state ORDER BY total_revenue DESC
        """)
        if not geo.empty:
            col1, col2, col3 = st.columns(3)
            col1.metric("States Covered", f"{len(geo)}")
            col2.metric("Avg Targeting Score", f"{geo['AVG_SCORE'].mean():.3f}")
            col3.metric("Total Geo Revenue", f"${geo['TOTAL_REVENUE'].sum():,.0f}")

            chart3 = alt.Chart(geo.head(20)).mark_bar().encode(
                x=alt.X("TOTAL_REVENUE:Q", title="Revenue"),
                y=alt.Y("STATE:N", sort="-x", title="State"),
                color=alt.Color("AVG_SCORE:Q", scale=alt.Scale(scheme="viridis"), title="Targeting Score"),
                tooltip=["STATE:N", alt.Tooltip("TOTAL_REVENUE:Q", format=",.0f"),
                         alt.Tooltip("CUSTOMERS:Q", format=","), alt.Tooltip("AVG_LTV:Q", format=",.2f"),
                         "TOP_CATEGORY:N", "TOP_CHANNEL:N", alt.Tooltip("AVG_SCORE:Q", format=".3f")]
            ).properties(title="Top 20 States by Revenue (colored by targeting score)", height=500)
            st.altair_chart(chart3, use_container_width=True)

        st.subheader("Weather-Triggered Campaigns")
        triggers = run_query("""
            SELECT trigger_action, recommended_product_category, COUNT(*) AS zip_count, ROUND(AVG(avg_ltv), 2) AS avg_ltv
            FROM MARKETING_AI_BI.MARKETING_ANALYTICS.GEO_WEATHER_TRIGGERS
            WHERE trigger_action != 'NO TRIGGER'
            GROUP BY trigger_action, recommended_product_category
            ORDER BY zip_count DESC
        """)
        if not triggers.empty:
            chart4 = alt.Chart(triggers).mark_bar().encode(
                x=alt.X("ZIP_COUNT:Q", title="Zip Codes Triggered"),
                y=alt.Y("TRIGGER_ACTION:N", sort="-x", title="Trigger"),
                color="RECOMMENDED_PRODUCT_CATEGORY:N",
                tooltip=["TRIGGER_ACTION:N", "RECOMMENDED_PRODUCT_CATEGORY:N",
                         alt.Tooltip("ZIP_COUNT:Q", format=","), alt.Tooltip("AVG_LTV:Q", format=",.2f")]
            ).properties(title="Active Weather Triggers", height=300)
            st.altair_chart(chart4, use_container_width=True)
        else:
            st.info("No weather triggers currently active.")

        st.subheader("AI State Recommendations")
        recs = run_query("""
            SELECT state, customer_count, avg_ltv, total_revenue, recommendation
            FROM MARKETING_AI_BI.MARKETING_ANALYTICS.GEO_AI_RECOMMENDATIONS
            ORDER BY total_revenue DESC LIMIT 10
        """)
        if not recs.empty:
            state_sel = st.selectbox("Select State", recs["STATE"].tolist(), key="geo_state")
            row = recs[recs["STATE"] == state_sel].iloc[0]
            c1, c2, c3 = st.columns(3)
            c1.metric("Customers", f"{row['CUSTOMER_COUNT']:,}")
            c2.metric("Avg LTV", f"${row['AVG_LTV']:,.2f}")
            c3.metric("Revenue", f"${row['TOTAL_REVENUE']:,.0f}")
            st.markdown(row["RECOMMENDATION"])

    with tab3:
        st.subheader("CLV Risk Distribution")
        clv = run_query("""
            SELECT clv_tier, COUNT(*) AS customers, ROUND(AVG(lifetime_value), 2) AS avg_ltv,
                   ROUND(AVG(churn_risk_score), 2) AS avg_churn_risk, ROUND(AVG(total_orders), 1) AS avg_orders
            FROM MARKETING_AI_BI.MARKETING_ANALYTICS.CLV_RISK_CLASSIFICATION
            GROUP BY clv_tier ORDER BY customers DESC
        """)
        if not clv.empty:
            total_customers = clv["CUSTOMERS"].sum()
            at_risk = clv.loc[clv["CLV_TIER"].isin(["At-Risk", "Lapsed"]), "CUSTOMERS"].sum()
            loyal = clv.loc[clv["CLV_TIER"] == "Loyal High-Value", "CUSTOMERS"].sum()
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Customers", f"{total_customers:,}")
            c2.metric("Loyal High-Value", f"{loyal:,}")
            c3.metric("At-Risk + Lapsed", f"{at_risk:,}")
            c4.metric("At-Risk %", f"{at_risk / max(total_customers, 1) * 100:.1f}%")

            tier_order = ["Loyal High-Value", "Growth Potential", "New Customer", "At-Risk", "Lapsed"]
            tier_colors = ["#2ecc71", "#3498db", "#9b59b6", "#e67e22", "#e74c3c"]
            chart5 = alt.Chart(clv).mark_arc(innerRadius=60).encode(
                theta="CUSTOMERS:Q",
                color=alt.Color("CLV_TIER:N", sort=tier_order,
                                scale=alt.Scale(domain=tier_order, range=tier_colors)),
                tooltip=["CLV_TIER:N", alt.Tooltip("CUSTOMERS:Q", format=","),
                         alt.Tooltip("AVG_LTV:Q", format=",.2f"),
                         alt.Tooltip("AVG_CHURN_RISK:Q", format=".2f"),
                         alt.Tooltip("AVG_ORDERS:Q", format=".1f")]
            ).properties(title="Customer CLV Tiers", height=350)
            st.altair_chart(chart5, use_container_width=True)

            st.dataframe(clv, use_container_width=True)

        st.subheader("Churn Risk by Lifestyle Segment")
        lifestyle = run_query("""
            SELECT lifestyle_segment, clv_tier, COUNT(*) AS cnt
            FROM MARKETING_AI_BI.MARKETING_ANALYTICS.CLV_RISK_CLASSIFICATION
            WHERE lifestyle_segment IS NOT NULL
            GROUP BY lifestyle_segment, clv_tier ORDER BY lifestyle_segment, cnt DESC
        """)
        if not lifestyle.empty:
            chart6 = alt.Chart(lifestyle).mark_bar().encode(
                x=alt.X("CNT:Q", title="Customers", stack="normalize"),
                y=alt.Y("LIFESTYLE_SEGMENT:N", title="Lifestyle Segment"),
                color=alt.Color("CLV_TIER:N", sort=tier_order,
                                scale=alt.Scale(domain=tier_order, range=tier_colors)),
                tooltip=["LIFESTYLE_SEGMENT:N", "CLV_TIER:N", alt.Tooltip("CNT:Q", format=",")]
            ).properties(title="CLV Tier Mix by Lifestyle (Normalized)", height=350)
            st.altair_chart(chart6, use_container_width=True)

        st.subheader("Weather Impact on Revenue")
        weather = run_query("""
            SELECT temp_band, ROUND(AVG(total_revenue), 2) AS avg_revenue, COUNT(*) AS days
            FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_WEATHER_REVENUE
            WHERE temp_band IS NOT NULL
            GROUP BY temp_band ORDER BY avg_revenue DESC
        """)
        if not weather.empty:
            temp_order = ["freezing", "cold", "mild", "warm", "hot"]
            chart7 = alt.Chart(weather).mark_bar().encode(
                x=alt.X("TEMP_BAND:N", sort=temp_order, title="Temperature Band"),
                y=alt.Y("AVG_REVENUE:Q", title="Avg Daily Revenue"),
                color=alt.Color("TEMP_BAND:N", sort=temp_order,
                                scale=alt.Scale(domain=temp_order,
                                                range=["#3498db", "#2980b9", "#27ae60", "#f39c12", "#e74c3c"])),
                tooltip=["TEMP_BAND:N", alt.Tooltip("AVG_REVENUE:Q", format=",.2f"),
                         alt.Tooltip("DAYS:Q", format=",")]
            ).properties(title="Average Revenue by Temperature Band", height=300)
            st.altair_chart(chart7, use_container_width=True)


def page_forecasting_anomalies():
    st.title("Forecasting & Anomalies")

    st.subheader("Revenue Forecast")
    forecast_series = st.selectbox("Forecast Series", ["Total", "DTC", "wholesale"])

    forecast = run_query(f"""
        SELECT * FROM SEMANTIC_VIEW(
            {SV_ML}
            DIMENSIONS forecasts.series, forecasts.ts
            FACTS forecasts.forecast, forecasts.lower_bound, forecasts.upper_bound
            WHERE forecasts.series = '{forecast_series}'
        ) ORDER BY ts
    """)

    if forecast_series == "Total":
        actuals = run_query(f"""
            SELECT * FROM SEMANTIC_VIEW(
                {SV_MARKETING}
                DIMENSIONS daily_revenue.order_date
                FACTS daily_revenue.total_revenue
            ) ORDER BY order_date
        """)
        actuals = actuals.groupby("ORDER_DATE")["TOTAL_REVENUE"].sum().reset_index()
        actuals.columns = ["TS", "ACTUAL"]
    else:
        actuals = run_query(f"""
            SELECT * FROM SEMANTIC_VIEW(
                {SV_MARKETING}
                DIMENSIONS daily_revenue.order_date, daily_revenue.channel
                FACTS daily_revenue.total_revenue
                WHERE daily_revenue.channel = '{forecast_series}'
            ) ORDER BY order_date
        """)
        actuals = actuals.rename(columns={"ORDER_DATE": "TS", "TOTAL_REVENUE": "ACTUAL"})

    actuals["TS"] = pd.to_datetime(actuals["TS"])
    actual_line = alt.Chart(actuals).mark_line().encode(
        x=alt.X("TS:T", title="Date"),
        y=alt.Y("ACTUAL:Q", title="Revenue"),
        tooltip=["TS:T", alt.Tooltip("ACTUAL:Q", format=",.0f")]
    ).properties(height=400)

    layers = [actual_line]
    if not forecast.empty:
        forecast["TS"] = pd.to_datetime(forecast["TS"])
        forecast_line = alt.Chart(forecast).mark_line(strokeDash=[5, 5], color="orange").encode(
            x="TS:T",
            y=alt.Y("FORECAST:Q"),
            tooltip=["TS:T", alt.Tooltip("FORECAST:Q", format=",.0f")]
        )
        band = alt.Chart(forecast).mark_area(opacity=0.15, color="steelblue").encode(
            x="TS:T",
            y="LOWER_BOUND:Q",
            y2="UPPER_BOUND:Q"
        )
        layers = [band, actual_line, forecast_line]

    chart = alt.layer(*layers).properties(title=f"{forecast_series} Revenue Forecast")
    st.altair_chart(chart, use_container_width=True)

    try:
        fi = run_query(f"""
            SELECT * FROM SEMANTIC_VIEW(
                {SV_ML}
                DIMENSIONS feature_importance.feature_name, feature_importance.feature_type
                FACTS feature_importance.score, feature_importance.rank
            )
        """)
        if not fi.empty:
            st.subheader("Feature Importance")
            chart_fi = alt.Chart(fi).mark_bar().encode(
                x=alt.X("SCORE:Q", title="Importance Score"),
                y=alt.Y("FEATURE_NAME:N", sort="-x", title="Feature"),
                color="FEATURE_TYPE:N",
                tooltip=["FEATURE_NAME:N", "FEATURE_TYPE:N", alt.Tooltip("SCORE:Q", format=".4f")]
            ).properties(height=300)
            st.altair_chart(chart_fi, use_container_width=True)
    except Exception:
        pass

    st.subheader("Anomaly Detection")
    anomaly_series = st.selectbox("Anomaly Metric", ["Ad Spend", "DTC Conversion Rate", "Wholesale Orders"])

    anomalies = run_query(f"""
        SELECT * FROM SEMANTIC_VIEW(
            {SV_ML}
            DIMENSIONS anomalies.series, anomalies.ts, anomalies.is_anomaly
            FACTS anomalies.y, anomalies.forecast, anomalies.lower_bound, anomalies.upper_bound, anomalies.percentile
            WHERE anomalies.series = '{anomaly_series}'
        ) ORDER BY ts
    """)

    if not anomalies.empty:
        anomalies["TS"] = pd.to_datetime(anomalies["TS"])
        actual_line = alt.Chart(anomalies).mark_line().encode(
            x=alt.X("TS:T", title="Date"),
            y=alt.Y("Y:Q", title="Value"),
            tooltip=["TS:T", alt.Tooltip("Y:Q", format=",.1f")]
        )
        expected_line = alt.Chart(anomalies).mark_line(strokeDash=[4, 4], color="gray").encode(
            x="TS:T",
            y="FORECAST:Q"
        )
        anomaly_pts = anomalies[anomalies["IS_ANOMALY"] == True]
        anomaly_marks = alt.Chart(anomaly_pts).mark_point(
            color="red", size=120, shape="cross", filled=True
        ).encode(
            x="TS:T",
            y="Y:Q",
            tooltip=["TS:T",
                     alt.Tooltip("Y:Q", title="Value", format=",.1f"),
                     alt.Tooltip("FORECAST:Q", title="Expected", format=",.1f")]
        )
        chart = alt.layer(actual_line, expected_line, anomaly_marks).properties(
            title=f"Anomaly Detection: {anomaly_series}", height=400
        )
        st.altair_chart(chart, use_container_width=True)


def page_ai_insights():
    st.title("AI Insights")

    st.subheader("Sentiment Distribution: DTC vs Wholesale")
    sentiment = run_query(f"""
        SELECT * FROM SEMANTIC_VIEW(
            {SV_REVIEWS}
            DIMENSIONS sentiment.channel, sentiment.review_text
            FACTS sentiment.sentiment_score
        )
    """)
    chart = alt.Chart(sentiment).mark_bar(opacity=0.7).encode(
        x=alt.X("SENTIMENT_SCORE:Q", bin=alt.Bin(maxbins=30), title="Sentiment Score"),
        y=alt.Y("count()", title="Count"),
        color="CHANNEL:N",
        tooltip=["CHANNEL:N", "count()"]
    ).properties(title="Review Sentiment Distribution", height=350)
    st.altair_chart(chart, use_container_width=True)

    st.subheader("Theme Summaries by Channel")
    themes = run_query(f"""
        SELECT * FROM SEMANTIC_VIEW(
            {SV_REVIEWS}
            DIMENSIONS themes.channel, themes.themes
        )
    """)
    col1, col2 = st.columns(2)
    for _, row in themes.iterrows():
        target = col1 if "dtc" in row["CHANNEL"].lower() else col2
        with target:
            st.markdown(f"**{row['CHANNEL']}**")
            st.write(row["THEMES"])

    st.subheader("Campaign Performance Tiers")
    classify = run_query(f"""
        SELECT * FROM SEMANTIC_VIEW(
            {SV_MARKETING}
            DIMENSIONS campaign_tiers.performance_tier
            METRICS orders.total_orders
        )
    """)
    chart2 = alt.Chart(classify).mark_arc(innerRadius=50).encode(
        theta="TOTAL_ORDERS:Q",
        color=alt.Color("PERFORMANCE_TIER:N", title="Tier"),
        tooltip=["PERFORMANCE_TIER:N", alt.Tooltip("TOTAL_ORDERS:Q", format=",")]
    ).properties(title="Campaign Tiers", height=350)
    st.altair_chart(chart2, use_container_width=True)

    st.subheader("AI-Extracted Review Details")
    with st.expander("Show AI_EXTRACT Results"):
        extract = run_query(f"""
            SELECT * FROM SEMANTIC_VIEW(
                {SV_REVIEWS}
                DIMENSIONS extracts.product_category, extracts.product_name, extracts.channel,
                           extracts.review_text, extracts.product_feedback,
                           extracts.competitor_mention, extracts.recommendation
                FACTS extracts.rating
            ) LIMIT 50
        """)
        st.dataframe(extract, use_container_width=True)


AGENT_API_ENDPOINT = "/api/v2/databases/MARKETING_AI_BI/schemas/MARKETING_ANALYTICS/agents/SUMMIT_GEAR_AGENT:run"
AGENT_TIMEOUT = 60000


def call_agent(question, chat_history=None):
    messages = []
    if chat_history:
        messages.extend(chat_history)
    messages.append({"role": "user", "content": [{"type": "text", "text": question}]})

    payload = {
        "messages": messages,
    }

    resp = _snowflake.send_snow_api_request(
        "POST",
        AGENT_API_ENDPOINT,
        {},
        {},
        payload,
        None,
        AGENT_TIMEOUT
    )

    if resp["status"] != 200:
        resp = _snowflake.send_snow_api_request(
            "POST",
            AGENT_API_ENDPOINT,
            {},
            {"stream": True},
            payload,
            None,
            AGENT_TIMEOUT
        )
        if resp["status"] != 200:
            raise Exception(f"Agent API returned HTTP {resp['status']}: {resp.get('content', '')}")

    return json.loads(resp["content"])


def _extract_result_set(rs):
    cols = [c["name"] for c in rs.get("resultSetMetaData", {}).get("rowType", [])]
    rows = rs.get("data", [])
    if cols and rows:
        return pd.DataFrame(rows, columns=cols)
    return None


def _extract_json_content(jd, text_parts, sql_blocks, tables):
    if "sql" in jd:
        sql_blocks.append(jd["sql"])
    if "text" in jd:
        text_parts.append(jd["text"])
    if "result_set" in jd:
        df = _extract_result_set(jd["result_set"])
        if df is not None:
            tables.append(df)


def _walk_for_content(obj, text_parts, sql_blocks, tables):
    if isinstance(obj, dict):
        if "resultSetMetaData" in obj and "data" in obj:
            df = _extract_result_set(obj)
            if df is not None:
                tables.append(df)
                return
        if obj.get("type") == "text" and "text" in obj:
            text_parts.append(obj["text"])
        if obj.get("type") == "json" and "json" in obj:
            _extract_json_content(obj["json"], text_parts, sql_blocks, tables)
        if "sql" in obj and isinstance(obj["sql"], str) and "SELECT" in obj["sql"].upper():
            sql_blocks.append(obj["sql"])
        if "text" in obj and isinstance(obj["text"], str) and obj.get("type") != "text" and len(obj.get("text", "")) > 20:
            text_parts.append(obj["text"])
        for v in obj.values():
            if isinstance(v, (dict, list)):
                _walk_for_content(v, text_parts, sql_blocks, tables)
    elif isinstance(obj, list):
        for item in obj:
            _walk_for_content(item, text_parts, sql_blocks, tables)


def process_sse_response(response_content):
    text_parts = []
    sql_blocks = []
    tables = []

    if isinstance(response_content, str):
        try:
            response_content = json.loads(response_content)
        except json.JSONDecodeError:
            return [response_content], [], []

    if isinstance(response_content, dict) and "message" in response_content:
        msg = response_content["message"]
        for item in msg.get("content", []):
            it = item.get("type")
            if it == "text":
                text_parts.append(item.get("text", ""))
            elif it == "tool_results":
                tr = item.get("tool_results", {})
                for ci in tr.get("content", []):
                    if ci.get("type") == "json":
                        _extract_json_content(ci.get("json", {}), text_parts, sql_blocks, tables)
                    elif ci.get("type") == "result_set":
                        df = _extract_result_set(ci.get("result_set", {}))
                        if df is not None:
                            tables.append(df)
            elif it == "tool_result":
                tr = item.get("tool_result", {})
                for ci in tr.get("content", []):
                    if ci.get("type") == "json":
                        _extract_json_content(ci.get("json", {}), text_parts, sql_blocks, tables)
                    elif ci.get("type") == "result_set":
                        df = _extract_result_set(ci.get("result_set", {}))
                        if df is not None:
                            tables.append(df)
        return text_parts, sql_blocks, tables

    if isinstance(response_content, list):
        for event in response_content:
            if not isinstance(event, dict):
                continue
            event_type = event.get("event", "")
            data = event.get("data", {})
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except json.JSONDecodeError:
                    continue

            if event_type == "message.delta":
                delta = data.get("delta", {})
                for ci in delta.get("content", []):
                    ct = ci.get("type")
                    if ct == "text":
                        text_parts.append(ci.get("text", ""))
                    elif ct in ("tool_results", "tool_result"):
                        tr = ci.get("tool_results", ci.get("tool_result", {}))
                        for rc in tr.get("content", []):
                            if rc.get("type") == "json":
                                _extract_json_content(rc.get("json", {}), text_parts, sql_blocks, tables)

            elif event_type == "response.text":
                text_parts.append(data.get("text", ""))

            elif event_type == "response.tool_result":
                for ci in data.get("content", []):
                    if ci.get("type") == "json":
                        _extract_json_content(ci.get("json", {}), text_parts, sql_blocks, tables)

            elif event_type == "response.table":
                df = _extract_result_set(data.get("result_set", {}))
                if df is not None:
                    tables.append(df)

            elif event_type == "response":
                for item in data.get("content", []):
                    it = item.get("type")
                    if it == "text":
                        text_parts.append(item.get("text", ""))
                    elif it == "tool_result":
                        tr = item.get("tool_result", {})
                        for ci in tr.get("content", []):
                            if ci.get("type") == "json":
                                _extract_json_content(ci.get("json", {}), text_parts, sql_blocks, tables)
                    elif it == "table":
                        df = _extract_result_set(item.get("table", {}).get("result_set", {}))
                        if df is not None:
                            tables.append(df)

    elif isinstance(response_content, dict):
        for item in response_content.get("content", []):
            it = item.get("type")
            if it == "text":
                text_parts.append(item.get("text", ""))
            elif it == "tool_result":
                tr = item.get("tool_result", {})
                for ci in tr.get("content", []):
                    if ci.get("type") == "json":
                        _extract_json_content(ci.get("json", {}), text_parts, sql_blocks, tables)
            elif it == "table":
                df = _extract_result_set(item.get("table", {}).get("result_set", {}))
                if df is not None:
                    tables.append(df)

    if not any(t.strip() for t in text_parts) and not sql_blocks and not tables:
        _walk_for_content(response_content, text_parts, sql_blocks, tables)

    return text_parts, sql_blocks, tables


def render_agent_response(response_content):
    text_parts, sql_blocks, tables = process_sse_response(response_content)

    for text in text_parts:
        if text.strip():
            st.markdown(text)

    for sql in sql_blocks:
        with st.expander("View SQL Query"):
            st.code(sql, language="sql")

    for df in tables:
        st.dataframe(df, use_container_width=True)

    if not any(t.strip() for t in text_parts) and not sql_blocks and not tables:
        with st.expander("Debug: Raw Agent Response"):
            st.json(response_content)

    return text_parts


def page_cortex_agent():
    st.title("Ask the Marketing Agent")
    st.caption("Powered by Cortex Agent + Semantic Views")

    if "agent_messages" not in st.session_state:
        st.session_state["agent_messages"] = []

    suggestions = [
        "Why did DTC sales drop in March?",
        "Which wholesale partner has the best sell-through rate?",
        "Compare email vs social campaign ROI",
        "What are the top complaints in product reviews?",
        "Show me the revenue forecast for the next 90 days"
    ]

    st.markdown("**Suggested questions:**")
    cols = st.columns(len(suggestions))
    for i, s in enumerate(suggestions):
        if cols[i].button(s, key=f"suggestion_{i}"):
            st.session_state["agent_question"] = s

    for msg in st.session_state["agent_messages"]:
        with st.chat_message(msg["role"]):
            if msg["role"] == "user":
                st.markdown(msg["content"])
            else:
                st.markdown(msg["content"])

    question = st.chat_input("Ask a question about marketing data...")
    if not question:
        question = st.session_state.pop("agent_question", None)

    if question:
        st.session_state["agent_messages"].append({"role": "user", "content": question})
        with st.chat_message("user"):
            st.markdown(question)

        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                try:
                    response = call_agent(question)
                    text_parts = render_agent_response(response)

                    text_summary = " ".join(t for t in text_parts if t.strip()).strip()
                    st.session_state["agent_messages"].append(
                        {"role": "assistant", "content": text_summary or "(see results above)"}
                    )
                except Exception as e:
                    st.error(f"Agent error: {e}")


pages = {
    "KPI Overview": page_kpi_overview,
    "Advanced Analytics": page_advanced_analytics,
    "Forecasting & Anomalies": page_forecasting_anomalies,
    "AI Insights": page_ai_insights,
    "Marketing Agent": page_cortex_agent
}

st.sidebar.title("Summit Gear Co.")
selection = st.sidebar.radio("Navigate", list(pages.keys()))
pages[selection]()
