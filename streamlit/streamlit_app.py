import streamlit as st
from snowflake.snowpark.context import get_active_session
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

st.set_page_config(page_title="Summit Gear Co. Marketing Dashboard", layout="wide")

session = get_active_session()

DB_SCHEMA = "MARKETING_AI_BI.DEMO_DATA"


def run_query(sql):
    return session.sql(sql).to_pandas()


# --- PAGE 1: KPI OVERVIEW ---
def page_kpi_overview():
    st.title("Summit Gear Co. -- KPI Overview")

    orders = run_query(f"""
        SELECT
            SUM(revenue) AS total_revenue,
            SUM(CASE WHEN channel='DTC' THEN revenue ELSE 0 END) AS dtc_revenue,
            SUM(CASE WHEN channel='wholesale' THEN revenue ELSE 0 END) AS wholesale_revenue,
            COUNT(*) AS total_orders,
            ROUND(AVG(revenue),2) AS avg_order_value
        FROM {DB_SCHEMA}.ORDERS
    """)

    prev_orders = run_query(f"""
        SELECT
            SUM(revenue) AS total_revenue,
            SUM(CASE WHEN channel='DTC' THEN revenue ELSE 0 END) AS dtc_revenue,
            SUM(CASE WHEN channel='wholesale' THEN revenue ELSE 0 END) AS wholesale_revenue,
            COUNT(*) AS total_orders,
            ROUND(AVG(revenue),2) AS avg_order_value
        FROM {DB_SCHEMA}.ORDERS
        WHERE order_date < DATE_TRUNC('MONTH', CURRENT_DATE())
          AND order_date >= DATEADD(MONTH, -1, DATE_TRUNC('MONTH', CURRENT_DATE()))
    """)

    spend = run_query(f"SELECT SUM(amount) AS total_spend FROM {DB_SCHEMA}.MARKETING_SPEND")
    roas_val = round(orders["TOTAL_REVENUE"].iloc[0] / max(spend["TOTAL_SPEND"].iloc[0], 1), 2)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Revenue", f"${orders['TOTAL_REVENUE'].iloc[0]:,.0f}")
    c2.metric("DTC Revenue", f"${orders['DTC_REVENUE'].iloc[0]:,.0f}")
    c3.metric("Wholesale Revenue", f"${orders['WHOLESALE_REVENUE'].iloc[0]:,.0f}")
    c4.metric("Total Orders", f"{orders['TOTAL_ORDERS'].iloc[0]:,}")
    c5.metric("Avg Order Value", f"${orders['AVG_ORDER_VALUE'].iloc[0]:,.2f}")
    c6.metric("ROAS", f"{roas_val}x")

    st.subheader("Daily Revenue by Channel")

    daily = run_query(f"""
        SELECT order_date, channel, total_revenue
        FROM {DB_SCHEMA}.DT_DAILY_REVENUE
        ORDER BY order_date
    """)

    col1, col2 = st.columns(2)
    min_date = daily["ORDER_DATE"].min()
    max_date = daily["ORDER_DATE"].max()
    with col1:
        start_date = st.date_input("Start Date", min_date, min_value=min_date, max_value=max_date)
    with col2:
        end_date = st.date_input("End Date", max_date, min_value=min_date, max_value=max_date)

    daily_filtered = daily[(daily["ORDER_DATE"] >= pd.Timestamp(start_date)) & (daily["ORDER_DATE"] <= pd.Timestamp(end_date))]
    fig = px.line(daily_filtered, x="ORDER_DATE", y="TOTAL_REVENUE", color="CHANNEL", title="Daily Revenue")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Revenue by Product Category")
    cat = run_query(f"""
        SELECT product_category, SUM(revenue) AS revenue
        FROM {DB_SCHEMA}.ORDERS
        GROUP BY product_category ORDER BY revenue DESC
    """)
    fig2 = px.bar(cat, x="REVENUE", y="PRODUCT_CATEGORY", orientation="h", title="Revenue by Category")
    st.plotly_chart(fig2, use_container_width=True)


# --- PAGE 2: CHANNEL DEEP DIVE ---
def page_channel_deep_dive():
    st.title("Channel Deep Dive")

    st.subheader("DTC: Spend vs Conversions by Sub-Channel")
    dtc = run_query(f"""
        SELECT campaign_name, sub_channel, total_spend, total_conversions, cpa, roas
        FROM {DB_SCHEMA}.DT_CAMPAIGN_METRICS
        WHERE channel = 'DTC' AND total_spend > 0
    """)
    sub_agg = dtc.groupby("SUB_CHANNEL").agg(
        total_spend=("TOTAL_SPEND", "sum"),
        total_conversions=("TOTAL_CONVERSIONS", "sum"),
        avg_roas=("ROAS", "mean")
    ).reset_index()
    fig = px.scatter(sub_agg, x="total_spend", y="total_conversions", size="avg_roas",
                     color="SUB_CHANNEL", title="DTC Sub-Channels: Spend vs Conversions (sized by ROAS)")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("DTC Campaign Metrics")
    st.dataframe(dtc.sort_values("ROAS", ascending=False), use_container_width=True)

    st.subheader("Wholesale: Partner Sell-Through Rate (Top 15)")
    partners = run_query(f"""
        SELECT partner_name, avg_sell_through_rate, total_revenue, tier
        FROM {DB_SCHEMA}.DT_PARTNER_PERFORMANCE
        ORDER BY avg_sell_through_rate DESC LIMIT 15
    """)
    fig2 = px.bar(partners, x="AVG_SELL_THROUGH_RATE", y="PARTNER_NAME", color="TIER",
                  orientation="h", title="Sell-Through Rate by Partner")
    st.plotly_chart(fig2, use_container_width=True)

    st.subheader("Wholesale Trade Promo Performance")
    trade = run_query(f"""
        SELECT campaign_name, total_spend, campaign_revenue, roas
        FROM {DB_SCHEMA}.DT_CAMPAIGN_METRICS
        WHERE sub_channel = 'trade_promo'
        ORDER BY roas DESC NULLS LAST
    """)
    fig3 = px.bar(trade, x="CAMPAIGN_NAME", y=["TOTAL_SPEND", "CAMPAIGN_REVENUE"],
                  barmode="group", title="Trade Promo: Spend vs Revenue")
    st.plotly_chart(fig3, use_container_width=True)


# --- PAGE 3: FORECASTING & ANOMALIES ---
def page_forecasting_anomalies():
    st.title("Forecasting & Anomalies")

    st.subheader("Revenue Forecast")
    forecast_series = st.selectbox("Forecast Series", ["Total", "DTC", "wholesale"])

    forecast = run_query(f"""
        SELECT ts, forecast, lower_bound, upper_bound
        FROM {DB_SCHEMA}.FORECAST_RESULTS
        WHERE series = '{forecast_series}'
        ORDER BY ts
    """)

    if forecast_series == "Total":
        actuals = run_query(f"""
            SELECT order_date AS ts, SUM(total_revenue) AS actual
            FROM {DB_SCHEMA}.DT_DAILY_REVENUE GROUP BY order_date ORDER BY order_date
        """)
    else:
        actuals = run_query(f"""
            SELECT order_date AS ts, total_revenue AS actual
            FROM {DB_SCHEMA}.DT_DAILY_REVENUE
            WHERE channel = '{forecast_series}' ORDER BY order_date
        """)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=actuals["TS"], y=actuals["ACTUAL"], mode="lines", name="Actual"))
    if not forecast.empty:
        fig.add_trace(go.Scatter(x=forecast["TS"], y=forecast["FORECAST"], mode="lines", name="Forecast", line=dict(dash="dash")))
        fig.add_trace(go.Scatter(x=pd.concat([forecast["TS"], forecast["TS"][::-1]]),
                                 y=pd.concat([forecast["UPPER_BOUND"], forecast["LOWER_BOUND"][::-1]]),
                                 fill="toself", fillcolor="rgba(68,68,255,0.1)", line=dict(color="rgba(255,255,255,0)"), name="Confidence Interval"))
    fig.update_layout(title=f"{forecast_series} Revenue Forecast")
    st.plotly_chart(fig, use_container_width=True)

    try:
        fi = run_query(f"SELECT * FROM {DB_SCHEMA}.FORECAST_FEATURE_IMPORTANCE")
        if not fi.empty:
            st.subheader("Feature Importance")
            rank_col = [c for c in fi.columns if "RANK" in c.upper() or "IMPORTANCE" in c.upper()]
            name_col = [c for c in fi.columns if "FEATURE" in c.upper() or "NAME" in c.upper()]
            if rank_col and name_col:
                fig_fi = px.bar(fi, x=rank_col[0], y=name_col[0], orientation="h", title="Feature Importance")
                st.plotly_chart(fig_fi, use_container_width=True)
            else:
                st.dataframe(fi)
    except Exception:
        pass

    st.subheader("Anomaly Detection")
    anomaly_series = st.selectbox("Anomaly Metric", ["Ad Spend", "DTC Conversion Rate", "Wholesale Orders"])

    anomalies = run_query(f"""
        SELECT ts, y, forecast, lower_bound, upper_bound, is_anomaly, percentile
        FROM {DB_SCHEMA}.ANOMALY_DETECTION_RESULTS
        WHERE series = '{anomaly_series}'
        ORDER BY ts
    """)

    if not anomalies.empty:
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=anomalies["TS"], y=anomalies["Y"], mode="lines", name="Actual"))
        fig2.add_trace(go.Scatter(x=anomalies["TS"], y=anomalies["FORECAST"], mode="lines", name="Expected", line=dict(dash="dot")))
        anomaly_pts = anomalies[anomalies["IS_ANOMALY"] == True]
        fig2.add_trace(go.Scatter(x=anomaly_pts["TS"], y=anomaly_pts["Y"], mode="markers",
                                  marker=dict(color="red", size=10, symbol="x"), name="Anomaly",
                                  text=anomaly_pts.apply(lambda r: f"Date: {r['TS']}<br>Value: {r['Y']:.1f}<br>Expected: {r['FORECAST']:.1f}", axis=1),
                                  hoverinfo="text"))
        fig2.update_layout(title=f"Anomaly Detection: {anomaly_series}")
        st.plotly_chart(fig2, use_container_width=True)


# --- PAGE 4: AI INSIGHTS ---
def page_ai_insights():
    st.title("AI Insights")

    st.subheader("Sentiment Distribution: DTC vs Wholesale")
    sentiment = run_query(f"""
        SELECT channel, sentiment_score FROM {DB_SCHEMA}.AI_SENTIMENT_RESULTS
    """)
    fig = px.histogram(sentiment, x="SENTIMENT_SCORE", color="CHANNEL", barmode="overlay",
                       nbins=30, title="Review Sentiment Distribution")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Theme Summaries by Channel")
    themes = run_query(f"SELECT channel, themes FROM {DB_SCHEMA}.AI_AGG_RESULTS")
    col1, col2 = st.columns(2)
    for _, row in themes.iterrows():
        target = col1 if "dtc" in row["CHANNEL"].lower() else col2
        with target:
            st.markdown(f"**{row['CHANNEL']}**")
            st.write(row["THEMES"])

    st.subheader("Campaign Performance Tiers")
    classify = run_query(f"""
        SELECT performance_tier, COUNT(*) AS count
        FROM {DB_SCHEMA}.AI_CLASSIFY_RESULTS
        GROUP BY performance_tier
    """)
    fig2 = px.pie(classify, values="COUNT", names="PERFORMANCE_TIER", title="Campaign Tiers", hole=0.4)
    st.plotly_chart(fig2, use_container_width=True)

    st.subheader("AI-Extracted Review Details")
    with st.expander("Show AI_EXTRACT Results"):
        extract = run_query(f"SELECT * FROM {DB_SCHEMA}.AI_EXTRACT_RESULTS LIMIT 50")
        st.dataframe(extract, use_container_width=True)


# --- PAGE 5: CORTEX AGENT ---
def page_cortex_agent():
    st.title("Ask the Marketing Agent")
    st.caption("Powered by Cortex Agent + Semantic View")

    suggestions = [
        "Why did DTC sales drop in March?",
        "Which wholesale partner has the best sell-through rate?",
        "How does weather affect winter gear sales?",
        "Compare email vs social campaign ROI",
        "What are the top complaints in product reviews?"
    ]

    st.markdown("**Suggested questions:**")
    cols = st.columns(len(suggestions))
    for i, s in enumerate(suggestions):
        if cols[i].button(s, key=f"suggestion_{i}"):
            st.session_state["agent_question"] = s

    question = st.text_input("Your question:", value=st.session_state.get("agent_question", ""))

    if question:
        with st.spinner("Thinking..."):
            try:
                result = session.sql(f"""
                    SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
                        'MARKETING_AI_BI.DEMO_DATA.SUMMIT_GEAR_AGENT',
                        '{question.replace("'", "''")}'
                    ) AS response
                """).to_pandas()
                response = result["RESPONSE"].iloc[0]
                import json
                try:
                    resp_obj = json.loads(response)
                    if "message" in resp_obj:
                        st.markdown(resp_obj["message"])
                    if "sql" in resp_obj:
                        with st.expander("View SQL Query"):
                            st.code(resp_obj["sql"], language="sql")
                except (json.JSONDecodeError, TypeError):
                    st.markdown(str(response))
            except Exception as e:
                st.error(f"Agent error: {e}")


# --- NAVIGATION ---
pages = {
    "KPI Overview": page_kpi_overview,
    "Channel Deep Dive": page_channel_deep_dive,
    "Forecasting & Anomalies": page_forecasting_anomalies,
    "AI Insights": page_ai_insights,
    "Marketing Agent": page_cortex_agent
}

st.sidebar.title("Summit Gear Co.")
st.sidebar.image("https://via.placeholder.com/200x80?text=Summit+Gear+Co.", use_container_width=True)
selection = st.sidebar.radio("Navigate", list(pages.keys()))
pages[selection]()
