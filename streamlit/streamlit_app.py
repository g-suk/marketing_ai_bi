import streamlit as st
import altair as alt
import pandas as pd
import json
import _snowflake
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Summit Gear Co. Marketing", layout="wide", page_icon="⛰️")

session = get_active_session()

SV_MARKETING = "MARKETING_AI_BI.MARKETING_ANALYTICS.SV_SUMMIT_GEAR_MARKETING"
SV_ML = "MARKETING_AI_BI.MARKETING_ANALYTICS.SV_SUMMIT_GEAR_ML"
SV_REVIEWS = "MARKETING_AI_BI.MARKETING_ANALYTICS.SV_SUMMIT_GEAR_REVIEWS"
SV_ADVANCED = "MARKETING_AI_BI.MARKETING_ANALYTICS.SV_SUMMIT_GEAR_ADVANCED"

AGENT_ENDPOINT = "/api/v2/databases/MARKETING_AI_BI/schemas/MARKETING_ANALYTICS/agents/SUMMIT_GEAR_AGENT:run"
AGENT_TIMEOUT = 60000


def run_query(sql):
    return session.sql(sql).to_pandas()


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
    if limit:
        parts.append(f"    LIMIT {limit}")
    q = "\n".join(parts) + "\n)"
    if order:
        q += f" ORDER BY {order}"
    return run_query(q)


def call_agent(question, chat_history=None):
    messages = []
    if chat_history:
        messages.extend(chat_history)
    messages.append({"role": "user", "content": [{"type": "text", "text": question}]})
    payload = {"messages": messages}
    resp = _snowflake.send_snow_api_request("POST", AGENT_ENDPOINT, {}, {}, payload, None, AGENT_TIMEOUT)
    if resp["status"] != 200:
        resp = _snowflake.send_snow_api_request("POST", AGENT_ENDPOINT, {}, {"stream": True}, payload, None, AGENT_TIMEOUT)
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


pages = {
    "KPI Overview": "📊",
    "Advanced Analytics": "🔬",
    "Forecasting & Anomalies": "📈",
    "AI Insights": "🤖",
    "Marketing Agent": "💬",
}

with st.sidebar:
    st.title("⛰️ Summit Gear Co.")
    st.caption("Marketing AI+BI Lab")
    page = st.radio("Navigate", list(pages.keys()), format_func=lambda x: f"{pages[x]} {x}")

# ─── PAGE 1: KPI Overview ─────────────────────────────────────────────────────
if page == "KPI Overview":
    st.title("📊 KPI Overview")

    kpi_data = run_sv(
        SV_MARKETING,
        dimensions="campaigns.channel",
        metrics="orders.total_revenue, orders.total_orders, orders.avg_order_value, spend.total_spend",
    )

    total_rev = kpi_data["TOTAL_REVENUE"].sum()
    dtc_rev = kpi_data.loc[kpi_data["CHANNEL"] == "DTC", "TOTAL_REVENUE"].sum()
    ws_rev = kpi_data.loc[kpi_data["CHANNEL"] == "wholesale", "TOTAL_REVENUE"].sum()
    total_orders = kpi_data["TOTAL_ORDERS"].sum()
    avg_ov = kpi_data["AVG_ORDER_VALUE"].mean()
    total_spend = kpi_data["TOTAL_SPEND"].sum()
    roas = total_rev / total_spend if total_spend > 0 else 0

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Revenue", f"${total_rev:,.0f}")
    c2.metric("DTC Revenue", f"${dtc_rev:,.0f}")
    c3.metric("Wholesale Revenue", f"${ws_rev:,.0f}")
    c4.metric("Total Orders", f"{total_orders:,.0f}")
    c5.metric("Avg Order Value", f"${avg_ov:,.2f}")
    c6.metric("ROAS", f"{roas:.2f}x")

    st.subheader("Daily Revenue by Channel")
    daily = run_sv(
        SV_MARKETING,
        dimensions="daily_revenue.order_date, daily_revenue.channel",
        facts="daily_revenue.total_revenue",
    )
    daily.columns = [c.upper() for c in daily.columns]
    if not daily.empty:
        date_col = "ORDER_DATE"
        daily[date_col] = pd.to_datetime(daily[date_col])
        d1, d2 = st.columns(2)
        min_d, max_d = daily[date_col].min().date(), daily[date_col].max().date()
        start = d1.date_input("Start", min_d, min_value=min_d, max_value=max_d)
        end = d2.date_input("End", max_d, min_value=min_d, max_value=max_d)
        mask = (daily[date_col].dt.date >= start) & (daily[date_col].dt.date <= end)
        filtered = daily[mask]
        chart = (
            alt.Chart(filtered)
            .mark_line(strokeWidth=2)
            .encode(
                x=alt.X(f"{date_col}:T", title="Date"),
                y=alt.Y("TOTAL_REVENUE:Q", title="Revenue ($)"),
                color=alt.Color("CHANNEL:N", title="Channel"),
            )
            .properties(height=350)
        )
        st.altair_chart(chart, use_container_width=True)

    st.subheader("Revenue by Product Category")
    prod = run_sv(
        SV_MARKETING,
        dimensions="product_revenue.product_category",
        facts="product_revenue.total_revenue",
    )
    prod.columns = [c.upper() for c in prod.columns]
    if not prod.empty:
        prod_agg = prod.groupby("PRODUCT_CATEGORY", as_index=False)["TOTAL_REVENUE"].sum()
        bar = (
            alt.Chart(prod_agg)
            .mark_bar()
            .encode(
                x=alt.X("TOTAL_REVENUE:Q", title="Revenue ($)"),
                y=alt.Y("PRODUCT_CATEGORY:N", sort="-x", title="Category"),
                color=alt.Color("PRODUCT_CATEGORY:N", legend=None),
            )
            .properties(height=300)
        )
        st.altair_chart(bar, use_container_width=True)

# ─── PAGE 2: Advanced Analytics ────────────────────────────────────────────────
elif page == "Advanced Analytics":
    st.title("🔬 Advanced Analytics")
    tab1, tab2, tab3 = st.tabs(["Marketing Mix Model", "Geo-Targeting", "CLV & Churn"])

    with tab1:
        st.subheader("Marketing Mix Model")
        mmm_contrib = run_query("SELECT sub_channel, period, total_spend, attributed_revenue, roi, share_of_spend FROM MARKETING_AI_BI.MARKETING_ANALYTICS.MMM_CHANNEL_CONTRIBUTIONS ORDER BY period, sub_channel")
        if not mmm_contrib.empty:
            periods = sorted(mmm_contrib["PERIOD"].unique())
            sel_period = st.selectbox("Quarter", periods, index=len(periods) - 1)
            pdata = mmm_contrib[mmm_contrib["PERIOD"] == sel_period]
            bar = (
                alt.Chart(pdata)
                .mark_bar()
                .encode(
                    x=alt.X("ATTRIBUTED_REVENUE:Q", title="Attributed Revenue ($)"),
                    y=alt.Y("SUB_CHANNEL:N", sort="-x", title="Channel"),
                    color=alt.Color("ROI:Q", scale=alt.Scale(scheme="viridis"), title="ROI"),
                )
                .properties(height=300)
            )
            st.altair_chart(bar, use_container_width=True)

        mmm_weekly = run_query("SELECT week_start, sub_channel, spend, attributed_revenue, efficiency_index FROM MARKETING_AI_BI.MARKETING_ANALYTICS.MMM_WEEKLY_DECOMPOSITION ORDER BY week_start")
        if not mmm_weekly.empty:
            channels = sorted(mmm_weekly["SUB_CHANNEL"].unique())
            sel_channels = st.multiselect("Channels", channels, default=channels[:3])
            wdata = mmm_weekly[mmm_weekly["SUB_CHANNEL"].isin(sel_channels)]
            wdata["WEEK_START"] = pd.to_datetime(wdata["WEEK_START"])
            spend_line = alt.Chart(wdata).mark_line(strokeDash=[5, 5], opacity=0.6).encode(
                x="WEEK_START:T", y=alt.Y("SPEND:Q", title="$"), color="SUB_CHANNEL:N"
            )
            rev_line = alt.Chart(wdata).mark_line(strokeWidth=2).encode(
                x="WEEK_START:T", y=alt.Y("ATTRIBUTED_REVENUE:Q", title="$"), color="SUB_CHANNEL:N"
            )
            st.altair_chart((spend_line + rev_line).properties(height=300), use_container_width=True)
            st.caption("Dashed = Spend, Solid = Attributed Revenue")

        insights = run_query("SELECT insight_text FROM MARKETING_AI_BI.MARKETING_ANALYTICS.MMM_AI_INSIGHTS")
        if not insights.empty:
            st.info(insights.iloc[0]["INSIGHT_TEXT"])

    with tab2:
        st.subheader("Geo-Targeting")
        geo = run_query("SELECT state, SUM(customer_count) AS customer_count, ROUND(AVG(avg_ltv),2) AS avg_ltv, SUM(total_revenue) AS total_revenue, ROUND(AVG(targeting_score),3) AS targeting_score FROM MARKETING_AI_BI.MARKETING_ANALYTICS.GEO_TARGETING_PROFILES GROUP BY state ORDER BY total_revenue DESC LIMIT 20")
        if not geo.empty:
            bar = (
                alt.Chart(geo)
                .mark_bar()
                .encode(
                    x=alt.X("TOTAL_REVENUE:Q", title="Revenue ($)"),
                    y=alt.Y("STATE:N", sort="-x", title="State"),
                    color=alt.Color("TARGETING_SCORE:Q", scale=alt.Scale(scheme="orangered"), title="Score"),
                )
                .properties(height=400)
            )
            st.altair_chart(bar, use_container_width=True)

        triggers = run_query("SELECT trigger_action, COUNT(*) AS zip_count FROM MARKETING_AI_BI.MARKETING_ANALYTICS.GEO_WEATHER_TRIGGERS WHERE trigger_action != 'NO TRIGGER' GROUP BY trigger_action ORDER BY zip_count DESC")
        if not triggers.empty:
            tbar = alt.Chart(triggers).mark_bar().encode(
                x=alt.X("ZIP_COUNT:Q", title="Zip Codes"), y=alt.Y("TRIGGER_ACTION:N", sort="-x")
            ).properties(height=200)
            st.altair_chart(tbar, use_container_width=True)

        recs = run_query("SELECT state, customer_count, avg_ltv, total_revenue, recommendation FROM MARKETING_AI_BI.MARKETING_ANALYTICS.GEO_AI_RECOMMENDATIONS ORDER BY total_revenue DESC")
        if not recs.empty:
            sel_state = st.selectbox("State", recs["STATE"].tolist())
            row = recs[recs["STATE"] == sel_state].iloc[0]
            st.metric("Customers", f"{row['CUSTOMER_COUNT']:,.0f}")
            st.metric("Avg LTV", f"${row['AVG_LTV']:,.2f}")
            st.markdown(row["RECOMMENDATION"])

    with tab3:
        st.subheader("CLV & Churn Risk")
        clv = run_query("SELECT clv_tier, COUNT(*) AS cnt, ROUND(AVG(lifetime_value),2) AS avg_ltv, ROUND(AVG(churn_risk_score),3) AS avg_churn FROM MARKETING_AI_BI.MARKETING_ANALYTICS.CLV_RISK_CLASSIFICATION GROUP BY clv_tier ORDER BY cnt DESC")
        if not clv.empty:
            c1, c2 = st.columns(2)
            with c1:
                donut = (
                    alt.Chart(clv)
                    .mark_arc(innerRadius=50)
                    .encode(
                        theta=alt.Theta("CNT:Q"),
                        color=alt.Color("CLV_TIER:N", title="Tier"),
                        tooltip=["CLV_TIER", "CNT", "AVG_LTV", "AVG_CHURN"],
                    )
                    .properties(height=300)
                )
                st.altair_chart(donut, use_container_width=True)
            with c2:
                st.dataframe(clv, use_container_width=True)

        seg = run_query("SELECT lifestyle_segment, clv_tier, COUNT(*) AS cnt FROM MARKETING_AI_BI.MARKETING_ANALYTICS.CLV_RISK_CLASSIFICATION GROUP BY lifestyle_segment, clv_tier ORDER BY lifestyle_segment")
        if not seg.empty:
            total = seg.groupby("LIFESTYLE_SEGMENT")["CNT"].transform("sum")
            seg["PCT"] = seg["CNT"] / total
            sbar = alt.Chart(seg).mark_bar().encode(
                x=alt.X("PCT:Q", stack="normalize", title="Share"),
                y=alt.Y("LIFESTYLE_SEGMENT:N", title="Lifestyle"),
                color="CLV_TIER:N",
            ).properties(height=250)
            st.altair_chart(sbar, use_container_width=True)

        weather_rev = run_query("SELECT temp_band, ROUND(AVG(total_revenue),2) AS avg_rev FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_WEATHER_REVENUE WHERE temp_band IS NOT NULL GROUP BY temp_band")
        if not weather_rev.empty:
            order = ["freezing", "cold", "mild", "warm", "hot"]
            wbar = alt.Chart(weather_rev).mark_bar().encode(
                x=alt.X("TEMP_BAND:N", sort=order, title="Temperature Band"),
                y=alt.Y("AVG_REV:Q", title="Avg Daily Revenue ($)"),
                color=alt.Color("TEMP_BAND:N", sort=order, legend=None),
            ).properties(height=250)
            st.altair_chart(wbar, use_container_width=True)

# ─── PAGE 3: Forecasting & Anomalies ──────────────────────────────────────────
elif page == "Forecasting & Anomalies":
    st.title("📈 Forecasting & Anomalies")

    st.subheader("Revenue Forecast")
    series_opt = st.selectbox("Series", ["Total", "DTC", "wholesale"], key="fc_series")

    actuals = run_sv(
        SV_MARKETING,
        dimensions="daily_revenue.order_date, daily_revenue.channel",
        facts="daily_revenue.total_revenue",
    )
    actuals.columns = [c.upper() for c in actuals.columns]
    if not actuals.empty:
        actuals["ORDER_DATE"] = pd.to_datetime(actuals["ORDER_DATE"])
        if series_opt == "Total":
            act = actuals.groupby("ORDER_DATE", as_index=False)["TOTAL_REVENUE"].sum()
        else:
            act = actuals[actuals["CHANNEL"] == series_opt].groupby("ORDER_DATE", as_index=False)["TOTAL_REVENUE"].sum()
        act.rename(columns={"ORDER_DATE": "DATE", "TOTAL_REVENUE": "VALUE"}, inplace=True)
        act["TYPE"] = "Actual"

        fc_series_val = series_opt if series_opt == "Total" else series_opt
        fc = run_sv(
            SV_ML,
            dimensions="forecasts.series, forecasts.ts",
            facts="forecasts.forecast, forecasts.lower_bound, forecasts.upper_bound",
            where=f"forecasts.model_name = 'total_revenue' AND forecasts.series = '{fc_series_val}'",
        )
        fc.columns = [c.upper() for c in fc.columns]

        if not fc.empty:
            fc["TS"] = pd.to_datetime(fc["TS"])
            fc_df = fc.rename(columns={"TS": "DATE", "FORECAST": "VALUE"})
            fc_df["TYPE"] = "Forecast"

            combined = pd.concat([act[["DATE", "VALUE", "TYPE"]], fc_df[["DATE", "VALUE", "TYPE"]]])
            line = alt.Chart(combined).mark_line(strokeWidth=2).encode(
                x="DATE:T", y=alt.Y("VALUE:Q", title="Revenue ($)"),
                color="TYPE:N",
                strokeDash=alt.StrokeDash("TYPE:N", legend=None),
            )
            band = alt.Chart(fc).mark_area(opacity=0.15).encode(
                x="TS:T", y="LOWER_BOUND:Q", y2="UPPER_BOUND:Q",
            )
            st.altair_chart((band + line).properties(height=350), use_container_width=True)

    st.subheader("Anomaly Detection")
    anom_opt = st.selectbox("Metric", ["Ad Spend", "DTC Conversion Rate", "Wholesale Orders"], key="anom_series")
    anom_map = {"Ad Spend": "spend_subchannel", "DTC Conversion Rate": "dtc_conversion", "Wholesale Orders": "wholesale_product"}
    model = anom_map[anom_opt]

    if model == "dtc_conversion":
        anom = run_sv(
            SV_ML,
            dimensions="anomalies.series, anomalies.ts, anomalies.is_anomaly",
            facts="anomalies.y, anomalies.forecast, anomalies.lower_bound, anomalies.upper_bound, anomalies.percentile",
            where=f"anomalies.model_name = '{model}'",
        )
    else:
        anom = run_sv(
            SV_ML,
            dimensions="anomalies.series, anomalies.ts, anomalies.is_anomaly",
            facts="anomalies.y, anomalies.forecast, anomalies.lower_bound, anomalies.upper_bound, anomalies.percentile",
            where=f"anomalies.model_name = '{model}'",
        )
    anom.columns = [c.upper() for c in anom.columns]
    if not anom.empty:
        anom["TS"] = pd.to_datetime(anom["TS"])
        if model != "dtc_conversion":
            series_list = sorted(anom["SERIES"].unique())
            sel_s = st.selectbox("Sub-series", series_list, key="anom_sub")
            anom = anom[anom["SERIES"] == sel_s]

        base_line = alt.Chart(anom).mark_line(color="steelblue").encode(x="TS:T", y=alt.Y("Y:Q", title="Value"))
        fc_line = alt.Chart(anom).mark_line(color="gray", strokeDash=[4, 4]).encode(x="TS:T", y="FORECAST:Q")
        band = alt.Chart(anom).mark_area(opacity=0.1, color="gray").encode(x="TS:T", y="LOWER_BOUND:Q", y2="UPPER_BOUND:Q")
        anomalies_df = anom[anom["IS_ANOMALY"] == True]
        points = alt.Chart(anomalies_df).mark_circle(size=80, color="red").encode(x="TS:T", y="Y:Q", tooltip=["TS", "Y", "PERCENTILE"])
        st.altair_chart((band + fc_line + base_line + points).properties(height=350), use_container_width=True)
        st.caption(f"🔴 {len(anomalies_df)} anomalies detected")

# ─── PAGE 4: AI Insights ──────────────────────────────────────────────────────
elif page == "AI Insights":
    st.title("🤖 AI Insights")

    st.subheader("Sentiment by Channel")
    sent = run_sv(
        SV_REVIEWS,
        dimensions="sentiment.channel, sentiment.review_text",
        facts="sentiment.sentiment_score",
    )
    sent.columns = [c.upper() for c in sent.columns]
    if not sent.empty:
        hist = alt.Chart(sent).mark_bar(opacity=0.7).encode(
            x=alt.X("SENTIMENT_SCORE:Q", bin=alt.Bin(maxbins=30), title="Sentiment Score"),
            y=alt.Y("count()", title="Reviews"),
            color="CHANNEL:N",
        ).properties(height=250)
        st.altair_chart(hist, use_container_width=True)

    st.subheader("Theme Summaries")
    themes = run_sv(SV_REVIEWS, dimensions="themes.channel, themes.themes")
    themes.columns = [c.upper() for c in themes.columns]
    if not themes.empty:
        cols = st.columns(len(themes))
        for i, row in themes.iterrows():
            with cols[i]:
                st.markdown(f"**{row['CHANNEL']}**")
                st.markdown(row["THEMES"][:1000])

    st.subheader("Campaign Performance Tiers")
    tiers = run_sv(
        SV_MARKETING,
        dimensions="campaign_tiers.performance_tier",
        metrics="orders.total_orders",
    )
    tiers.columns = [c.upper() for c in tiers.columns]
    if not tiers.empty:
        donut = alt.Chart(tiers).mark_arc(innerRadius=50).encode(
            theta="TOTAL_ORDERS:Q", color="PERFORMANCE_TIER:N",
            tooltip=["PERFORMANCE_TIER", "TOTAL_ORDERS"],
        ).properties(height=300)
        st.altair_chart(donut, use_container_width=True)

    st.subheader("Campaign Executive Summaries")
    summaries = run_sv(
        SV_MARKETING,
        dimensions="campaign_summaries.campaign_name, campaign_summaries.channel, campaign_summaries.sub_channel, campaign_summaries.executive_summary",
    )
    summaries.columns = [c.upper() for c in summaries.columns]
    if not summaries.empty:
        for ch in sorted(summaries["CHANNEL"].unique()):
            st.markdown(f"#### {ch.upper()}")
            ch_data = summaries[summaries["CHANNEL"] == ch]
            for _, r in ch_data.iterrows():
                with st.expander(f"{r['CAMPAIGN_NAME']} ({r['SUB_CHANNEL']})"):
                    st.markdown(r["EXECUTIVE_SUMMARY"])

    st.subheader("Review Extractions")
    extracts = run_sv(
        SV_REVIEWS,
        dimensions="extracts.product_category, extracts.product_name, extracts.channel, extracts.review_text, extracts.product_feedback, extracts.competitor_mention, extracts.recommendation",
        facts="extracts.rating",
        limit="50",
    )
    extracts.columns = [c.upper() for c in extracts.columns]
    if not extracts.empty:
        with st.expander("Show review extractions"):
            st.dataframe(extracts, use_container_width=True)

# ─── PAGE 5: Marketing Agent ──────────────────────────────────────────────────
elif page == "Marketing Agent":
    st.title("💬 Marketing Agent")
    st.caption("Ask questions about campaigns, revenue, forecasts, reviews, MMM, geo-targeting, and CLV.")

    if "agent_messages" not in st.session_state:
        st.session_state.agent_messages = []

    for msg in st.session_state.agent_messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if "sql" in msg:
                with st.expander("SQL"):
                    st.code(msg["sql"], language="sql")
            if "df" in msg:
                st.dataframe(msg["df"], use_container_width=True)

    suggestions = [
        "What were the top 5 campaigns by ROAS?",
        "Show total revenue by product category",
        "Which anomalies were detected in ad spend?",
        "What is the average sentiment by product category?",
        "Which sub-channel has the best ROI in the MMM?",
        "How many customers are at risk of churning?",
    ]

    cols = st.columns(3)
    clicked = None
    for i, s in enumerate(suggestions):
        if cols[i % 3].button(s, key=f"sug_{i}"):
            clicked = s

    prompt = st.chat_input("Ask the marketing agent...")
    question = clicked or prompt

    if question:
        st.session_state.agent_messages.append({"role": "user", "content": question})
        with st.chat_message("user"):
            st.markdown(question)

        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                try:
                    response = call_agent(question)
                    text_parts = render_agent_response(response)
                    text_summary = " ".join(t for t in text_parts if t.strip()).strip()
                    st.session_state.agent_messages.append(
                        {"role": "assistant", "content": text_summary or "(see results above)"}
                    )
                except Exception as e:
                    st.error(f"Error: {e}")
                    st.session_state.agent_messages.append({"role": "assistant", "content": f"Error: {e}"})
