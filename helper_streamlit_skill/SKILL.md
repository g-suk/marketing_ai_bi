# Streamlit-in-Snowflake (SiS) + Cortex Agent Skill

Use this skill when building Streamlit apps that run inside Snowflake (Streamlit-in-Snowflake) and/or integrate with Cortex Agents via the REST API. This covers the SiS runtime constraints, the `_snowflake` internal API, Cortex Agent response parsing, charting with Altair, and deployment via stage upload.

---

## 1. SiS Runtime Environment

### Connection
SiS apps use `get_active_session()` — NOT `st.connection("snowflake")`.

```python
from snowflake.snowpark.context import get_active_session
session = get_active_session()
df = session.sql("SELECT * FROM my_table").to_pandas()
```

- `st.connection("snowflake")` is for local/external Streamlit apps. Inside SiS, the session is pre-authenticated.
- `session.sql(...)` returns a Snowpark DataFrame; call `.to_pandas()` to convert.

### Python Restrictions
The SiS Python environment has restrictions compared to standard Python:

- **NO `nonlocal` keyword in nested functions** — causes `SyntaxError: no binding for nonlocal found`. Always use module-level functions with mutable arguments (lists) instead of closures.
- **NO arbitrary pip packages** — only packages listed in `environment.yml` from the `snowflake` conda channel.
- **NO file system writes** — the app runs in a read-only sandbox.
- **NO subprocess or os.system calls**.
- **NO `st.cache_resource` or `st.cache_data`** — SiS manages caching differently. Use `@st.cache_data(ttl=...)` only if the SiS version supports it; otherwise rely on Snowpark's built-in caching.

### environment.yml
SiS uses a conda-based `environment.yml` (NOT `pyproject.toml` or `requirements.txt`):

```yaml
name: sf_env
channels:
  - snowflake
dependencies:
  - streamlit
  - snowflake-snowpark-python
  - altair
  - pandas
```

Only packages available in the `snowflake` conda channel can be used. Plotly is NOT available — use Altair for all charts.

---

## 2. Charting with Altair (Required for SiS)

SiS renders charts as SVG. Plotly and other WebGL-based libraries are not supported.

### Import
```python
import altair as alt
```

### Basic Patterns

**Line chart:**
```python
chart = (
    alt.Chart(df)
    .mark_line(strokeWidth=2)
    .encode(
        x=alt.X("DATE:T", title="Date"),
        y=alt.Y("VALUE:Q", title="Revenue ($)"),
        color=alt.Color("CHANNEL:N", title="Channel"),
    )
    .properties(height=350)
)
st.altair_chart(chart, use_container_width=True)
```

**Bar chart:**
```python
bar = (
    alt.Chart(df)
    .mark_bar()
    .encode(
        x=alt.X("REVENUE:Q", title="Revenue ($)"),
        y=alt.Y("CATEGORY:N", sort="-x", title="Category"),
        color=alt.Color("CATEGORY:N", legend=None),
    )
    .properties(height=300)
)
st.altair_chart(bar, use_container_width=True)
```

**Donut chart:**
```python
donut = (
    alt.Chart(df)
    .mark_arc(innerRadius=50)
    .encode(
        theta=alt.Theta("COUNT:Q"),
        color=alt.Color("TIER:N", title="Tier"),
        tooltip=["TIER", "COUNT"],
    )
    .properties(height=300)
)
st.altair_chart(donut, use_container_width=True)
```

**Histogram:**
```python
hist = alt.Chart(df).mark_bar(opacity=0.7).encode(
    x=alt.X("SCORE:Q", bin=alt.Bin(maxbins=30), title="Score"),
    y=alt.Y("count()", title="Count"),
    color="CHANNEL:N",
).properties(height=250)
st.altair_chart(hist, use_container_width=True)
```

**Layered chart (forecast with confidence band):**
```python
line = alt.Chart(combined).mark_line(strokeWidth=2).encode(
    x="DATE:T", y=alt.Y("VALUE:Q", title="Revenue ($)"),
    color="TYPE:N",
    strokeDash=alt.StrokeDash("TYPE:N", legend=None),
)
band = alt.Chart(forecast_df).mark_area(opacity=0.15).encode(
    x="TS:T", y="LOWER_BOUND:Q", y2="UPPER_BOUND:Q",
)
st.altair_chart((band + line).properties(height=350), use_container_width=True)
```

**Anomaly chart (line + expected + red markers):**
```python
base = alt.Chart(df).mark_line(color="steelblue").encode(x="TS:T", y=alt.Y("Y:Q", title="Value"))
expected = alt.Chart(df).mark_line(color="gray", strokeDash=[4, 4]).encode(x="TS:T", y="FORECAST:Q")
band = alt.Chart(df).mark_area(opacity=0.1, color="gray").encode(x="TS:T", y="LOWER_BOUND:Q", y2="UPPER_BOUND:Q")
anomalies = alt.Chart(df[df["IS_ANOMALY"] == True]).mark_circle(size=80, color="red").encode(
    x="TS:T", y="Y:Q", tooltip=["TS", "Y", "PERCENTILE"]
)
st.altair_chart((band + expected + base + anomalies).properties(height=350), use_container_width=True)
```

**Normalized stacked bar:**
```python
total = df.groupby("SEGMENT")["CNT"].transform("sum")
df["PCT"] = df["CNT"] / total
sbar = alt.Chart(df).mark_bar().encode(
    x=alt.X("PCT:Q", stack="normalize", title="Share"),
    y=alt.Y("SEGMENT:N", title="Segment"),
    color="TIER:N",
).properties(height=250)
st.altair_chart(sbar, use_container_width=True)
```

### Key Rules
- Always set `.properties(height=N)` — default height is too small.
- Use `use_container_width=True` on `st.altair_chart()`.
- Column names in Snowflake DataFrames are UPPERCASE — use `df.columns = [c.upper() for c in df.columns]` after querying.
- Convert date columns: `df["DATE_COL"] = pd.to_datetime(df["DATE_COL"])`.

---

## 3. Cortex Agent REST API (SiS)

### The `_snowflake` Module
SiS provides a private `_snowflake` module for making authenticated REST API calls within Snowflake. There is NO SQL function for calling Cortex Agents — you MUST use this REST API.

```python
import _snowflake
import json
```

### Agent Endpoint Format
```
/api/v2/databases/{DB}/schemas/{SCHEMA}/agents/{AGENT_NAME}:run
```

### Calling the Agent

```python
AGENT_ENDPOINT = "/api/v2/databases/MY_DB/schemas/MY_SCHEMA/agents/MY_AGENT:run"
AGENT_TIMEOUT = 60000

def call_agent(question, chat_history=None):
    messages = []
    if chat_history:
        messages.extend(chat_history)
    messages.append({"role": "user", "content": [{"type": "text", "text": question}]})
    payload = {"messages": messages}
    resp = _snowflake.send_snow_api_request(
        "POST", AGENT_ENDPOINT, {}, {}, payload, None, AGENT_TIMEOUT
    )
    if resp["status"] != 200:
        resp = _snowflake.send_snow_api_request(
            "POST", AGENT_ENDPOINT, {}, {"stream": True}, payload, None, AGENT_TIMEOUT
        )
        if resp["status"] != 200:
            raise Exception(f"Agent API returned HTTP {resp['status']}: {resp.get('content', '')}")
    return json.loads(resp["content"])
```

**Key details:**
- `_snowflake.send_snow_api_request(method, path, headers, query_params, body, ?, timeout_ms)`
- Returns a dict with `status` (int) and `content` (str).
- `json.loads(resp["content"])` can return EITHER a dict OR a list depending on the response format.
- **Always retry with `{"stream": True}` as query param** if the first request returns non-200.

---

## 4. Cortex Agent Response Parsing (CRITICAL)

The Cortex Agent API has **multiple response formats**. Your parser MUST handle all of them. The response from `json.loads(resp["content"])` can be:

### Format A: Single message dict
```json
{"message": {"content": [{"type": "text", "text": "..."}, {"type": "tool_results", "tool_results": {"content": [{"type": "json", "json": {"sql": "...", "text": "...", "result_set": {...}}}]}}]}}
```

### Format B: SSE event list
```json
[{"event": "message.delta", "data": {"delta": {"content": [{"type": "text", "text": "..."}]}}}, {"event": "message.delta", "data": {"delta": {"content": [{"type": "tool_results", "tool_results": {"content": [...]}}]}}}]
```

### Format C: Flat content dict
```json
{"content": [{"type": "text", "text": "..."}, {"type": "tool_result", "tool_result": {"content": [...]}}]}
```

### SSE Event Types
When the response is a list, each event has an `event` field:
- `message.delta` — primary format; content is in `data.delta.content[]`
- `response.text` — plain text in `data.text`
- `response.tool_result` — tool results in `data.content[]`
- `response.table` — result set in `data.result_set`
- `response` — full response in `data.content[]`

### Result Set Structure
Tool results contain SQL query results as `result_set`:
```json
{
  "resultSetMetaData": {"rowType": [{"name": "COL1"}, {"name": "COL2"}]},
  "data": [["val1", "val2"], ["val3", "val4"]]
}
```

Convert to DataFrame:
```python
cols = [c["name"] for c in rs.get("resultSetMetaData", {}).get("rowType", [])]
rows = rs.get("data", [])
if cols and rows:
    df = pd.DataFrame(rows, columns=cols)
```

### Complete Response Parser

**IMPORTANT: Use module-level functions with mutable list arguments. Do NOT use closures or `nonlocal` — they cause SyntaxError in SiS.**

```python
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

    # Format A: single message dict
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

    # Format B: SSE event list
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

    # Format C: flat content dict
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

    # Fallback: recursive walk if nothing was extracted
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
```

### Chat UI Integration

```python
if "agent_messages" not in st.session_state:
    st.session_state.agent_messages = []

for msg in st.session_state.agent_messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

suggestions = ["What were the top 5 campaigns by ROAS?", "Show total revenue by category"]
cols = st.columns(3)
clicked = None
for i, s in enumerate(suggestions):
    if cols[i % 3].button(s, key=f"sug_{i}"):
        clicked = s

prompt = st.chat_input("Ask a question...")
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
```

---

## 5. Common Pitfalls & Fixes

| Problem | Cause | Fix |
|---|---|---|
| `SyntaxError: no binding for nonlocal` | Nested function with `nonlocal` keyword | Use module-level functions; pass mutable lists as arguments |
| `'list' object has no attribute 'get'` | Assumed response is dict but it's a list (SSE events) | Check `isinstance(response, list)` before calling `.get()` |
| Agent returns no text | Parsing only one response format | Use the full `process_sse_response()` parser above |
| `ModuleNotFoundError: plotly` | Plotly not available in SiS conda channel | Use Altair for all charts |
| `use_column_width` deprecation warning | Old Streamlit API | Replace with `use_container_width=True` |
| Uppercase column mismatch | Snowflake returns UPPERCASE columns | Use `df.columns = [c.upper() for c in df.columns]` |
| Date filtering fails | Date column is string not datetime | `df["DATE"] = pd.to_datetime(df["DATE"])` |

---

## 6. SiS Deployment via Stage Upload

SiS apps are deployed by uploading files to a stage and creating the Streamlit object.

### Upload Files
```sql
PUT 'file:///path/to/streamlit_app.py' @DB.SCHEMA.STAGE/streamlit/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT 'file:///path/to/environment.yml' @DB.SCHEMA.STAGE/streamlit/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
```

### Create Streamlit Object
```sql
CREATE OR REPLACE STREAMLIT DB.SCHEMA.MY_APP
  ROOT_LOCATION = '@DB.SCHEMA.STAGE/streamlit'
  MAIN_FILE = 'streamlit_app.py'
  QUERY_WAREHOUSE = COMPUTE_WH
  TITLE = 'My Dashboard';
```

### Update Existing App
Just re-PUT the files with `OVERWRITE=TRUE`. The app picks up changes on next load.

---

## 7. Layout Best Practices

### Page Structure
Use sidebar navigation with `st.radio`:
```python
pages = {"Page 1": "icon1", "Page 2": "icon2"}
with st.sidebar:
    st.title("My App")
    page = st.radio("Navigate", list(pages.keys()),
                     format_func=lambda x: f"{pages[x]} {x}")

if page == "Page 1":
    # ... page content
elif page == "Page 2":
    # ... page content
```

### Metric Cards
```python
c1, c2, c3 = st.columns(3)
c1.metric("Revenue", f"${total_rev:,.0f}")
c2.metric("Orders", f"{total_orders:,}")
c3.metric("ROAS", f"{roas:.2f}x")
```

### Date Range Filters
```python
daily["DATE"] = pd.to_datetime(daily["DATE"])
d1, d2 = st.columns(2)
min_d, max_d = daily["DATE"].min().date(), daily["DATE"].max().date()
start = d1.date_input("Start", min_d, min_value=min_d, max_value=max_d)
end = d2.date_input("End", max_d, min_value=min_d, max_value=max_d)
mask = (daily["DATE"].dt.date >= start) & (daily["DATE"].dt.date <= end)
filtered = daily[mask]
```

### Tabs
```python
tab1, tab2, tab3 = st.tabs(["Tab A", "Tab B", "Tab C"])
with tab1:
    st.subheader("Tab A Content")
    # ... content
```

### Expanders for Detail
```python
with st.expander("Show details"):
    st.dataframe(detail_df, use_container_width=True)
```

---

## 8. Querying Semantic Views

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
    if limit:
        parts.append(f"    LIMIT {limit}")
    q = "\n".join(parts) + "\n)"
    if order:
        q += f" ORDER BY {order}"
    return session.sql(q).to_pandas()
```

**Rules:**
- `DIMENSIONS` = grouping columns (entity.column format)
- `METRICS` = aggregates (SUM, COUNT, AVG) — cross-entity joins allowed
- `FACTS` = raw values — same-entity only, no cross-entity joins
- All column references use `entity.column` format
- Column names come back UPPERCASE from Snowflake
