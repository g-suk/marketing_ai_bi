# Summit Gear Co. -- Marketing AI+BI Lab Guide

**Duration:** 60 minutes | **Audience:** Mixed technical | **Difficulty:** Intermediate

---

## Prerequisites

- Snowflake account with ACCOUNTADMIN access
- Cortex Code enabled (for dashboard building)
- Web browser with Snowsight

---

## Part 0: Connect to the GitHub Repository (Optional)

If you want to run the lab scripts directly from Snowflake Workspaces via Git integration:

### 0.1 Create a Git Integration

As ACCOUNTADMIN, run:

```sql
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE API INTEGRATION github_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/g-suk')
  ENABLED = TRUE;
```

### 0.2 Create the Database First (needed for the Git Repository object)

```sql
CREATE OR REPLACE DATABASE MARKETING_AI_BI;
CREATE SCHEMA IF NOT EXISTS MARKETING_AI_BI.DEMO_DATA;
```

### 0.3 Create the Git Repository

```sql
CREATE OR REPLACE GIT REPOSITORY MARKETING_AI_BI.DEMO_DATA.MARKETING_LAB_REPO
  API_INTEGRATION = github_api_integration
  ORIGIN = 'https://github.com/g-suk/marketing-data-ai-bi.git';
```

### 0.4 Fetch and Browse

```sql
ALTER GIT REPOSITORY MARKETING_AI_BI.DEMO_DATA.MARKETING_LAB_REPO FETCH;

-- List files in the repo
SHOW GIT BRANCHES IN MARKETING_AI_BI.DEMO_DATA.MARKETING_LAB_REPO;

-- Run the deploy script directly from the repo
EXECUTE IMMEDIATE FROM @MARKETING_AI_BI.DEMO_DATA.MARKETING_LAB_REPO/branches/main/deploy_all.sql;
```

> **Tip:** You can also browse the repository files in Snowsight under **Data > Databases > MARKETING_AI_BI > DEMO_DATA > Git Repositories**.

---

## Part 1: Setup (0-5 min)

### 1.1 Run the Deploy Script

**Option A (Git):** If you set up the Git integration above, the deploy script already ran.

**Option B (Copy/Paste):** Open a Snowsight SQL worksheet and paste the contents of `deploy_all.sql`. Execute it.

This creates:
- Role: `MARKETING_LAB_ROLE`
- Database: `MARKETING_AI_BI` with schema `DEMO_DATA`
- 6 source tables with synthetic data
- 6 dynamic tables for analytics transformations

Verify with the row count query at the bottom of the script:

| Table | Expected Rows |
|-------|--------------|
| WHOLESALE_PARTNERS | 30 |
| CAMPAIGNS | 60 |
| CUSTOMERS | 8,000 |
| ORDERS | 25,000 |
| MARKETING_SPEND | ~4,000 |
| PRODUCT_REVIEWS | 1,500 |

### 1.2 Install Marketplace Listings

Navigate to **Data Products > Marketplace** and install these free listings:

1. **Snowflake Public Data** -- [Install](https://app.snowflake.com/marketplace/listing/GZTSZ290BV255)
   - Installs as database: `SNOWFLAKE_PUBLIC_DATA_PAID`
2. **SMS CustomerConnect 360 Sample** -- [Install](https://app.snowflake.com/marketplace/listing/GZT0ZU1ICEX)
   - Installs as database: `CUSTOMERCONNECT360__SAMPLE`

Bonus (pre-install if time allows):
3. **GWI Core** -- [Install](https://app.snowflake.com/marketplace/listing/GZ2FSZGU5YB)
   - Installs as database: `GWI_OPEN_DATA`
4. **Weather Source Global Weather** -- [Install](https://app.snowflake.com/marketplace/listing/GZSOZ1LLD8)
   - Installs as database: `FROSTBYTE_WEATHERSOURCE`

After installing, grant access to your lab role. As ACCOUNTADMIN:

```sql
USE ROLE ACCOUNTADMIN;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE_PUBLIC_DATA_PAID TO ROLE MARKETING_LAB_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE CUSTOMERCONNECT360__SAMPLE TO ROLE MARKETING_LAB_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE GWI_OPEN_DATA              TO ROLE MARKETING_LAB_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE FROSTBYTE_WEATHERSOURCE    TO ROLE MARKETING_LAB_ROLE;
```

---

## Part 2: Marketplace + Dynamic Tables (5-10 min)

### 2.1 Verify Enrichment Views

Run `sql/02_data/02_marketplace_enrichment.sql` to create enrichment views.

Test a sample query:
```sql
SELECT * FROM V_ECONOMIC_INDICATORS LIMIT 10;
SELECT * FROM V_CUSTOMER_ENRICHMENT LIMIT 10;
SELECT * FROM V_NATIONAL_WEATHER WHERE weather_date >= '2024-07-01' LIMIT 10;
```

### 2.2 Explore Dynamic Table Lineage

In Snowsight, navigate to **Data > Databases > MARKETING_AI_BI > DEMO_DATA > Dynamic Tables**.

Click on `DT_CAMPAIGN_METRICS` and explore the **Lineage** tab. Notice how it joins CAMPAIGNS, MARKETING_SPEND, and ORDERS automatically.

Try a query:
```sql
SELECT campaign_name, channel, sub_channel, total_spend, campaign_revenue, roas
FROM DT_CAMPAIGN_METRICS
ORDER BY roas DESC NULLS LAST
LIMIT 10;
```

---

## Part 3: Cortex ML Functions (10-20 min)

### 3.1 Forecasting

Run `sql/04_ml/01_forecast.sql`.

This trains two FORECAST models:
- **Total revenue** (single series)
- **Revenue by channel** (multi-series)

Explore results:
```sql
SELECT series, MIN(ts) AS forecast_start, MAX(ts) AS forecast_end, COUNT(*) AS points
FROM FORECAST_RESULTS
GROUP BY series;
```

### 3.2 Anomaly Detection

Run `sql/04_ml/02_anomaly_detection.sql`.

This trains three ANOMALY_DETECTION models and should surface the seeded anomalies:
- **Nov 2024:** Influencer overspend spike
- **Feb 2025:** DTC conversion crash
- **Sep 2025:** Wholesale order surge

Explore anomalies:
```sql
SELECT series, ts, y, forecast, is_anomaly, percentile
FROM ANOMALY_DETECTION_RESULTS
WHERE is_anomaly = TRUE
ORDER BY ts;
```

---

## Part 4: Cortex AI Functions (20-30 min)

Run `sql/05_ai/01_ai_functions.sql`.

This applies 5 AI functions and materializes results:

| Function | Table | What it Does |
|----------|-------|-------------|
| SENTIMENT | AI_SENTIMENT_RESULTS | Scores review sentiment (-1 to 1) |
| CLASSIFY | AI_CLASSIFY_RESULTS | Classifies campaigns into performance tiers |
| EXTRACT | AI_EXTRACT_RESULTS | Extracts structured fields from review text |
| COMPLETE | AI_COMPLETE_RESULTS | Generates executive summaries per campaign |
| SUMMARIZE | AI_AGG_RESULTS | Aggregates themes by channel |

Explore:
```sql
SELECT channel, ROUND(AVG(sentiment_score), 3) AS avg_sentiment
FROM AI_SENTIMENT_RESULTS
GROUP BY channel;

SELECT performance_tier, COUNT(*) AS campaign_count
FROM AI_CLASSIFY_RESULTS
GROUP BY performance_tier;
```

---

## Part 5: Build Your Dashboard (30-50 min)

This is the creative section! You'll use **Cortex Code** to build a Streamlit dashboard by pasting progressive prompts.

### Getting Started

1. Open Cortex Code in your Snowflake account
2. Start a new conversation
3. Paste the prompts below one at a time

### Prompt Sequence

Paste each prompt, review the generated code, tweak to your liking, then move to the next:

1. **Foundation + KPIs** -- see `prompts/01_foundation_kpis.md`
2. **Channel Deep Dive** -- see `prompts/02_channel_deep_dive.md`
3. **Forecasting & Anomalies** -- see `prompts/03_forecasting_anomalies.md`
4. **AI Insights** -- see `prompts/04_ai_insights.md`
5. **Cortex Agent** -- see `prompts/05_cortex_agent.md`

### Tips

- Customize! Change chart types, add filters, rearrange the layout
- Try your own questions with the Agent
- If stuck or running behind, use the backup app (see below)

### Backup App

If you need a working app quickly:

```sql
USE ROLE MARKETING_LAB_ROLE;
USE DATABASE MARKETING_AI_BI;
USE SCHEMA DEMO_DATA;

PUT file:///path/to/streamlit/streamlit_app.py @STREAMLIT_STAGE/streamlit/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file:///path/to/streamlit/environment.yml @STREAMLIT_STAGE/streamlit/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

CREATE STREAMLIT MARKETING_AI_BI.DEMO_DATA.SUMMIT_GEAR_DASHBOARD
  ROOT_LOCATION = '@MARKETING_AI_BI.DEMO_DATA.STREAMLIT_STAGE/streamlit'
  MAIN_FILE = 'streamlit_app.py'
  QUERY_WAREHOUSE = 'COMPUTE_WH';
```

---

## Part 6: Wrap-up (50-60 min)

### 6.1 Semantic View + Agent

Run `sql/06_semantic/01_semantic_view.sql` and `sql/06_semantic/02_cortex_agent.sql` if not already done.

Try the agent from a SQL worksheet:
```sql
SELECT SNOWFLAKE.CORTEX.AGENT(
    'SUMMIT_GEAR_AGENT',
    'Which DTC sub-channel has the best ROAS?'
);
```

### 6.2 Discussion Prompts

- How do DTC and wholesale channels compare on key metrics?
- What anomalies did the ML models detect? Do they match the known events?
- What themes emerge from the AI review analysis?
- How could Marketplace data improve your own forecasting models?

### 6.3 Teardown

When finished, clean up all resources:

```sql
-- Run teardown_all.sql as ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;
DROP DATABASE IF EXISTS MARKETING_AI_BI;
DROP ROLE IF EXISTS MARKETING_LAB_ROLE;
```

---

## Resources

- [Cortex ML Functions](https://docs.snowflake.com/en/guides-overview-ml-functions)
- [Cortex AI Functions](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-llm-functions)
- [Dynamic Tables](https://docs.snowflake.com/en/user-guide/dynamic-tables-about)
- [Snowflake Marketplace](https://app.snowflake.com/marketplace)
- [Streamlit in Snowflake](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)
- [Git Integration](https://docs.snowflake.com/en/developer-guide/git/git-overview)
