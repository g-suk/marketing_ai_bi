# Summit Gear Co. -- Marketing AI+BI Lab Guide

**Duration:** 60 minutes | **Audience:** Mixed technical | **Difficulty:** Intermediate

---

## Prerequisites

- Snowflake account with ACCOUNTADMIN access
- Cortex Code enabled (for dashboard building)
- Web browser with Snowsight

---

## Part 0: Environment Setup (0-5 min)

### 0.1 Create the Database, Schema, and Git Integration

Open a SQL worksheet as ACCOUNTADMIN and run:

```sql
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE DATABASE MARKETING_AI_BI;
CREATE SCHEMA IF NOT EXISTS MARKETING_AI_BI.MARKETING_RAW;

CREATE OR REPLACE API INTEGRATION github_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/g-suk')
  ENABLED = TRUE;
```

### 0.2 Create a Workspace from Git

1. In Snowsight, navigate to **Projects > Workspaces**
2. Click **+ Create Workspace** (top right)
3. Select **Create Workspace from Git Repository**
4. Paste the repository URL: `https://github.com/g-suk/marketing_ai_bi.git`
5. Select **Public Repository** (no credentials needed)
6. Choose database `MARKETING_AI_BI` and schema `MARKETING_RAW`
7. Click **Create**

You now have a workspace with all lab SQL files. For each step below, open the referenced file in the workspace and run it.

### 0.3 Run the Setup Script

Open `sql/01_setup/setup.sql` in your workspace and run it.

This creates:
- Role: `MARKETING_LAB_ROLE` (granted to your current user automatically)
- Schema: `MARKETING_ANALYTICS` (for dynamic tables and semantic objects)
- Stages for Streamlit and data
- 6 source tables with synthetic data (WHOLESALE_PARTNERS, CAMPAIGNS, CUSTOMERS, ORDERS, MARKETING_SPEND, PRODUCT_REVIEWS)

### 0.4 Install Marketplace Listings

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

After installing, grant access. As ACCOUNTADMIN:

```sql
USE ROLE ACCOUNTADMIN;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE_PUBLIC_DATA_PAID TO ROLE MARKETING_LAB_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE CUSTOMERCONNECT360__SAMPLE TO ROLE MARKETING_LAB_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE GWI_OPEN_DATA              TO ROLE MARKETING_LAB_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE FROSTBYTE_WEATHERSOURCE    TO ROLE MARKETING_LAB_ROLE;
```

---

## Part 1: Marketplace Enrichment + Dynamic Tables (10-15 min)

### 1.1 Create Enrichment Views

Open `sql/02_data/02_marketplace_enrichment.sql` in your workspace and run it.

This creates views that join your synthetic data to the Marketplace shared databases.

Test a sample query:
```sql
SELECT * FROM MARKETING_AI_BI.MARKETING_RAW.V_ECONOMIC_INDICATORS LIMIT 10;
SELECT * FROM MARKETING_AI_BI.MARKETING_RAW.V_CUSTOMER_ENRICHMENT LIMIT 10;
SELECT * FROM MARKETING_AI_BI.MARKETING_RAW.V_NATIONAL_WEATHER WHERE weather_date >= '2024-07-01' LIMIT 10;
```

### 1.2 Create Dynamic Tables

Open `sql/03_dynamic_tables/01_dynamic_tables.sql` in your workspace and run it.

This creates 6 dynamic tables in the `MARKETING_ANALYTICS` schema that automatically transform raw data into analytics-ready tables.

### 1.3 Explore Dynamic Table Lineage

In Snowsight, navigate to **Data > Databases > MARKETING_AI_BI > MARKETING_ANALYTICS > Dynamic Tables**.

Click on `DT_CAMPAIGN_METRICS` and explore the **Lineage** tab. Notice how it joins CAMPAIGNS, MARKETING_SPEND, and ORDERS automatically.

Try a query:
```sql
SELECT campaign_name, channel, sub_channel, total_spend, campaign_revenue, roas
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.DT_CAMPAIGN_METRICS
ORDER BY roas DESC NULLS LAST
LIMIT 10;
```

---

## Part 2: Cortex ML Functions (15-25 min)

### 2.1 Forecasting

Open `sql/04_ml/01_forecast.sql` in your workspace and run it.

This trains two FORECAST models:
- **Total revenue** (single series)
- **Revenue by channel** (multi-series: DTC vs wholesale)

> **Note:** Model training takes 2-3 minutes. This is a good time to discuss how Snowflake ML works.

Explore results:
```sql
SELECT series, MIN(ts) AS forecast_start, MAX(ts) AS forecast_end, COUNT(*) AS points
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.FORECAST_RESULTS
GROUP BY series;
```

### 2.2 Anomaly Detection

Open `sql/04_ml/02_anomaly_detection.sql` in your workspace and run it.

This trains three ANOMALY_DETECTION models and should surface the seeded anomalies:
- **Nov 2024:** Influencer overspend spike (3x normal)
- **Feb 2025:** DTC conversion crash (80% drop)
- **Sep 2025:** Wholesale order surge (5x normal)

Explore anomalies:
```sql
SELECT series, ts, y, forecast, is_anomaly, percentile
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.ANOMALY_DETECTION_RESULTS
WHERE is_anomaly = TRUE
ORDER BY ts;
```

---

## Part 3: Cortex AI Functions (25-35 min)

Open `sql/05_ai/01_ai_functions.sql` in your workspace and run it.

> **Note:** AI functions process all rows and may take 3-5 minutes to complete.

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
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.AI_SENTIMENT_RESULTS
GROUP BY channel;

SELECT performance_tier, COUNT(*) AS campaign_count
FROM MARKETING_AI_BI.MARKETING_ANALYTICS.AI_CLASSIFY_RESULTS
GROUP BY performance_tier;
```

---

## Part 4: Semantic View + Cortex Agent (35-40 min)

### 4.1 Create the Semantic View

Open `sql/06_semantic/01_semantic_view.sql` in your workspace and run it.

This creates a semantic view that maps business concepts (metrics, dimensions, relationships) over the raw and analytics tables.

### 4.2 Create the Cortex Agent

Open `sql/06_semantic/02_cortex_agent.sql` in your workspace and run it.

Try the agent from a SQL worksheet:
```sql
SELECT SNOWFLAKE.CORTEX.AGENT(
    'SUMMIT_GEAR_AGENT',
    'Which DTC sub-channel has the best ROAS?'
);
```

---

## Part 5: Build Your Dashboard (40-55 min)

This is the creative section! You'll use **Cortex Code** to build a Streamlit dashboard in a single prompt.

### Getting Started

1. Open Cortex Code in your Snowflake account
2. Start a new conversation
3. Paste the prompt from `prompts/01_build_dashboard.md`

This single prompt gives Cortex Code everything it needs to generate a full 5-page dashboard with KPIs, channel analytics, forecasting, AI insights, and the Cortex Agent chat interface.

### Make It Your Own

Once the base app is running, check out `prompts/02_enhancements.md` for follow-up prompts you can paste to add features like:
- Dark theme and brand colors
- AI-generated executive summaries
- Customer segmentation drilldowns
- Anomaly drill-through with AI explanations
- Marketing funnel visualizations
- And more

### Tips

- Customize! Change chart types, add filters, rearrange the layout
- Try your own questions with the Agent
- If stuck or running behind, use the backup app (see below)

### Backup App

If you need a working app quickly:

```sql
USE ROLE MARKETING_LAB_ROLE;
USE DATABASE MARKETING_AI_BI;
USE SCHEMA MARKETING_RAW;

PUT file:///path/to/streamlit/streamlit_app.py @STREAMLIT_STAGE/streamlit/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file:///path/to/streamlit/environment.yml @STREAMLIT_STAGE/streamlit/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

CREATE STREAMLIT MARKETING_AI_BI.MARKETING_RAW.SUMMIT_GEAR_DASHBOARD
  ROOT_LOCATION = '@MARKETING_AI_BI.MARKETING_RAW.STREAMLIT_STAGE/streamlit'
  MAIN_FILE = 'streamlit_app.py'
  QUERY_WAREHOUSE = 'COMPUTE_WH';
```

---

## Part 6: Wrap-up (55-60 min)

### 6.1 Discussion Prompts

- How do DTC and wholesale channels compare on key metrics?
- What anomalies did the ML models detect? Do they match the known events?
- What themes emerge from the AI review analysis?
- How could Marketplace data improve your own forecasting models?

### 6.2 Teardown

When finished, clean up all resources:

```sql
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
- [Snowflake Workspaces](https://docs.snowflake.com/en/user-guide/ui-snowsight-worksheets-gs)
