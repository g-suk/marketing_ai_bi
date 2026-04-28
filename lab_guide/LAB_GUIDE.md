# Summit Gear Co. -- Marketing AI+BI Lab Guide

**Duration:** 60 minutes | **Audience:** Mixed technical | **Difficulty:** Intermediate

---

## Prerequisites

- Snowflake account with ACCOUNTADMIN access
- Cortex Code enabled (for dashboard building)
- Web browser with Snowsight

> **Doing the lab at a live event?** Your facilitator has already pre-loaded the Snowflake Public Data listing and deployed the backup Streamlit app. In **0.4**, skip listing #1 (Snowflake Public Data) -- you still need to install the other three listings. You can also skip **0.1** (database/schema creation) and the **Backup App** section in Part 5. Start at **0.2** to create your workspace, then run the setup script at **0.3**.

---

## Part 0: Environment Setup (0-5 min)

### 0.1 Create the Database, Schema, and Git Integration

> **Live event attendees: This step has already been done for you. Do not execute this -- skip to 0.2.**

Open a SQL worksheet as ACCOUNTADMIN and run:

```sql
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE DATABASE MARKETING_AI_BI;
CREATE SCHEMA IF NOT EXISTS MARKETING_AI_BI.MARKETING_RAW;

CREATE OR REPLACE API INTEGRATION github_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/g-suk')
  ENABLED = TRUE;

CREATE ROLE IF NOT EXISTS MARKETING_LAB_ROLE;
GRANT ROLE MARKETING_LAB_ROLE TO ROLE SYSADMIN;

GRANT OWNERSHIP ON DATABASE MARKETING_AI_BI TO ROLE MARKETING_LAB_ROLE COPY CURRENT GRANTS;
GRANT OWNERSHIP ON SCHEMA MARKETING_AI_BI.MARKETING_RAW TO ROLE MARKETING_LAB_ROLE COPY CURRENT GRANTS;

CREATE SCHEMA IF NOT EXISTS MARKETING_AI_BI.MARKETING_ANALYTICS;
GRANT OWNERSHIP ON SCHEMA MARKETING_AI_BI.MARKETING_ANALYTICS TO ROLE MARKETING_LAB_ROLE COPY CURRENT GRANTS;

CREATE OR REPLACE WAREHOUSE COMPUTE_WH
  WAREHOUSE_SIZE = MEDIUM
  GENERATION = '2'
  AUTO_SUSPEND = 300;

GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE MARKETING_LAB_ROLE;

GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE MARKETING_LAB_ROLE;
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

1. **Snowflake Public Data** *(pre-loaded at live events)* -- [Install](https://app.snowflake.com/marketplace/listing/GZTSZ290BV255)
   - Installs as database: `SNOWFLAKE_PUBLIC_DATA_PAID`
2. **SMS CustomerConnect 360 Sample** -- [Install](https://app.snowflake.com/marketplace/listing/GZT0ZU1ICEX)
   - Installs as database: `CUSTOMERCONNECT360__SAMPLE`
3. **GWI Core** -- [Install](https://app.snowflake.com/marketplace/listing/GZ2FSZGU5YB)
   - Installs as database: `GWI_OPEN_DATA`
4. **Pelmorex Weather Source: Frostbyte** -- [Install](https://app.snowflake.com/marketplace/listing/GZSOZ1LLEL)
   - Installs as database: `FROSTBYTE_WEATHERSOURCE`

After installing, grant access. As ACCOUNTADMIN:

```sql
USE ROLE ACCOUNTADMIN;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE_PUBLIC_DATA_PAID TO ROLE MARKETING_LAB_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE CUSTOMERCONNECT360__SAMPLE TO ROLE MARKETING_LAB_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE GWI_OPEN_DATA              TO ROLE MARKETING_LAB_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE FROSTBYTE_WEATHERSOURCE    TO ROLE MARKETING_LAB_ROLE;
```

> **Why Snowflake Marketplace?** Marketplace lets you enrich your first-party data with live third-party datasets -- economic indicators, consumer demographics, weather -- without any ETL pipelines, file transfers, or API integrations. The data is shared in-place, always fresh, and query-ready in seconds. In this lab, four Marketplace datasets transform a basic marketing database into a rich analytics platform with weather correlations, consumer psychographics, and macroeconomic context. This is something that would take weeks of data engineering work on any other platform.

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

This creates 12 dynamic tables in the `MARKETING_ANALYTICS` schema that automatically transform raw data into analytics-ready tables.

> **Why Dynamic Tables?** Dynamic tables are Snowflake's declarative approach to data transformation. Instead of writing and scheduling ETL jobs, stored procedures, or dbt models, you write a single SQL query that defines what the table should look like -- and Snowflake handles the rest. Set a `TARGET_LAG` (e.g., `'1 day'`) and Snowflake automatically detects upstream changes, determines what needs refreshing, and incrementally updates only the changed data. There are no DAGs to manage, no orchestrators to configure, no failure alerts to wire up. In this lab, 12 dynamic tables cascade from raw sources through enrichment to analytics-ready aggregates -- and every table stays current automatically. If someone inserts new orders or the Marketplace weather data updates, the entire pipeline refreshes on its own. This is the equivalent of building and maintaining a complete ELT pipeline -- accomplished here with pure SQL `CREATE DYNAMIC TABLE` statements.

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

> **Lineage for free:** Because dynamic tables declare their dependencies in SQL, Snowflake automatically builds a visual lineage graph. You can see exactly which source tables feed which analytics tables, trace data flow end-to-end, and understand impact before making changes. This lineage is live and always accurate -- no separate catalog tool required.

---

## Part 2: Cortex ML Functions (15-25 min)

### 2.1 Forecasting

Open `sql/04_ml/01_forecast.sql` in your workspace and run it.

This trains two FORECAST models:
- **Total revenue** (single series)
- **Revenue by channel** (multi-series: DTC vs wholesale)

> **Note:** Model training takes 2-3 minutes. This is a good time to discuss how Snowflake ML works.

> **Why Cortex ML FORECAST?** Traditional forecasting requires a data science team: choosing algorithms, tuning hyperparameters, building training pipelines, deploying model artifacts, and scheduling retraining jobs. Snowflake's `SNOWFLAKE.ML.FORECAST` replaces all of that with a single SQL statement. You point it at a time series, specify a forecast horizon, and Snowflake automatically selects the best algorithm, handles seasonality detection, and produces forecasts with confidence intervals. Multi-series forecasting (e.g., revenue by channel) works the same way -- one call handles all series in parallel. The model is a first-class Snowflake object: it lives in your schema, respects RBAC, and can be called by anyone with the right privileges. No Python, no notebooks, no MLOps infrastructure. In this lab, we train three forecast models with a few lines of SQL that would typically require a data scientist, a model registry, and a deployment pipeline.

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

> **Why Cortex ML ANOMALY_DETECTION?** The same simplicity applies to anomaly detection. One SQL statement trains a model that learns normal patterns and flags statistical outliers with percentile scores. No threshold tuning, no rolling-window logic, no custom alerting code. The model automatically adapts to the time series characteristics -- seasonality, trend, volatility. In this lab, it correctly identifies all three seeded anomalies across different metrics and time periods, including subtle pattern breaks that simple threshold rules would miss.

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

> **Why Cortex AI Functions?** These are LLM-powered SQL functions that bring the power of large language models directly into your data pipeline -- no API keys, no external services, no data leaving Snowflake. Call `SNOWFLAKE.CORTEX.SENTIMENT()` on a text column and every row gets a sentiment score. Call `CLASSIFY_TEXT()` with a list of categories and the LLM classifies each row. Call `COMPLETE()` with a prompt template and it generates custom text per row. This is transformative for analytics teams: work that previously required NLP engineers, custom model training, or expensive third-party APIs is now a single function call in a SQL `SELECT` statement. The LLMs run inside Snowflake's infrastructure, so your data never leaves the security boundary, and everything is governed by the same RBAC policies as your tables. In this lab, five different AI capabilities are applied across hundreds of rows to produce sentiment analysis, campaign classification, structured data extraction from freeform text, per-campaign executive summaries, and theme aggregation -- all in standard SQL.

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

## Part 4: Semantic Views + Cortex Agent (35-40 min)

### 4.1 Create the Semantic Views

Open `sql/06_semantic/01_semantic_view.sql` in your workspace and run it.

This creates four semantic views that map business concepts (metrics, dimensions, relationships) over the raw and analytics tables.

> **Why Semantic Views?** Semantic views are Snowflake's "define once, use everywhere" abstraction layer. You define business concepts -- what "total revenue" means, how campaigns relate to orders, which columns are dimensions vs. metrics -- in a single semantic view. Once defined, that knowledge is available everywhere: in SQL queries via `SEMANTIC_VIEW()`, in Cortex Agents for natural language Q&A, in dashboards, and in any future application. Without semantic views, every consumer of your data (analysts, dashboards, agents, ad-hoc queries) needs to independently know which tables to join, which columns to aggregate, and what the business rules are. Semantic views centralize that logic. Change a metric definition once and it propagates to every consumer automatically. In this lab, four semantic views define the entire business model for Summit Gear Co. -- campaigns, orders, ML results, reviews, marketing mix, geo-targeting, and CLV -- and that same definition powers both the Streamlit dashboard and the Cortex Agent. This is the foundation of a governed, self-service analytics layer.

### 4.2 Create the Cortex Agent

Open `sql/06_semantic/02_cortex_agent.sql` in your workspace and run it.

> **Why Cortex Agent?** A Cortex Agent is a natural language interface to your data that would typically require building a custom application: a prompt engineering layer, a text-to-SQL engine, result formatting, error handling, and a chat UI. Snowflake replaces all of that with a single `CREATE AGENT` statement. The agent is backed by semantic views, so it inherits all your business logic, metric definitions, and table relationships. It automatically routes questions to the right tool (MarketingAnalyst, MLAnalyst, ReviewAnalyst, AdvancedAnalyst), generates SQL, executes it, and returns formatted results. Compare this to the alternative: building a RAG pipeline, fine-tuning a model, writing a text-to-SQL layer, and deploying an API -- that's months of work. Here it's a 30-line SQL statement that produces a production-grade conversational analytics experience. The agent respects Snowflake RBAC, runs on your warehouse, and never sends data outside your account. It can be embedded in Streamlit, called from SQL, or accessed via REST API -- the same agent works everywhere.

Try the agent from a SQL worksheet:
```sql
SELECT SNOWFLAKE.CORTEX.AGENT(
    'SUMMIT_GEAR_AGENT',
    'Which DTC sub-channel has the best ROAS?'
);
```

Try a few more questions to see how the agent routes to different tools:
```sql
-- Routes to MLAnalyst
SELECT SNOWFLAKE.CORTEX.AGENT('SUMMIT_GEAR_AGENT', 'What anomalies were detected in ad spend?');

-- Routes to ReviewAnalyst
SELECT SNOWFLAKE.CORTEX.AGENT('SUMMIT_GEAR_AGENT', 'What are the main themes in customer reviews?');

-- Routes to AdvancedAnalyst
SELECT SNOWFLAKE.CORTEX.AGENT('SUMMIT_GEAR_AGENT', 'Which marketing channels have the best ROI in the mix model?');
```

---

## Part 5: Build Your Dashboard (40-55 min)

This is the creative section! You'll use **Cortex Code** to build a Streamlit dashboard in a single prompt.

> **Why Streamlit in Snowflake?** Streamlit in Snowflake (SiS) lets you build and deploy interactive data applications without leaving the Snowflake platform -- no separate hosting, no authentication setup, no data movement. The app runs inside Snowflake's security boundary with a pre-authenticated session, governed by the same roles and privileges as your SQL queries. Combined with everything built in this lab, a single prompt to Cortex Code produces a 5-page dashboard that queries semantic views, displays ML forecasts and anomalies, renders AI-generated insights, and embeds a conversational agent -- all running natively in Snowflake. This is the full stack: data ingestion (Marketplace), transformation (Dynamic Tables), ML (Forecast/Anomaly), AI (Cortex Functions), semantic layer (Semantic Views), conversational AI (Cortex Agent), and application (Streamlit) -- entirely within one platform, governed by one set of policies, and built with SQL and a single prompt.

### Upload the Streamlit Helper Skill

Before prompting Cortex Code, upload the helper skill so it knows Streamlit-in-Snowflake constraints, Cortex Agent parsing patterns, and Altair charting conventions.

1. Open **Cortex Code** in your Snowflake account
2. Click the **Skills** icon (puzzle piece) in the left sidebar
3. Click **+ Add Skill**
4. Navigate to the `helper_streamlit_skill` folder in your workspace and select `SKILL.md`
5. The skill is now loaded for this session -- Cortex Code will reference it automatically when building Streamlit apps

> **What is a skill?** A skill is a markdown file you upload to Cortex Code that gives it specialized knowledge for a particular task. Think of it as a "cheat sheet" -- it teaches Cortex Code domain-specific rules, code patterns, API conventions, and guardrails that it wouldn't otherwise know. Without the skill, Cortex Code might use plotly (which doesn't work in SiS), miss the Cortex Agent response parser format, or forget that Snowflake returns uppercase column names. With the skill loaded, it follows all of these constraints automatically. You can create skills for any domain -- dbt conventions, internal API patterns, team coding standards -- and share them across your organization.

### Getting Started

1. Start a new conversation in Cortex Code
2. Paste the prompt from `prompts/01_build_dashboard.md`

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

CREATE STREAMLIT MARKETING_AI_BI.MARKETING_ANALYTICS.SUMMIT_GEAR_DASHBOARD_BACKUP
  ROOT_LOCATION = '@MARKETING_AI_BI.MARKETING_ANALYTICS.STREAMLIT_STAGE/streamlit'
  MAIN_FILE = 'streamlit_app.py'
  QUERY_WAREHOUSE = 'COMPUTE_WH';
```

---

## Part 6: Wrap-up (55-60 min)

### What We Built

Take a step back and look at what this lab accomplished with native Snowflake features:

| Layer | Feature | What It Replaced |
|-------|---------|-----------------|
| **Data Enrichment** | Marketplace | API integrations, ETL pipelines, data vendor contracts |
| **Transformation** | Dynamic Tables | Scheduled stored procedures, dbt, Airflow DAGs |
| **Machine Learning** | Cortex ML (Forecast, Anomaly Detection) | Data science teams, Python notebooks, MLOps pipelines |
| **AI / NLP** | Cortex AI Functions (Sentiment, Classify, Extract, Complete, Summarize) | NLP engineers, external LLM APIs, custom model training |
| **Semantic Layer** | Semantic Views | Looker LookML, dbt metrics, custom metadata layers |
| **Conversational AI** | Cortex Agent | Custom RAG pipelines, text-to-SQL engines, chatbot frameworks |
| **Application** | Streamlit in Snowflake | Separate web hosting, auth systems, data connectors |

Every layer is governed by the same RBAC policies, runs on the same compute, and lives in the same platform. No data ever leaves Snowflake.

### 6.1 Discussion Prompts

- How do DTC and wholesale channels compare on key metrics?
- What anomalies did the ML models detect? Do they match the known events?
- What themes emerge from the AI review analysis?
- How could Marketplace data improve your own forecasting models?
- How would you extend this pattern to your own data? What semantic views would you define?

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
- [Semantic Views](https://docs.snowflake.com/en/user-guide/views-semantic)
- [Cortex Agent](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agent)
- [Snowflake Marketplace](https://app.snowflake.com/marketplace)
- [Streamlit in Snowflake](https://docs.snowflake.com/en/developer-guide/streamlit/about-streamlit)
- [Snowflake Workspaces](https://docs.snowflake.com/en/user-guide/ui-snowsight-worksheets-gs)
