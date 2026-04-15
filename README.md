# Summit Gear Co. -- Marketing AI+BI Lab

A 60-minute hands-on lab showcasing Snowflake's AI, ML, Marketplace, Dynamic Tables, and Streamlit capabilities through a marketing analytics scenario.

## Quick Start

### Step 1: Create the Database and Git Integration

As ACCOUNTADMIN, open a SQL worksheet and run:

```sql
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE DATABASE MARKETING_AI_BI;
CREATE SCHEMA IF NOT EXISTS MARKETING_AI_BI.MARKETING_RAW;

CREATE OR REPLACE API INTEGRATION github_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/g-suk')
  ENABLED = TRUE;
```

### Step 2: Create a Workspace from Git

1. In Snowsight, navigate to **Projects > Workspaces**
2. Click **+ Create Workspace** (top right)
3. Select **Create Workspace from Git Repository**
4. Paste the repository URL: `https://github.com/g-suk/marketing_ai_bi.git`
5. Select **Public Repository** (no credentials needed)
6. Choose database `MARKETING_AI_BI` and schema `MARKETING_RAW`
7. Click **Create**

You now have a full workspace with all lab SQL files ready to open and run.

### Step 3: Follow the Lab Guide

Open `lab_guide/LAB_GUIDE.md` and work through each part. You'll open each SQL file in the workspace, review what it does, and run it step by step.

## Architecture

![Architecture Diagram](architecture_diagram.png)

## What You'll Build

- **Synthetic dataset:** 6 tables in `MARKETING_RAW` for "Summit Gear Co." (DTC + wholesale outdoor brand)
- **Marketplace enrichment:** Economic indicators, consumer demographics, weather data, consumer attitudes
- **Dynamic tables:** 12 declarative transformation layers in `MARKETING_ANALYTICS`
- **Cortex ML:** Revenue forecasting (with exogenous weather/economic features) + anomaly detection
- **Cortex AI:** Sentiment analysis, classification, extraction, summarization, MMM insights, geo-targeting recommendations
- **Marketing Mix Modeling:** Channel attribution, weekly spend decomposition, AI-generated budget recommendations
- **Location-Based Targeting:** Composite geo-scoring, weather-triggered campaigns, per-state AI briefs
- **CLV Risk Classification:** 5-tier customer segmentation with churn risk scoring
- **Semantic views + Agent:** 4 semantic views backing a natural language Q&A agent
- **Streamlit dashboard:** 5-page app with KPI Overview, Advanced Analytics, Forecasting, AI Insights, and Marketing Agent

## Marketplace Listings

Install these free listings from **Data Products > Marketplace** in Snowsight:

| Listing | Database Name | Required? |
|---------|--------------|-----------|
| [Snowflake Public Data](https://app.snowflake.com/marketplace/listing/GZTSZ290BV255) | `SNOWFLAKE_PUBLIC_DATA_PAID` | Yes |
| [SMS CustomerConnect 360 Sample](https://app.snowflake.com/marketplace/listing/GZT0ZU1ICEX) | `CUSTOMERCONNECT360__SAMPLE` | Yes |
| [GWI Core](https://app.snowflake.com/marketplace/listing/GZ2FSZGU5YB) | `GWI_OPEN_DATA` | Bonus |
| [Weather Source Global Weather](https://app.snowflake.com/marketplace/listing/GZSOZ1LLD8) | `FROSTBYTE_WEATHERSOURCE` | Bonus |

After installing, grant access:

```sql
USE ROLE ACCOUNTADMIN;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE_PUBLIC_DATA_PAID TO ROLE MARKETING_LAB_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE CUSTOMERCONNECT360__SAMPLE TO ROLE MARKETING_LAB_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE GWI_OPEN_DATA              TO ROLE MARKETING_LAB_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE FROSTBYTE_WEATHERSOURCE    TO ROLE MARKETING_LAB_ROLE;
```

## File Structure

```
sql/
  01_setup/setup.sql             -- RBAC, role, schemas, stages
  02_data/                       -- Synthetic data + marketplace enrichment
  03_dynamic_tables/             -- 6 dynamic tables (MARKETING_ANALYTICS)
  04_ml/                         -- FORECAST + ANOMALY_DETECTION
  05_ai/                         -- AI functions
  06_semantic/                   -- Semantic view + Cortex Agent (MARKETING_ANALYTICS)
lab_guide/                       -- Step-by-step instructions + Cortex Code prompts
streamlit/                       -- Backup reference app
teardown_all.sql                 -- Clean up everything
```

## Schemas

| Schema | Purpose |
|--------|---------|
| `MARKETING_RAW` | Source tables, ML results, AI results, marketplace views, marketplace snapshots |
| `MARKETING_ANALYTICS` | Dynamic tables, analytics tables (MMM, geo, CLV), semantic views, Cortex Agent |

## Teardown

```sql
USE ROLE ACCOUNTADMIN;
DROP DATABASE IF EXISTS MARKETING_AI_BI;
DROP ROLE IF EXISTS MARKETING_LAB_ROLE;
```
