# Summit Gear Co. -- Marketing AI+BI Lab

A 60-minute hands-on lab showcasing Snowflake's AI, ML, Marketplace, Dynamic Tables, and Streamlit capabilities through a marketing analytics scenario.

## Quick Start

### Step 1: Create the Database (required before Git integration)

```sql
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE DATABASE MARKETING_AI_BI;
CREATE SCHEMA IF NOT EXISTS MARKETING_AI_BI.MARKETING_RAW;
```

### Step 2: Connect via Git Integration (Recommended)

```sql
CREATE OR REPLACE API INTEGRATION github_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/g-suk')
  ENABLED = TRUE;

CREATE OR REPLACE GIT REPOSITORY MARKETING_AI_BI.MARKETING_RAW.MARKETING_LAB_REPO
  API_INTEGRATION = github_api_integration
  ORIGIN = 'https://github.com/g-suk/marketing_ai_bi.git';
```

### Step 3: Fetch and Run Scripts

```sql
ALTER GIT REPOSITORY MARKETING_AI_BI.MARKETING_RAW.MARKETING_LAB_REPO FETCH;
```

Then run each step individually from the repo (see lab guide for order):

```sql
EXECUTE IMMEDIATE FROM @MARKETING_AI_BI.MARKETING_RAW.MARKETING_LAB_REPO/branches/main/sql/01_setup/setup.sql;
EXECUTE IMMEDIATE FROM @MARKETING_AI_BI.MARKETING_RAW.MARKETING_LAB_REPO/branches/main/sql/02_data/01_synthetic_data.sql;
EXECUTE IMMEDIATE FROM @MARKETING_AI_BI.MARKETING_RAW.MARKETING_LAB_REPO/branches/main/sql/03_dynamic_tables/01_dynamic_tables.sql;
EXECUTE IMMEDIATE FROM @MARKETING_AI_BI.MARKETING_RAW.MARKETING_LAB_REPO/branches/main/sql/04_ml/01_forecast.sql;
EXECUTE IMMEDIATE FROM @MARKETING_AI_BI.MARKETING_RAW.MARKETING_LAB_REPO/branches/main/sql/04_ml/02_anomaly_detection.sql;
EXECUTE IMMEDIATE FROM @MARKETING_AI_BI.MARKETING_RAW.MARKETING_LAB_REPO/branches/main/sql/05_ai/01_ai_functions.sql;
EXECUTE IMMEDIATE FROM @MARKETING_AI_BI.MARKETING_RAW.MARKETING_LAB_REPO/branches/main/sql/06_semantic/01_semantic_view.sql;
EXECUTE IMMEDIATE FROM @MARKETING_AI_BI.MARKETING_RAW.MARKETING_LAB_REPO/branches/main/sql/06_semantic/02_cortex_agent.sql;
```

### Alternative: Copy/Paste

Open a Snowsight SQL worksheet and run each SQL file in order from the `sql/` folder.

## What You'll Build

- **Synthetic dataset:** 6 tables in `MARKETING_RAW` for "Summit Gear Co." (DTC + wholesale outdoor brand)
- **Marketplace enrichment:** Economic indicators, consumer demographics, weather data
- **Dynamic tables:** 6 declarative transformation layers in `MARKETING_ANALYTICS`
- **Cortex ML:** Revenue forecasting + anomaly detection
- **Cortex AI:** Sentiment analysis, classification, extraction, summarization
- **Semantic view + Agent:** Natural language Q&A over marketing data
- **Streamlit dashboard:** Built interactively using Cortex Code prompts

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
| `MARKETING_RAW` | Source tables, ML results, AI results, marketplace views |
| `MARKETING_ANALYTICS` | Dynamic tables, semantic view, Cortex Agent |

## Teardown

```sql
USE ROLE ACCOUNTADMIN;
DROP DATABASE IF EXISTS MARKETING_AI_BI;
DROP ROLE IF EXISTS MARKETING_LAB_ROLE;
```
