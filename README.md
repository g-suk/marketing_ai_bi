# Summit Gear Co. -- Marketing AI+BI Lab

A 60-minute hands-on lab showcasing Snowflake's AI, ML, Marketplace, Dynamic Tables, and Streamlit capabilities through a marketing analytics scenario.

## Quick Start

### Option A: Run from Snowflake Workspaces (Recommended)

1. **Create a Git Integration** in your Snowflake account:

```sql
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE API INTEGRATION github_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/g-suk')
  ENABLED = TRUE;

CREATE OR REPLACE GIT REPOSITORY MARKETING_AI_BI.DEMO_DATA.MARKETING_LAB_REPO
  API_INTEGRATION = github_api_integration
  ORIGIN = 'https://github.com/g-suk/marketing-data-ai-bi.git';
```

2. **Open a Snowsight SQL Worksheet** and run:

```sql
ALTER GIT REPOSITORY MARKETING_AI_BI.DEMO_DATA.MARKETING_LAB_REPO FETCH;

EXECUTE IMMEDIATE FROM @MARKETING_AI_BI.DEMO_DATA.MARKETING_LAB_REPO/branches/main/deploy_all.sql;
```

3. Install Marketplace listings (see lab guide below)
4. Follow `lab_guide/LAB_GUIDE.md`

### Option B: Copy/Paste

1. Open a Snowsight SQL worksheet
2. Copy the contents of `deploy_all.sql` and execute as ACCOUNTADMIN
3. Install Marketplace listings (see lab guide)
4. Follow `lab_guide/LAB_GUIDE.md`

## What You'll Build

- **Synthetic dataset:** 6 tables for "Summit Gear Co." (DTC + wholesale outdoor brand)
- **Marketplace enrichment:** Economic indicators, consumer demographics, weather data
- **Dynamic tables:** 6 declarative transformation layers
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
  01_setup/            -- RBAC, database, schema
  02_data/             -- Synthetic data + marketplace enrichment
  03_dynamic_tables/   -- 6 dynamic tables
  04_ml/               -- FORECAST + ANOMALY_DETECTION
  05_ai/               -- AI functions
  06_semantic/         -- Semantic view + Cortex Agent
lab_guide/             -- Step-by-step instructions + Cortex Code prompts
streamlit/             -- Backup reference app
```

## Teardown

```sql
USE ROLE ACCOUNTADMIN;
DROP DATABASE IF EXISTS MARKETING_AI_BI;
DROP ROLE IF EXISTS MARKETING_LAB_ROLE;
```
