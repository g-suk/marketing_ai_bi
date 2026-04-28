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

```mermaid
flowchart TB
    subgraph marketplace [Snowflake Marketplace]
        EconIndicators[Economic Indicators]
        Customer360[Customer 360]
        ConsumerAttitudes[Consumer Attitudes]
        WeatherSource[Weather Source]
    end

    subgraph raw [MARKETING_RAW - Source Tables]
        WP[WHOLESALE_PARTNERS<br/>30 rows]
        CAMP[CAMPAIGNS<br/>70 rows]
        CUST[CUSTOMERS<br/>8K rows]
        ORD[ORDERS<br/>29K rows]
        SPEND[MARKETING_SPEND<br/>4.5K rows]
        REV[PRODUCT_REVIEWS<br/>1.5K rows]
    end

    subgraph analytics [MARKETING_ANALYTICS - Dynamic Tables and Derived Tables]
        direction TB
        subgraph coreDT [Core Dynamic Tables]
            DT1[DT_DAILY_REVENUE]
            DT2[DT_CAMPAIGN_METRICS]
            DT3[DT_PARTNER_PERFORMANCE]
            DT4[DT_CUSTOMER_ENRICHED]
            DT5[DT_FORECAST_INPUT]
            DT6[DT_SPEND_DAILY]
            DT7[DT_SPEND_FORECAST_INPUT]
            DT8[DT_PRODUCT_REVENUE]
        end
        subgraph mktDT [Marketplace-Fed Dynamic Tables]
            DT9[DT_WEATHER_REVENUE]
            DT10[DT_GEO_TARGETING]
            DT11[DT_MMM_DAILY]
        end
        subgraph mtaDT [MTA Dynamic Tables]
            DT12[DT_MTA_TOUCHPOINTS]
            DT13[DT_MTA_JOURNEY_SUMMARY]
        end
        subgraph derived [Materialized and Derived Tables]
            GEO1[GEO_TARGETING_PROFILES]
            GEO2[GEO_WEATHER_TRIGGERS]
            GEO3[GEO_AI_RECOMMENDATIONS]
            MMM1[MMM_CHANNEL_CONTRIBUTIONS]
            MMM2[MMM_WEEKLY_DECOMPOSITION]
            MMM3[MMM_AI_INSIGHTS]
            CLV1[CLV_RISK_CLASSIFICATION]
        end
        subgraph attrViews [Attribution Views]
            V1[V_FIRST_TOUCH]
            V2[V_LAST_TOUCH]
            V3[V_LINEAR]
            V4[V_TIME_DECAY]
            V5[V_POSITION_BASED]
            V6[V_CHANNEL_ATTRIBUTION_SUMMARY]
        end
    end

    subgraph ml [Snowflake ML Models]
        FC[FORECAST - 3 models]
        AD[ANOMALY_DETECTION - 3 models]
    end

    subgraph ai [Cortex AI Functions]
        SENT[SENTIMENT]
        CLASS[CLASSIFY_TEXT]
        EXTRACT[EXTRACT_ANSWER]
        COMPLETE[COMPLETE]
        SUMMARIZE[SUMMARIZE_AGG]
    end

    subgraph semantic [Semantic Views - Cortex Analyst]
        SV1["SV_SUMMIT_GEAR_MARKETING<br/>10 entities"]
        SV2["SV_SUMMIT_GEAR_ML<br/>3 entities"]
        SV3["SV_SUMMIT_GEAR_REVIEWS<br/>3 entities"]
        SV4["SV_SUMMIT_GEAR_ADVANCED<br/>11 entities"]
    end

    subgraph agent [Cortex Agent - SUMMIT_GEAR_AGENT]
        T1["MarketingAnalyst<br/>SV_MARKETING"]
        T2["MLAnalyst<br/>SV_ML"]
        T3["ReviewAnalyst<br/>SV_REVIEWS"]
        T4["AdvancedAnalyst<br/>SV_ADVANCED"]
    end

    subgraph dashboard [Streamlit-in-Snowflake Dashboard]
        P1[KPI Overview]
        P2["Advanced Analytics<br/>(MMM | MTA | Geo | CLV)"]
        P3[Forecasting and Anomalies]
        P4[AI Insights]
        P5[Marketing Agent]
    end

    marketplace --> raw
    raw --> coreDT
    raw --> mtaDT
    raw --> mtaDT
    marketplace --> mktDT
    coreDT --> ml
    coreDT --> mktDT
    raw --> mtaDT
    DT12 --> DT13
    DT12 --> attrViews
    DT10 --> GEO1
    DT10 --> GEO2
    ai --> GEO3
    ai --> MMM3
    coreDT --> derived
    ml --> SV2
    ai --> SV3
    raw --> SV1
    analytics --> SV4
    SV1 --> T1
    SV2 --> T2
    SV3 --> T3
    SV4 --> T4
    semantic --> dashboard
    agent --> P5
```

## What You'll Build

- **Synthetic dataset:** 6 tables in `MARKETING_RAW` for "Summit Gear Co." (DTC + wholesale outdoor brand)
- **Marketplace enrichment:** Economic indicators, consumer demographics, weather data, consumer attitudes
- **Dynamic tables:** 13 declarative transformation layers in `MARKETING_ANALYTICS` plus 7 materialized derived tables
- **Multi-Touch Attribution:** 5 rule-based attribution models (first/last touch, linear, time-decay, position-based) with journey analysis across 7 sub-channels
- **Marketing Mix Modeling:** Channel attribution, weekly spend decomposition, AI-generated budget recommendations, ROAS targets with reference lines
- **Location-Based Targeting:** Composite Market Opportunity Index geo-scoring, weather-triggered campaigns, per-state AI briefs
- **CLV Risk Classification:** 5-tier customer segmentation with churn risk scoring
- **Cortex ML:** Revenue forecasting (with exogenous weather/economic features) + anomaly detection across 6 models
- **Cortex AI:** Sentiment analysis, classification, extraction, summarization, MMM insights, geo-targeting recommendations
- **Semantic views + Agent:** 4 semantic views (27 entities total) backing a natural language Q&A agent with 4 specialized tools
- **Streamlit dashboard:** 5-page app with KPI Overview, Advanced Analytics (4 tabs: MMM, MTA, Geo-Targeting, CLV & Churn), Forecasting & Anomalies, AI Insights, and Marketing Agent

## Marketplace Listings

Install these free listings from **Data Products > Marketplace** in Snowsight:

| Listing | Database Name |
|---------|--------------|
| [Snowflake Public Data](https://app.snowflake.com/marketplace/listing/GZTSZ290BV255) | `SNOWFLAKE_PUBLIC_DATA_PAID` |
| [SMS CustomerConnect 360 Sample](https://app.snowflake.com/marketplace/listing/GZT0ZU1ICEX) | `CUSTOMERCONNECT360__SAMPLE` |
| [GWI Core](https://app.snowflake.com/marketplace/listing/GZ2FSZGU5YB) | `GWI_OPEN_DATA` |
| [Weather Source Global Weather](https://app.snowflake.com/marketplace/listing/GZSOZ1LLD8) | `FROSTBYTE_WEATHERSOURCE` |

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
  01_setup/setup.sql                  -- RBAC, role, schemas, stages, 6 source tables
  02_data/02_marketplace_enrichment.sql -- Marketplace snapshot tables
  03_dynamic_tables/01_dynamic_tables.sql -- 13 dynamic tables, 6 derived tables, 6 attribution views
  04_ml/01_forecast.sql               -- FORECAST (3 models: total revenue, product/channel, spend)
  04_ml/02_anomaly_detection.sql      -- ANOMALY_DETECTION (3 models: spend, DTC conversion, wholesale)
  05_ai/01_ai_functions.sql           -- Cortex AI functions (sentiment, classify, extract, summarize, complete)
  06_semantic/01_semantic_view.sql     -- 4 semantic views (marketing, ML, reviews, advanced)
  06_semantic/02_cortex_agent.sql      -- Cortex Agent with 4 text-to-SQL tools
lab_guide/
  LAB_GUIDE.md                        -- Step-by-step instructions
  prompts/01_build_dashboard.md       -- Cortex Code prompt for building the dashboard
  prompts/02_enhancements.md          -- Follow-up enhancement ideas
streamlit/
  streamlit_app.py                    -- Dashboard source code
  environment.yml                     -- SiS conda dependencies
helper_streamlit_skill/SKILL.md       -- SiS runtime constraints and Cortex Agent parser reference
teardown_all.sql                      -- Clean up everything
```

## Schemas

| Schema | Purpose |
|--------|---------|
| `MARKETING_RAW` | Source tables, ML results, AI results, marketplace views, marketplace snapshots |
| `MARKETING_ANALYTICS` | Dynamic tables, MMM tables, MTA touchpoints & attribution views, geo-targeting, CLV risk, semantic views, Cortex Agent, Streamlit app |

## Teardown

```sql
USE ROLE ACCOUNTADMIN;
DROP DATABASE IF EXISTS MARKETING_AI_BI;
DROP ROLE IF EXISTS MARKETING_LAB_ROLE;
```
