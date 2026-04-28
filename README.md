# Summit Gear Co. -- Marketing AI+BI Lab

A 60-minute hands-on lab showcasing Snowflake's AI, ML, Marketplace, Dynamic Tables, and Streamlit capabilities through a marketing analytics scenario.

## Quick Start

> **Doing the lab at a live event?** Snowflake Public Data has already been pre-loaded and the backup Streamlit app is deployed for you. You still need to install the other three Marketplace listings (SMS CustomerConnect 360, GWI Core, Weather Source). Your facilitator will provide the account details.

For full setup instructions (database creation, Git workspace, Marketplace listings, and grants), see [**Lab Guide -- Part 0: Environment Setup**](lab_guide/LAB_GUIDE.md#part-0-environment-setup-0-5-min).

Once your environment is ready, open `lab_guide/LAB_GUIDE.md` and work through each part. You'll open each SQL file in the workspace, review what it does, and run it step by step.

## Architecture

![Architecture Diagram](architecture_diagram.png)

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
