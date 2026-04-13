# Prompt 2: Channel Deep Dive

Paste this into Cortex Code:

---

Add a second page called "Channel Deep Dive" to my Streamlit app. This page should have two sections:

**DTC Section:**
- Scatter plot of spend vs conversions by DTC sub-channel (email, social, search, influencer), sized by ROAS
- A table showing campaign-level metrics: campaign name, spend, conversions, CPA, ROAS -- sortable and filterable

**Wholesale Section:**
- Bar chart of sell-through rate by wholesale partner (top 15)
- Comparison of trade promo campaigns: spend vs incremental wholesale revenue

Use the MARKETING_AI_BI.MARKETING_ANALYTICS.DT_CAMPAIGN_METRICS and MARKETING_AI_BI.MARKETING_ANALYTICS.DT_PARTNER_PERFORMANCE dynamic tables.
