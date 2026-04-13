# Prompt 5: Cortex Agent Integration

Paste this into Cortex Code:

---

Add a chat interface to my Streamlit app (either on the AI Insights page or as its own tab) that lets users ask natural language questions about the marketing data.

Use Snowflake's Cortex Agent API. The agent is named SUMMIT_GEAR_AGENT in MARKETING_AI_BI.DEMO_DATA and is backed by a semantic view called SV_SUMMIT_GEAR_MARKETING that covers the CAMPAIGNS, ORDERS, CUSTOMERS, MARKETING_SPEND, WHOLESALE_PARTNERS, DT_CAMPAIGN_METRICS, DT_PARTNER_PERFORMANCE, and AI_SENTIMENT_RESULTS tables.

Show a few suggested starter questions as clickable chips:
- "Why did DTC sales drop in March?"
- "Which wholesale partner has the best sell-through rate?"
- "How does weather affect winter gear sales?"
- "Compare email vs social campaign ROI"
- "What are the top complaints in product reviews?"

Display the agent's SQL query in an expander below the answer so participants can learn from the generated SQL.
