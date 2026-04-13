# Prompt 3: Forecasting & Anomalies

Paste this into Cortex Code:

---

Add a third page called "Forecasting & Anomalies" to my Streamlit app.

**Forecast section:**
- Query the forecast results from the table FORECAST_RESULTS (columns: series, ts, forecast, lower_bound, upper_bound). Plot the historical actuals from DT_DAILY_REVENUE alongside the forecast with a shaded confidence interval band.
- Add a selectbox to toggle between forecast series: "Total", "DTC", "Wholesale".
- Show the feature importance results from FORECAST_FEATURE_IMPORTANCE as a horizontal bar chart.

**Anomaly section:**
- Query ANOMALY_DETECTION_RESULTS (columns: series, ts, y, forecast, lower_bound, upper_bound, is_anomaly, percentile). Plot the time series and highlight anomaly points in red.
- Add a selectbox to toggle between "Ad Spend", "DTC Conversion Rate", "Wholesale Orders".
- When a user clicks or hovers on an anomaly point, show the date and magnitude of the deviation.

Use tables in MARKETING_AI_BI.DEMO_DATA.
