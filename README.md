# tfl-realtime-streaming

A real-time data pipeline built on live Transport for London (TfL) data. It collects tube line statuses and bike point availability from the TfL API, streams them through Kafka into Snowflake, transforms them with dbt, schedules the work with Airflow, and shows the results on a Streamlit dashboard.

## Architecture

```text
TfL API -> Kafka -> Snowflake -> dbt -> Airflow -> Streamlit
```

**Status: in progress**
