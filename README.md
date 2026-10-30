# tfl-realtime-streaming

A real-time data pipeline built on live Transport for London (TfL) data. It collects tube line statuses and bike point availability from the TfL API, streams them through Kafka into Snowflake, transforms them with dbt, schedules the work with Airflow, and shows the results on a Streamlit dashboard.

## Architecture

```text
TfL API -> Kafka -> Snowflake -> dbt -> Airflow -> Streamlit
```

**Status: in progress**

## Running locally

You need Docker Desktop running. Run these commands in PowerShell from the project's main folder.

**Start Kafka and the Kafka UI:**

```powershell
docker compose -f docker/docker-compose.yml up -d
```

**Open the UI:** go to <http://localhost:8080>. Kafka has to pass its healthcheck first, so the UI can take up to a minute to appear.

**Create the topics** (safe to run more than once):

```powershell
powershell -ExecutionPolicy Bypass -File docker/create_topics.ps1
```

**Stop everything** (your data is kept):

```powershell
docker compose -f docker/docker-compose.yml down
```

Programs on your laptop connect to Kafka at `localhost:9092`. Programs in other containers use `kafka:29092`.
