"""Every hour: drain Kafka to files, load them to Snowflake, then run and check dbt."""

# datetime, timedelta and timezone let us set dates and lengths of time
from datetime import datetime, timedelta, timezone

# DAG is the object that describes the workflow
from airflow.sdk import DAG
# BashOperator runs a terminal command as a task
from airflow.providers.standard.operators.bash import BashOperator

# The Python that has our packages installed, separate from Airflow's own
PYTHON = "/opt/tfl-venv/bin/python"
# The dbt program in that same environment, pointed at our read-only project and its profile
DBT = "/opt/tfl-venv/bin/dbt"
# The options that tell dbt where the project and profiles.yml are
DBT_OPTIONS = "--project-dir /opt/tfl/dbt --profiles-dir /opt/tfl/dbt"

# The settings shared by every task
default_args = {
    # Try a failed task 2 more times, in case the problem was temporary
    "retries": 2,
    # Wait 2 minutes before each retry
    "retry_delay": timedelta(minutes=2),
# End of the shared settings
}

# Describe the workflow; everything inside this "with" block belongs to it
with DAG(
    # The name shown in the UI
    dag_id="tfl_line_status_pipeline",
    # What this DAG does
    description="Drain Kafka to files, load them to Snowflake, then run and check dbt",
    # Run at minute 0 of every hour, which saves Snowflake trial credits
    schedule="0 * * * *",
    # The first date this DAG can run from
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    # Do not run all the missed past schedules; the next run picks up all waiting data anyway
    catchup=False,
    # Only one run at a time, so two runs can't work on the same files
    max_active_runs=1,
    # Mark a whole run as failed if it takes longer than 45 minutes
    dagrun_timeout=timedelta(minutes=45),
    # Use the shared task settings above
    default_args=default_args,
    # Labels for finding this DAG in the UI
    tags=["tfl"],
# End of the DAG's settings
) as dag:
    # Task 1: read everything waiting in Kafka, write it to files, commit, then exit
    consume_line_status = BashOperator(
        # The name shown in the UI
        task_id="consume_line_status",
        # Run the consumer in drain mode
        bash_command=f"CONSUMER_RUN_ONCE=true {PYTHON} /opt/tfl/consumer/consumer.py",
        # Stop the task if it takes longer than 10 minutes
        execution_timeout=timedelta(minutes=10),
    # End of this task
    )

    # Task 2: upload the files to the stage and copy new ones into the raw table
    load_to_snowflake = BashOperator(
        # The name shown in the UI
        task_id="load_to_snowflake",
        # Run the loader
        bash_command=f"{PYTHON} /opt/tfl/snowflake/load_to_snowflake.py",
        # Stop the task if it takes longer than 10 minutes
        execution_timeout=timedelta(minutes=10),
    # End of this task
    )

    # Task 3: rebuild the dbt models
    dbt_run = BashOperator(
        # The name shown in the UI
        task_id="dbt_run",
        # Run dbt run
        bash_command=f"{DBT} run {DBT_OPTIONS}",
        # Stop the task if it takes longer than 10 minutes
        execution_timeout=timedelta(minutes=10),
    # End of this task
    )

    # Task 4: check the data in the models
    dbt_test = BashOperator(
        # The name shown in the UI
        task_id="dbt_test",
        # Run dbt test
        bash_command=f"{DBT} test {DBT_OPTIONS}",
        # Stop the task if it takes longer than 10 minutes
        execution_timeout=timedelta(minutes=10),
    # End of this task
    )

    # Task 5: check how recently data arrived in the raw table
    dbt_source_freshness = BashOperator(
        # The name shown in the UI
        task_id="dbt_source_freshness",
        # Run dbt source freshness
        bash_command=f"{DBT} source freshness {DBT_OPTIONS}",
        # Stop the task if it takes longer than 5 minutes
        execution_timeout=timedelta(minutes=5),
    # End of this task
    )

    # Set the order: each task starts only if the one before it succeeded
    consume_line_status >> load_to_snowflake >> dbt_run >> dbt_test >> dbt_source_freshness
