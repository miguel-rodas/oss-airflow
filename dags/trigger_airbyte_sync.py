from airflow.decorators import dag, task
from pendulum import datetime
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor
from airflow.operators.empty import EmptyOperator
from datetime import timedelta
import os

AIRBYTE_CONN_ID = "airbyte_conn"  # Airflow connection ID to Airbyte
CONNECTION_ID = "945e32f7-b483-4436-b195-371e120f28e8" # Airbyte connection ID

default_args = {
    "owner": "Miguel Rodas",
    "depends_on_past": False,
    'email_on_failure': False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

# Define the DAG with decorator
@dag(
    description="Trigger Airbyte Sync DAG",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    dagrun_timeout=timedelta(minutes=15),
    catchup=False,
    max_active_runs=1,
    tags=["airbyte", "demo"],
)

def trigger_airbyte_sync():

    start = EmptyOperator(task_id="start")

    trigger_sync = AirbyteTriggerSyncOperator(
        task_id="trigger_airbyte_sync",
        airbyte_conn_id=AIRBYTE_CONN_ID,
        connection_id=CONNECTION_ID,
        asynchronous=True,
        timeout=3600
    )

    wait_for_sync = AirbyteJobSensor(
        task_id="wait_for_airbyte_sync",
        airbyte_conn_id=AIRBYTE_CONN_ID,
        airbyte_job_id=trigger_sync.output
    )

    end = EmptyOperator(task_id="end")

    start >> trigger_sync >> wait_for_sync >> end

# Instantiate the DAG
dag = trigger_airbyte_sync()
