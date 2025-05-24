from airflow.decorators import dag, task
from pendulum import datetime
from airflow.operators.empty import EmptyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.sensors.python import PythonSensor
from datetime import timedelta
import requests
import time

AIRBYTE_CONN_ID = "airbyte_http_conn"  # Define this in Airflow as an HTTP connection
CONNECTION_ID = "945e32f7-b483-4436-b195-371e120f28e8"

default_args = {
    "owner": "Miguel Rodas",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

@dag(
    description="Trigger Airbyte Sync DAG via HTTP API",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    dagrun_timeout=timedelta(minutes=15),
    catchup=False,
    max_active_runs=1,
    tags=["airbyte", "http-api"],
)
def trigger_airbyte_sync_http():

    start = EmptyOperator(task_id="start")

    trigger_sync = SimpleHttpOperator(
        task_id="trigger_airbyte_sync",
        http_conn_id=AIRBYTE_CONN_ID,
        endpoint="api/v1/connections/sync",
        method="POST",
        data={"connectionId": CONNECTION_ID},
        headers={"Content-Type": "application/json"},
        response_filter=lambda response: response.json()["job"]["id"],
        log_response=True
    )

    @task(poll_interval=30, timeout=3600, retries=0)
    def wait_for_airbyte_sync(job_id: int):
        url = f"http://airbyte.airbyte.svc.cluster.local:8001/api/v1/jobs/get"
        while True:
            res = requests.post(url, json={"id": job_id})
            status = res.json()["job"]["status"]
            if status in ["succeeded", "failed", "cancelled"]:
                if status != "succeeded":
                    raise Exception(f"Airbyte job failed with status: {status}")
                return
            time.sleep(30)

    end = EmptyOperator(task_id="end")

    job_id = trigger_sync.output  # job_id passed to sensor
    sync_status = wait_for_airbyte_sync(job_id)

    start >> trigger_sync >> sync_status >> end

dag = trigger_airbyte_sync_http()