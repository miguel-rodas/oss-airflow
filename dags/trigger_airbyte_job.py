from airflow.decorators import dag
from pendulum import datetime
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.sensors.python import PythonSensor
from datetime import timedelta
import requests

AIRBYTE_CONN_ID = "airbyte_http_conn"
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

    trigger_sync = HttpOperator(
        task_id="trigger_airbyte_sync",
        http_conn_id=AIRBYTE_CONN_ID,
        endpoint="api/v1/connections/sync",
        method="POST",
        data={"connectionId": CONNECTION_ID},
        headers={"Content-Type": "application/json"},
        response_filter=lambda response: response.json()["job"]["id"],
        log_response=True,
        do_xcom_push=True,  # Needed to access output later
    )

    def check_airbyte_job():
        from airflow.models.xcom import XCom
        from airflow.utils.session import provide_session

        @provide_session
        def get_job_id_from_xcom(session=None):
            return XCom.get_one(
                execution_date="{{ ds }}",
                key="return_value",
                task_id="trigger_airbyte_sync",
                dag_id="trigger_airbyte_sync_http",
                session=session
            )

        job_id = get_job_id_from_xcom()
        if not job_id:
            return False

        url = "http://airbyte.airbyte.svc.cluster.local:8001/api/v1/jobs/get"
        response = requests.post(url, json={"id": job_id})
        status = response.json()["job"]["status"]
        if status in ["succeeded", "failed", "cancelled"]:
            if status != "succeeded":
                raise Exception(f"Airbyte job failed with status: {status}")
            return True
        return False

    wait_for_sync = PythonSensor(
        task_id="wait_for_airbyte_sync",
        python_callable=check_airbyte_job,
        poke_interval=30,
        timeout=3600,
        mode="poke"
    )

    end = EmptyOperator(task_id="end")

    start >> trigger_sync >> wait_for_sync >> end

dag = trigger_airbyte_sync_http()