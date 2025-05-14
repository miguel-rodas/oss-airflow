from airflow.decorators import dag, task
from pendulum import datetime

# Define the DAG with decorator
@dag(
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example", "demo"],
)
def simple_example_dag():
    
    @task
    def extract():
        print("✅ Extracting data...")
        return {"data": "some_value"}
    
    @task
    def transform(data: dict):
        print(f"🔄 Transforming data: {data}")
        return data["data"].upper()
    
    @task
    def load(result: str):
        print(f"📥 Loading result: {result}")
    
    # Task pipeline
    raw_data = extract()
    transformed = transform(raw_data)
    load(transformed)

# Instantiate the DAG
dag = simple_example_dag()