from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

DBT_PROJECT_DIR = "/opt/dbt/healthcare-dbt-mcp/Silver"  

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbt_silver_gold_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",   
    catchup=False,
    tags=["dbt", "silver", "gold"],
) as dag:

    #Run Silver (Staging) models
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"""
        cd /opt/dbt/healthcare-dbt-mcp/Silver && \
        dbt run --select path:models/staging
        """
    )

    # Test Silver (Staging)
    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"""
        cd /opt/dbt/healthcare-dbt-mcp/Silver && \
        dbt test --select path:models/staging
        """
    )

    # Run Gold (Marts)
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"""
        cd /opt/dbt/healthcare-dbt-mcp/Silver && \
        dbt run --select path:models/marts
        """
    )

    # Task order
    dbt_run_staging >> dbt_test_staging >> dbt_run_marts 
