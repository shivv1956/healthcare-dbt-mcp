from airflow import DAG
from airflow.operators.bash import BashOperator
# from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from airflow.models import Variable

# Paths & Config
DBT_PROJECT_DIR = "/opt/dbt/healthcare-dbt-mcp/Silver"  
SNOWFLAKE_PIPE_NAME = "FHIR_INGESTION_PIPE"  # Replace
# SNOWFLAKE_DATABASE = "SYNTHEA_HOSPITAL"            
# SNOWFLAKE_SCHEMA = "RAW"                  
# SNOWFLAKE_WAREHOUSE = "COMPUETE_WH"    

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dbt_hl7_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["dbt", "silver", "gold", "snowflake"],
) as dag:

    #Wait for Member 1 DAG to finish uploading S3
    wait_for_upload = ExternalTaskSensor(
        task_id="wait_for_s3_upload",
        external_dag_id="s3_upload_patient_data",   
        external_task_id="mark_as_uploaded",        
        mode="poke",
        poke_interval=60,
        timeout = 24*60*60,   # fail if not done in 1 day
        retries=0,
    )

    #Run Snowflake pipe to ingest new S3 data
#     run_snowflake_pipe = SnowflakeOperator(
#     task_id="run_snowflake_pipe",
#     snowflake_conn_id="snowflake_conn",
#     sql="ALTER PIPE FHIR_INGESTION_PIPE REFRESH;"
# )

    run_snowflake_pipe = BashOperator(
    task_id="refresh_snowflake_pipe",
    bash_command="""
    snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER -p $SNOWFLAKE_PASSWORD \
    -d $SNOWFLAKE_DB -s $SNOWFLAKE_SCHEMA -q "ALTER PIPE my_pipe REFRESH;"
    """,
    env={
        "SNOWFLAKE_ACCOUNT": "{{ conn.snowflake_conn.login }}",
        "SNOWFLAKE_USER": "{{ conn.snowflake_conn.login }}",
        "SNOWFLAKE_PASSWORD": "{{ conn.snowflake_conn.password }}",
        "SNOWFLAKE_DB": "{{ conn.snowflake_conn.schema }}",
        "SNOWFLAKE_SCHEMA": "{{ conn.snowflake_conn.extra_dejson['schema'] }}"
    }
)


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

    #Run Gold (Marts)
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"""
        cd /opt/dbt/healthcare-dbt-mcp/Silver && \
        dbt run --select path:models/marts
        """
    )

    wait_for_upload >> run_snowflake_pipe >> dbt_run_staging >> dbt_test_staging >> dbt_run_marts
