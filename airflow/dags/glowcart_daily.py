from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'glowcart',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='glowcart_daily_pipeline',
    default_args=default_args,
    description='Pipeline GlowCart - update setiap 5 menit',
    schedule='*/5 * * * *',
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['glowcart', 'kafka'],
) as dag:

    generate_data = BashOperator(
        task_id='generate_data',
        bash_command='timeout 60 python3 /opt/airflow/dags/producer_task.py || true',
    )

    save_data = BashOperator(
        task_id='save_data',
        bash_command='timeout 30 python3 /opt/airflow/dags/consumer_task.py || true',
    )

    spark_process = BashOperator(
        task_id='spark_process',
        bash_command="""
            pip install pyspark psycopg2-binary -q &&
            export JAVA_HOME=/usr/lib/jvm/default-java &&
            python3 /opt/airflow/dags/spark_task.py || true
        """,
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command="""
            pip install dbt-postgres -q &&
            cd /opt/airflow/dags/glowcart_dbt &&
            dbt run --profiles-dir /opt/airflow/dags/glowcart_dbt || true
        """,
    )

    generate_data >> save_data >> spark_process >> dbt_run