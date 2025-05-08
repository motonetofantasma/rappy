from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_meetup_dag',
    default_args=default_args,
    schedule_interval='*/15 * * * *',
    catchup=False,
    tags=['meetup', 'snowflake', 's3'],
) as dag:

    crear_tablas_auxiliares = SnowflakeOperator(
        task_id='crear_tablas_auxiliares',
        sql='sql/crear_tablas_auxiliares.sql',
        snowflake_conn_id='snowflake_conn',
        warehouse='RAPPIPAY',
        database='RAPPIPAY',
        schema='RAW',
    )

    exportar_a_s3 = SnowflakeOperator(
        task_id='exportar_resultado_a_s3',
        sql='sql/exportar_a_s3.sql',
        snowflake_conn_id='snowflake_conn',
        warehouse='RAPPIPAY',
        database='RAPPIPAY',
        schema='RAW',
    )

    crear_tablas_auxiliares >> exportar_a_s3
