from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
from collection.vistara_data import check_vistara_status, get_vistara_data
from collection.airIndia_data import check_airindia_status, get_airindia_data
from collection.spiceJet_data import get_spicejet_data_from_sql
from insertion.insert_data import insert_to_staging
from datetime import datetime

dag = DAG(
    dag_id='miniproject',
    description='Data ingestion pipeline',
    schedule_interval='@hourly',
    start_date=datetime(2021, 1, 1),
    catchup=False
)

# Define Tasks
check_status_vistara = PythonOperator(
    task_id='check_vistara_status',
    python_callable=check_vistara_status,
    op_kwargs={'conn_id': 'githubgistconn'},
    dag=dag
)

get_vistara_data_task = PythonOperator(
    task_id='get_vistara_data',
    python_callable=get_vistara_data,
    dag=dag
)

check_status_airindia = PythonOperator(
    task_id='check_airindia_status',
    python_callable=check_airindia_status,
    op_kwargs={'conn_id': 'fileconn'},
    dag=dag
)

get_airindia_data_task = PythonOperator(
    task_id='get_airindia_data',
    python_callable=get_airindia_data,
    op_kwargs={'conn_id': 'fileconn'},
    dag=dag
)

get_spicejet_data_task = PythonOperator(
    task_id='get_spicejet_data',
    python_callable=get_spicejet_data_from_sql,
    dag=dag
)

insert_to_staging_task = PythonOperator(
    task_id='insert_to_staging',
    python_callable=insert_to_staging,
    op_kwargs={'file_path': '/opt/airflow/dags/ixigo.parquet'},
    dag=dag
)

enrich_airline_data = MsSqlOperator(
        task_id='enrich_airline_data',
        mssql_conn_id='mssqlconn',
        sql='EXEC sp_EnrichAirlineData;',
        autocommit=True
)

    # Task to call sp_MergeAirlineData
merge_airline_data = MsSqlOperator(
        task_id='merge_airline_data',
        mssql_conn_id='mssqlconn',
        sql='EXEC sp_BlAirlineData;',
        autocommit=True
)

# Define Task Dependencies
check_status_vistara >> get_vistara_data_task
check_status_airindia >> get_airindia_data_task
get_spicejet_data_task
[get_vistara_data_task, get_airindia_data_task , get_spicejet_data_task ] >> insert_to_staging_task >> enrich_airline_data >> merge_airline_data
