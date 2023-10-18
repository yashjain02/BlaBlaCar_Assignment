from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from commons import fetch_data, data_transformation,date_partitioning

with DAG(
    dag_id='Netherlands_transportation',
    default_args={
        'depends_on_past': True,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'trigger_rule': 'all_success'
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 5, 9),
    catchup=False,
    tags=['Transportation', 'Netherlands', 'API'],
) as dag:
    
    
    query_data_write_to_datalake=PythonOperator(
    task_id='query_api_write_to_datalake',
    python_callable=fetch_data,
    op_kwargs={'url':"http://v0.ovapi.nl/line/",
               'datalake_path':  '/result/datalake',
               'rawdata_filename': 'raw_data.csv'},
    dag=dag
)
    
    data_transformation_write_to_csv=PythonOperator(
   task_id='data_transformation_write_to_datawarehouse',
   python_callable=data_transformation,
   op_kwargs={'datalake_path':'/result/datalake',
       'data_warehouse_path': '/result/datawarehouse/',
              'final_file': 'Line_data.csv',
              'rawdata_file':'raw_data.csv'},
   dag=dag
)


query_data_write_to_datalake>>data_transformation_write_to_csv