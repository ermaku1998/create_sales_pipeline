from airflow.models import DAG, Variable

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

import datetime


default_args= {
               'owner': 'Artem',
               'email_on_failure': False,
               'start_date': datetime.datetime(2022, 8, 25)
              }


with DAG(
         "uploading_sales_dag",
         description='',
         schedule_interval='@daily',
         default_args=default_args, 
         catchup=False
        ) as dag:       
            
with TaskGroup('uploading_sales') as uploading_sales:
        uploading = PythonOperator(
                                   task_id = "uploading",
                                   py = '/home/sassci/jupyter_services/artem/uploading_sales_dag.py'
                                  )
       
 uploading    
