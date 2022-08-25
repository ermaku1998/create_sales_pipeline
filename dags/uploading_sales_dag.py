from airflow.models import DAG, Variable

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime


default_args= {
               'owner': 'Artem',
               'email_on_failure': False,
               'start_date': datetime(2022, 08, 25)
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
            py = 'dags/utils/uploading_sales.py'
                                  )
       
 uploading    
