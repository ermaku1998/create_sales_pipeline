from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from utils.uploading_sales import uploading_sales


default_args= {
               'owner': 'Artem',
               'email_on_failure': False,
               'start_date': datetime(2022, 8, 26, 20, 0, 0)
              }


with DAG(
         "uploading_sales_dag",
         description='Uploading the general sales table for the last 40 days',
         schedule_interval=timedelta(days=1),
         default_args=default_args, 
         catchup=False,
         tags=['sales', 'pmix']
        ) as dag:       
      
  uploading = PythonOperator(
        task_id='upload',
        python_callable=uploading_sales
                            )
  
  uploading   
