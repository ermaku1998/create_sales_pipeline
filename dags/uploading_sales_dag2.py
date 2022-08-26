from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from utils import uploading_sales as u


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
      
  drop1 = PythonOperator(
        task_id='drop_pmix_sales',
        python_callable=u.drop_pmix_sales
                            )
  drop2 = PythonOperator(
        task_id='drop_dates_last40',
        python_callable=u.drop_dates_last40
                            )
  drop3 = PythonOperator(
        task_id='drop_assort_matrix',
        python_callable=u.drop_assort_matrix
                            )
  drop4 = PythonOperator(
        task_id='drop_price_hist',
        python_callable=u.drop_price_hist
                            )
  drop5 = PythonOperator(
        task_id='drop_assort_last40',
        python_callable=u.drop_assort_last40
                            )
  drop6 = PythonOperator(
        task_id='drop_prhist_last40',
        python_callable=u.drop_prhist_last40
                            )
  drop7 = PythonOperator(
        task_id='drop_assort_prhist_last40',
        python_callable=u.drop_assort_prhist_last40
                            )
  oracle_to_postgre = PythonOperator(
        task_id='from_oracle_to_postgre',
        python_callable=u.from_oracle_to_postgre
                            )  
  create = PythonOperator(
        task_id='creating',
        python_callable=u.creating
                            ) 
  insert_into_sales = PythonOperator(
        task_id='add_to_sales',
        python_callable=u.add_to_sales
                            ) 
  
  
  drop1 >> drop2 >> drop3 >> drop4 >> drop5 >> drop6 >> drop7 >> oracle_to_postgre >> create >> insert_into_sales
