from airflow.models import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash import BashOperator

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'Angela',
    'depends_on_past': False,
    'email': ['angelaa.x@icloud.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2020, 1, 1),
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2022, 1, 1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'trigger_rule': 'all_success'
}

dag_spark = DAG(
        dag_id = "pinterest",
        default_args=args,
        # schedule_interval='0 0 * * *',
        schedule_interval='@once',	
        dagrun_timeout=timedelta(minutes=60),
        description='use case of sparkoperator in airflow',
        start_date = airflow.utils.dates.days_ago(1))

spark_submit_local = SparkSubmitOperator(
		application ='/home/pinterest-data-processing-pipeline/spark_script.py' ,
		conn_id= 'spark_local', 
		task_id='s3_to_spark', 
		dag=dag_spark
		)
