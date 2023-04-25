import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator

from user_definition import *
from call_api import *
from upload_gcp import *

# NOTE : In order to make sure it send configurations requests first, do not import your .py reading from gs.



from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# spark_submit_args = ['--master', 'yarn']

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 2, 24),
    'email': ['goal.diggers@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def _download_static_review_data():
    static_review_file = "data/static_review_local.json"
    write_json_to_gcs(bucket_name, blob_name_static, service_account_key_file, static_review_file)

    
def _download_api_review_data():
    step1(bucket_name, blob_name_api, service_account_key_file)
    

with DAG(
    default_args=default_args,
    dag_id="mani",
    schedule=None,
    catchup=False
) as dag:

    create_insert_aggregate = SparkSubmitOperator(
        # spark_submit_args=spark_submit_args,
        task_id="aggregate_creation",
        packages="com.google.cloud.bigdataoss:gcs-connector:hadoop2-1.9.17,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
        exclude_packages="javax.jms:jms,com.sun.jdmk:jmxtools,com.sun.jmx:jmxri",
        conf={"spark.driver.userClassPathFirst":True,
             "spark.executor.userClassPathFirst":True,
            #  "spark.hadoop.fs.gs.impl":"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            #  "spark.hadoop.fs.AbstractFileSystem.gs.impl":"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            #  "spark.hadoop.fs.gs.auth.service.account.enable":True,
            #  "google.cloud.auth.service.account.json.keyfile":service_account_key_file,
             },
        verbose=True,
        application='aggregates_to_mongo.py'
    )
    download_api_review_data = PythonOperator(task_id = "download_api_review_data",
                                                  python_callable = _download_api_review_data,
                                                  dag=dag) #api call

    download_static_review_data = PythonOperator(task_id = "download_static_review_data",
                                                    python_callable = _download_static_review_data,
                                                    dag=dag) #static
    
    download_api_review_data >> create_insert_aggregate
    download_static_review_data >> create_insert_aggregate
