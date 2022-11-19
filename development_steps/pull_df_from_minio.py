import pandas as pd
import logging
import boto3
from datetime import datetime
import io
from botocore.config import Config

# today = datetime.now().strftime('%Y%m%d')
# current_ts = datetime.now().strftime('%Y%m%d%H%M%S')

s3_client = boto3.client('s3', endpoint_url='http://localhost:9000')
s3_res = boto3.resource('s3', endpoint_url='http://localhost:9000')
#s3_res.create_bucket(Bucket='dataops')

## Read from object storage
def transform_save_object_silver():
    # Read from bronze
    df = load_df_from_s3(bucket='dataops', key='sales/20221023/mall_customers_20221023155942.csv')

    print(df.head())

def save_df_to_s3(df, bucket, key, index=False):
    ''' Store df as a buffer, then save buffer to s3'''
    # s3_res = get_s3_resource()
    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=index)
        s3_res.Object(bucket, key).put(Body=csv_buffer.getvalue())
        logging.info(f'{key} saved to s3 bucket {bucket}')
    except Exception as e:
        raise logging.exception(e)


def load_df_from_s3(bucket, key, index_col=None, usecols=None, sep=","):
    ''' Read a csv from a s3 bucket & load into pandas dataframe'''
    try:
        logging.info(f"Loading {bucket, key}")
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        return pd.read_csv(obj['Body'], sep=sep, index_col=index_col, usecols=usecols, low_memory=False)
    except Exception as e:
        raise logging.exception(e)


def load_files_to_object():
    df = pd.read_csv("/home/train/datasets/Mall_Customers.csv")

    print(df.head())
    ## Write to object storage
    save_df_to_s3(df=df, bucket='dataops', key='sales/20221023/mall_customers_20221023155942.csv')






## DAG

from airflow import DAG
from datetime import datetime, timedelta
import pandas as pd
from airflow.operators.python import PythonOperator

start_date = datetime(2022, 10, 11)
# current_ts = datetime.now().strftime('%Y%m%d_%H%M%S')
default_args = {
    'owner': 'train',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('pull_to_df', default_args=default_args, schedule_interval='*/5 * * * *',
         catchup=False) as dag:
    push_files = PythonOperator(task_id='push_files', python_callable=load_files_to_object)
    pull_files = PythonOperator(task_id='pull_files', python_callable=transform_save_object_silver)

    push_files >> pull_files

