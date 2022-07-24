from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.utils.dates import datetime
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from plugins.snowflake_check_operators import (
    SnowflakeCheckOperator,
    SnowflakeValueCheckOperator,
    SnowflakeIntervalCheckOperator,
    SnowflakeThresholdCheckOperator,
)

TABLE = "YELLOW_TRIPDATA"
DATES = ["2019-01", "2019-02"]
TASK_DICT = {}
SNOWFLAKE_CONN_ID = "my_snowflake_conn"

with DAG(
    "Data_Quality_Pipeline_v1",
    start_date=datetime(2021, 7, 7),
    description="A sample Airflow DAG to perform data quality checks using SQL Operators.",
    schedule_interval=None,
    template_searchpath="/opt/airflow/sqlfile/",
    catchup=False,
) as dag:


    def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
        hook = S3Hook('s3_conn')
        hook.load_file(filename=filename, key=key, bucket_name=bucket_name)

    @task
    def add_upload_date(file_path, upload_date):
  
        trip_dict = pd.read_csv(
            file_path,
            header=0,
            parse_dates=["pickup_datetime"],
            infer_datetime_format=True
        )
        trip_dict["upload_date"] = upload_date
        trip_dict.to_csv(
            file_path,
            header=True,
            index=False
        )

    @task
    def delete_upload_date(file_path):
      
        trip_dict = pd.read_csv(
            file_path,
            header=0,
            parse_dates=["pickup_datetime"],
            infer_datetime_format=True
        )
        trip_dict.drop(columns="upload_date", inplace=True)
        trip_dict.to_csv(
            file_path,
            header=True,
            index=False
        )
    for i, date in enumerate(DATES):
        file_name = f"yellow_tripdata_sample_{date}.csv"
        file_path = f"/opt/airflow/datafile/yellow_trip_data/{file_name}"

        TASK_DICT[f"add_upload_date_{date}"] = add_upload_date(
            file_path,
            "{{ macros.ds_add(ds, " + str(-i) + ") }}"
        )

        """
        #### Upload task
        Simply loads the file to a specified location in S3.
        """
        TASK_DICT[f"upload_to_s3_{date}"] = PythonOperator(
            task_id='upload_to_s3',
            python_callable=upload_to_s3,
            op_kwargs={
                'filename': file_path,
                'key': file_name,
                'bucket_name': 'snowflakedata212'
            }
        )

        chain(
            [TASK_DICT[f"add_upload_date_{date}"]],
            [TASK_DICT[f"upload_to_s3_{date}"]],
        )