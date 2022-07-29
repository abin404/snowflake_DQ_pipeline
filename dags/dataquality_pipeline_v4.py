import pandas as pd
from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import datetime
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.s3 import S3DeleteObjectsOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.operators.email import EmailOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeCheckOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeValueCheckOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeIntervalCheckOperator
from threshold import SnowflakeThresholdCheckOperator

default_args = {
    "owner": "airflow",
    "email_on_failure": True,
    "email_on_retry": False,
    "email": "abin404@yopmail.com"
}

TABLE = "YELLOW_TRIPDATA"
LOAD_TABLE="TAXI_DATA_DW_TABLE"
DATES = ["2020-04", "2020-05"]
TASK_DICT = {}
SNOWFLAKE_CONN_ID = "my_snowflake_conn"

with DAG(
    "dataquality_pipeline_v4",
    default_args=default_args,
    start_date=datetime(2022, 7, 7),
    description="Continous Data Quality Checks in snowflake pipeline",
    schedule_interval=None,
    template_searchpath="/opt/airflow/sqlfile/",
    catchup=False,
) as dag:

    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")
    converge_1 = EmptyOperator(task_id="converge_1")

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

    
    create_snowflake_table = SnowflakeOperator(
        
        task_id="create_snowflake_table",
        sql="{% include 'create_snowflake_yellow_tripdata_table.sql' %}",
        params={"table_name": TABLE}
    )

    create_snowflake_stage = SnowflakeOperator(
        task_id="create_snowflake_stage",
        sql="{% include 'create_snowflake_yellow_tripdata_stage.sql' %}",
        params={"stage_name": f"{TABLE}_STAGE"}
    )

    load_to_snowflake = S3ToSnowflakeOperator(
        task_id="load_to_snowflake",
        stage=f"{TABLE}_STAGE",
        table=TABLE,
        file_format="(type = 'CSV', skip_header = 1, time_format = 'YYYY-MM-DD HH24:MI:SS')"
    )
    delete_objects = S3DeleteObjectsOperator(
        task_id="s3_delete_objects",
        bucket="snowflakedata212",
        prefix ="",
        aws_conn_id="aws-conn",
    )
    value_check = SnowflakeValueCheckOperator(
        task_id="check_row_count",
        sql=f"SELECT COUNT(*) FROM {TABLE};",
        pass_value=20000,
        tolerance=(0.5)
    )

    interval_check = SnowflakeIntervalCheckOperator(
        task_id="check_interval_data",
        table=TABLE,
        days_back=-1,
        date_filter_column="upload_date",
        metrics_thresholds={"AVG(trip_distance)": 1.5},
    )

    threshold_check = SnowflakeThresholdCheckOperator(
        task_id="check_threshold",
        sql=f"SELECT MAX(passenger_count) FROM {TABLE};",
        min_threshold=1,
        max_threshold=8,
    )

    with TaskGroup(group_id="row_quality_checks") as quality_check_group:

        date_check = SnowflakeCheckOperator(
            task_id="date_check",
            sql="row_quality_yellow_tripdata_template.sql",
            params={
                "check_name": "date_check",
                "check_statement": "dropoff_datetime > pickup_datetime",
                "table": TABLE
            },
        )

        passenger_count_check = SnowflakeCheckOperator(
            task_id="passenger_count_check",
            sql="row_quality_yellow_tripdata_template.sql",
            params={
                "check_name": "passenger_count_check",
                "check_statement": "passenger_count >= 0",
                "table": TABLE
            },
        )

        trip_distance_check = SnowflakeCheckOperator(
            task_id="trip_distance_check",
            sql="row_quality_yellow_tripdata_template.sql",
            params={
                "check_name": "trip_distance_check",
                "check_statement": "trip_distance >= 0 AND trip_distance <= 100",
                "table": TABLE
            },
        )

        fare_check = SnowflakeCheckOperator(
            task_id="fare_check",
            sql="row_quality_yellow_tripdata_template.sql",
            params={
                "check_name": "fare_check",
                "check_statement": "ROUND((fare_amount + extra + mta_tax + tip_amount + improvement_surcharge + COALESCE(congestion_surcharge, 0)), 1) = ROUND(total_amount, 1) THEN 1 WHEN ROUND(fare_amount + extra + mta_tax + tip_amount + improvement_surcharge, 1) = ROUND(total_amount, 1)",
                "table": TABLE
            },
        )

    load_into_DW_table = SnowflakeOperator(
        task_id="load_to_wh_table",
        sql="{% include 'load_to_wh_table.sql' %}",
        params={"table_name": TABLE,
                "load_table": LOAD_TABLE
        }
    )

    send_email_notification = EmailOperator(
        task_id="send_email_notification",
        to="abin404@yopmail.com",
        subject="dataQuality_pipeline",
        html_content="<h3>All Test are Passed!!! Data loaded into Data Warehouse </h3>"
    )

    for i, date in enumerate(DATES):
        file_name = f"yellow_tripdata_{date}.csv"
        file_path = f"/opt/airflow/datafile/yellow_trip_data_cleaned/{file_name}"

        TASK_DICT[f"add_upload_date_{date}"] = add_upload_date(
            file_path,
            "{{ macros.ds_add(ds, " + str(-i) + ") }}"
        )

        """
        #### Upload task
        Simply loads the file to a specified location in S3.
        """
        TASK_DICT[f"upload_to_s3_{date}"] = LocalFilesystemToS3Operator(
            task_id=f"upload_to_s3_{date}",
            filename=file_path,
            dest_key="s3://snowflakedata212/" + file_name,
            aws_conn_id="aws-conn",
            replace=True
        )

        chain(
            begin,
            [TASK_DICT[f"add_upload_date_{date}"]],
            converge_1,
            [TASK_DICT[f"upload_to_s3_{date}"]],
            create_snowflake_table,
            create_snowflake_stage,
            load_to_snowflake,
            [quality_check_group, value_check,interval_check, threshold_check],
            load_into_DW_table,
            send_email_notification,
            delete_objects,
            end
        )
