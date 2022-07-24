CREATE OR REPLACE STAGE {{ params.stage_name }} url=s3://snowflakedata212
credentials=(aws_key_id='{{ conn.aws_con.login }}' aws_secret_key='{{ conn.aws_con.password }}')
file_format=(type = 'CSV', skip_header = 1, time_format = 'YYYY-MM-DD HH24:MI:SS');
