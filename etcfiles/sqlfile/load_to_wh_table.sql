INSERT INTO {{params.load_table}}
SELECT * FROM {{ params.table_name }};
