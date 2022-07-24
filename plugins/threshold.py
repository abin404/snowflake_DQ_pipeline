from typing import Any, Optional, SupportsAbs

from airflow.models import BaseOperator
from airflow.operators.sql import SQLCheckOperator, SQLIntervalCheckOperator, SQLValueCheckOperator, SQLThresholdCheckOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def _convert_to_float_if_possible(s):
    """
    A small helper function to convert a string to a numeric value
    if appropriate
    :param s: the string to be converted
    :type s: str
    """
    try:
        ret = float(s)
    except (ValueError, TypeError):
        ret = s
    return ret


def get_db_hook(self) -> SnowflakeHook:
    """
    Create and return SnowflakeHook.
    :return: a SnowflakeHook instance.
    :rtype: SnowflakeHook
    """
    return SnowflakeHook(
        snowflake_conn_id=self.snowflake_conn_id,
        warehouse=self.warehouse,
        database=self.database,
        role=self.role,
        schema=self.schema,
        authenticator=self.authenticator,
        session_parameters=self.session_parameters,
    )

class SnowflakeThresholdCheckOperator(SQLThresholdCheckOperator):
    
    template_fields = ("sql", "min_threshold", "max_threshold")
    template_ext = (
        ".hql",
        ".sql",
    )

    def __init__(
        self,
        *,
        sql: str,
        min_threshold: Any,
        max_threshold: Any,
        snowflake_conn_id: str = 'my_snowflake_conn',
        parameters: Optional[dict] = None,
        autocommit: bool = True,
        do_xcom_push: bool = True,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        role: Optional[str] = None,
        schema: Optional[str] = None,
        authenticator: Optional[str] = None,
        session_parameters: Optional[dict] = None,
        **kwargs,
    ):
        super().__init__(sql=sql, min_threshold=min_threshold,
                         max_threshold=max_threshold, **kwargs)
        self.sql = sql
        self.min_threshold = _convert_to_float_if_possible(min_threshold)
        self.max_threshold = _convert_to_float_if_possible(max_threshold)
        self.snowflake_conn_id = snowflake_conn_id
        self.autocommit = autocommit
        self.do_xcom_push = do_xcom_push
        self.parameters = parameters
        self.warehouse = warehouse
        self.database = database
        self.role = role
        self.schema = schema
        self.authenticator = authenticator
        self.session_parameters = session_parameters
        self.query_ids = []

    def get_db_hook(self) -> SnowflakeHook:
        return get_db_hook(self)
