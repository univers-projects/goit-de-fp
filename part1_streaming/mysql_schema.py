"""DDL for the MySQL sink table.

Spark's JDBC writer would create `timestamp` as second-precision TIMESTAMP, which makes
micro-batches finished within the same second indistinguishable. The table is therefore
created up front with millisecond precision.
"""

import logging
import re

import pymysql

from config import Settings

logger = logging.getLogger(__name__)

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")


def ensure_output_table(settings: Settings) -> None:
    table = settings.mysql_output_table
    if not _IDENTIFIER.match(table):
        raise ValueError(f"Invalid MySQL table name: {table!r}")
    ddl = f"""
        CREATE TABLE IF NOT EXISTS `{table}` (
            sport        VARCHAR(255),
            medal        VARCHAR(32),
            sex          VARCHAR(16),
            country_noc  VARCHAR(16),
            avg_height   DOUBLE,
            avg_weight   DOUBLE,
            `timestamp`  TIMESTAMP(3) NULL
        )
    """
    connection = pymysql.connect(
        host=settings.mysql_host,
        port=int(settings.mysql_port),
        user=settings.mysql_user,
        password=settings.mysql_password,
        database=settings.mysql_db,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(ddl)
        connection.commit()
    finally:
        connection.close()
    logger.info("Output table %s is ready", table)
