import os
import sys
import logging
from textwrap import indent
import threading
from impala.util import as_pandas
import time
import utils
import datetime
from typing import List
import pandas as pd
import translate_utils
import requests
from pymysql.converters import escape_string

logger = logging.getLogger(__name__)
conn = utils.get_tidb_conn()
sql = 'select db, tidb_sql from test.translate_sqls where query_id = "214270402e123434:6b582c8500000000"'
df = utils.get_tidb_data(conn, sql)
db = df.at[0, 'db']
sql = df.at[0, 'tidb_sql']
utils.exec_tidb_sql(conn, f'use {db}')
start = time.time()
try:
    utils.exec_tidb_sql(conn, sql, 1)
except Exception as e:
    logger.info(str(e))
logger.info(f'finish in {time.time() - start} s')