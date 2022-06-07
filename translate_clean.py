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

def run():
    batch = 8
    conn = utils.get_tidb_conn()
    try:
        for i in range(batch):
            sql = f'update test.`translate_sqls` set execute_result = null \
                where sql_date="530" and execute_result > 0 and order_id %% {batch} = {i}'
            conn.execute(sql)
            logger.info(f'finish {i + 1} / {batch}')
    finally:
        conn.close()

if __name__ == '__main__':
    run()
