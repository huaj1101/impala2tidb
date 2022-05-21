import os
import sys
import logging
from textwrap import indent
import threading
from impala.util import as_pandas
import time
import utils
import json
from typing import List
import pandas as pd
import pymysql
import requests
from pymysql.converters import escape_string

logger = logging.getLogger(__name__)
_date = utils.conf.get('translate-api', 'date')

def run_one(query_id, conn):
    host = utils.conf.get('translate-api', 'host')
    url = f'{host}/translate?table_postfix={_date}&query_id={query_id}'
    response = requests.get(url)
    if response.status_code != 200:
        logger.error(f'api error, url: {url}, status_code: {response.status_code}, respoinse: {response.text}')
        return False
    result = json.loads(response.text)
    if not result['success']:
        err_msg = result['error']
        sql = f'update test.translate_err set err_msg = "{escape_string(err_msg)}" where query_id = "{query_id}"'
        utils.exec_tidb_sql(conn, sql)
        return False
    cursor = utils.get_impala_cursor()
    sql = f'update dp_stat.impala_query_log_{_date} set saved_in_tidb = false, processed = false where query_id = "{query_id}"'
    utils.exec_impala_sql(cursor, sql)
    sql = f'delete from test.translate_err where query_id = "{query_id}"'
    utils.exec_tidb_sql(conn, sql)
    return True

def run():
    with utils.get_tidb_conn() as conn:
        sql = 'select query_id from test.translate_err'
        df = utils.get_tidb_data(conn, sql)
        total_count = len(df)
        success_count = 0
        for i in range(total_count):
            query_id = df.at[i, 'query_id']
            if run_one(query_id, conn):
                success_count += 1
                logger.info(f'{i+1} / {total_count} {query_id} success now')
            else:
                logger.info(f'{i+1} / {total_count} {query_id} still fail')

    logger.info(f'{success_count} / {total_count} success')


if __name__ == '__main__':
    run()
