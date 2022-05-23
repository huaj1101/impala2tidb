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
_date = utils.conf.get('translate-api', 'date')

def run_one(query_id, catalog):
    host = utils.conf.get('translate-api', 'host')
    url = f'{host}/transfer-by-id?table_postfix={_date}&query_id={query_id}'
    try:
        response = requests.get(url)
    except Exception as e:
        logger.error(f'api error: {e}')
        return False
    if response.status_code != 200:
        with utils.get_tidb_conn() as conn:
            sql = f'update test.translate_err set catalog="translate_error", err_msg = "{escape_string(response.text)}" where query_id = "{query_id}"'
            utils.exec_tidb_sql(conn, sql)
            sql = f'update test.translate_sqls set execute_result = null, tidb_sql = "" where query_id = "{query_id}"'
            utils.exec_tidb_sql(conn, sql)
            return False
    db = response.headers['x-session-db']
    tidb_sql = response.text
    try:
        conn = utils.get_tidb_conn()
        err_msg = ''
        catalog = ''
        if db != 'default':
            utils.exec_tidb_sql(conn, f'use {db}')
        start = time.time()
        utils.exec_tidb_sql(conn, tidb_sql, 20)
        duration = time.time() - start
    except TimeoutError as e:
        err_msg = str(e)
        catalog = 'timeout'
        conn = utils.get_tidb_conn()
    except Exception as e:
        err_msg = str(e)
    if err_msg:
        sql = f'update test.translate_sqls set execute_result = 0, tidb_sql = "{escape_string(tidb_sql)}" where query_id = "{query_id}"'
        utils.exec_tidb_sql(conn, sql)
        if not catalog:
            catalog = translate_utils.get_error_catalog(tidb_sql, err_msg)
        sql = f'update test.translate_err set err_msg = "{escape_string(err_msg)}", catalog="{catalog}" where query_id = "{query_id}"'
        utils.exec_tidb_sql(conn, sql)
        conn.close()
        return False
    execute_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sql = f'update test.translate_sqls set \
                tidb_sql = "{escape_string(tidb_sql)}", \
                tidb_duration = {duration}, \
                execute_result = 1, \
                execute_time = "{execute_time}" \
            where query_id = "{query_id}"'
    utils.exec_tidb_sql(conn, sql)
    sql = f'delete from test.translate_err where query_id = "{query_id}"'
    utils.exec_tidb_sql(conn, sql)
    conn.close()
    return True

def run():
    with utils.get_tidb_conn() as conn:
        sql = 'select query_id, catalog from test.translate_err where catalog != "timeout"'
        df = utils.get_tidb_data(conn, sql)
    total_count = len(df)
    success_count = 0
    for i in range(total_count):
        query_id = df.at[i, 'query_id']
        # logger.info(query_id)
        catalog = df.at[i, 'catalog']
        if run_one(query_id, catalog):
            success_count += 1
            logger.info(f'{i+1} / {total_count} {query_id} success now')
        else:
            logger.info(f'{i+1} / {total_count} {query_id} still fail')
    logger.info(f'{success_count} / {total_count} success')


if __name__ == '__main__':
    run()
