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
_date = utils.conf.get('translate', 'date')

def run_one(query_id, sql_type, catalog, tiflash_only):
    host = utils.conf.get('translate', 'api-host')
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
        if sql_type == 'Query':
            conn = utils.get_tidb_conn(auto_commit=False, tiflash_only=tiflash_only)
        else:
            conn = utils.get_tidb_conn()
        err_msg = ''
        catalog = ''
        if db != 'default':
            utils.exec_tidb_sql(conn, f'use {db}')
        start = time.time()
        utils.exec_tidb_sql(conn, tidb_sql, 20)
        duration = time.time() - start
        conn.close()
    except TimeoutError as e:
        err_msg = str(e)
        catalog = 'timeout'
    except Exception as e:
        err_msg = str(e)
    execute_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn = utils.get_tidb_conn()
    if err_msg:
        #print(err_msg)
        sql = f'update test.translate_sqls set \
                execute_result = 0, \
                tidb_sql = "{escape_string(tidb_sql)}", \
                execute_time = "{execute_time}" \
                where query_id = "{query_id}"'
        utils.exec_tidb_sql(conn, sql)
        if not catalog:
            catalog = translate_utils.get_error_catalog(tidb_sql, err_msg)
        sql = f'update test.translate_err set err_msg = "{escape_string(err_msg)}", catalog="{catalog}" \
                where query_id = "{query_id}"'
        utils.exec_tidb_sql(conn, sql)
        conn.close()
        return False
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
        sql = f'select te.query_id, ts.sql_type, te.catalog, ts.tiflash_only from test.translate_err te \
                join test.`translate_sqls` ts on ts.`query_id` = te.`query_id` \
                where te.catalog in ("translate_error") and te.sql_date="{_date}"'
        df = utils.get_tidb_data(conn, sql)
    total_count = len(df)
    success_count = 0
    for i in range(total_count):
        query_id = df.at[i, 'query_id']
        sql_type = df.at[i, 'sql_type']
        catalog = df.at[i, 'catalog']
        tiflash_only = df.at[i, 'tiflash_only']
        if run_one(query_id, sql_type, catalog, tiflash_only):
            success_count += 1
            logger.info(f'{i+1} / {total_count} {query_id} success now')
        else:
            logger.info(f'{i+1} / {total_count} {query_id} still fail')
    logger.info(f'{success_count} / {total_count} success')


if __name__ == '__main__':
    run()
