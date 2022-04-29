import os
import sys
import logging
from textwrap import indent
import threading
from impala.util import as_pandas
import time
from concurrent.futures import ThreadPoolExecutor
import utils
import json
import re
import pandas as pd
import pymysql
import requests

logger = logging.getLogger(__name__)

def get_sql_text(query_id):
    url = f'http://192.168.1.146:9527/raw-sql-by-id?query_id={query_id}'
    response = requests.get(url)
    tenant = response.headers['x-tenant']
    db = response.headers['x-db']
    impala_sql = response.text
    # print(tenant)
    # print(db)
    # print(impala_sql)
    url = f'http://192.168.1.146:9527/transfer-by-text?tenant={tenant}&db={db}'
    response = requests.post(url, data=impala_sql.encode('utf-8'), headers={'Content-Type': 'text/plain;charset=utf-8'})
    tidb_sql = response.text
    # print(tidb_sql)
    return tenant, db, impala_sql, tidb_sql

def test_one(conn, cursor, query_id, impala_start_time, impala_duration):
    tenant, db, impala_sql, tidb_sql = get_sql_text(query_id)
    try:
        err_msg = ''
        duration = 0
        if db != 'default':
            utils.exec_tidb_sql(conn, f'use {db}')
        start = time.time()
        utils.exec_tidb_sql(conn, tidb_sql)
        duration = time.time() - start
    except Exception as e:
        err_msg = str(e)
        logger.error(f'{query_id} error')
    
    sql = 'insert into test.translate_test (query_id, tenant, db, impala_start_time, impala_sql, impala_duration, tidb_sql, tidb_duration, success, err_msg)' +\
        f'values("{query_id}", "{tenant}", "{db}", "{impala_start_time}", "{pymysql.escape_string(impala_sql)}", \
                 {impala_duration}, "{pymysql.escape_string(tidb_sql)}", {duration}, {0 if err_msg else 1}, \
                 "{pymysql.escape_string(err_msg)}")'
    # logger.error(sql)
    utils.exec_tidb_sql(conn, sql)

    sql = f'update dp_stat.impala_query_log set processed = true where query_id = "{query_id}"'
    utils.exec_impala_sql(cursor, sql)

def test_batch():
    batch_size = 100
    conn = utils.get_tidb_conn()
    sql = f"SELECT query_id, start_time, duration FROM dp_stat.impala_query_log where processed = false and start_time >= '2022-4-22' and start_time < '2022-4-29' limit {batch_size}"
    cursor = utils.get_impala_cursor()
    cursor.execute(sql)
    df = as_pandas(cursor)
    logger.info(f'get {len(df)} sqls')
    for i in range(len(df)):
        test_one(conn, cursor, df.at[i, 'query_id'], df.at[i, 'start_time'].strftime('%Y-%m-%d %H:%M:%S'), df.at[i, 'duration'])
        logger.info(f'{i + 1} / {batch_size} finish')
        # break
    cursor.close()
    conn.close()
    return len(df) > 0

def re_run_error_sql(conn, id, query_id):
    tenant, db, impala_sql, tidb_sql = get_sql_text(query_id)
    try:
        err_msg = ''
        duration = 0
        if db != 'default':
            utils.exec_tidb_sql(conn, f'use {db}')
        start = time.time()
        utils.exec_tidb_sql(conn, tidb_sql)
        logger.info(f'{query_id} success now !')
        duration = time.time() - start
    except Exception as e:
        err_msg = str(e)
        logger.error(f'{query_id} still error')
    
    sql = f'update test.translate_test set tidb_sql="{pymysql.escape_string(tidb_sql)}", tidb_duration={duration}, \
        success = {0 if err_msg else 1}, err_msg="{pymysql.escape_string(err_msg)}" where id={id}'
    # logger.error(sql)
    utils.exec_tidb_sql(conn, sql)

def re_run_error_sql_batch(start_id=0):
    batch_size = 100
    conn = utils.get_tidb_conn()
    sql = f'select id, query_id from test.`translate_test` where success=0 and id > {start_id} order by id limit {batch_size}'
    df = utils.get_tidb_data(conn, sql)
    logger.info(f'get {len(df)} sqls')
    last_id = 0
    for i in range(len(df)):
        last_id = df.at[i, 'id']
        query_id = df.at[i, 'query_id']
        re_run_error_sql(conn, last_id, query_id)
    return last_id

def main():
    # while test_batch():
    #     pass
    last_id = re_run_error_sql_batch()
    while last_id > 0:
        last_id = re_run_error_sql_batch(last_id)

if __name__ == '__main__':
    main()