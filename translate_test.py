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
from pymysql.converters import escape_string

logger = logging.getLogger(__name__)
lock = threading.Lock()
finish_count = 0

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

def mark_not_support(conn, query_id):
    sql = f'update test.translate_test set status="CodeRepairing" where query_id="{query_id}"'
    utils.exec_tidb_sql(conn, sql)

def mark_ignore(conn, query_id):
    sql = f'update test.translate_test set status="Ignore" where query_id="{query_id}"'
    utils.exec_tidb_sql(conn, sql)

def count_one(total):
    global finish_count
    lock.acquire()
    finish_count += 1
    if finish_count % 10 == 0:
        logger.info(' %d / %d finish' % (finish_count, total))
    lock.release()

@utils.thread_method
def test_one(query_id, impala_start_time, impala_duration, total):
    conn = utils.get_tidb_conn()
    cursor = utils.get_impala_cursor()
    try:
        try:
            # 接口不稳定，偶发报错，跳过
            tenant, db, impala_sql, tidb_sql = get_sql_text(query_id)
            # 忽略一些sql
            if '_parquet_' in impala_sql or db == 'ai' or 'NotsupportFunctionError' in tidb_sql or 'NDV(' in impala_sql:
                sql = f'update dp_stat.impala_query_log set processed = true where query_id = "{query_id}"'
                utils.exec_impala_sql(cursor, sql)
                logger.info('skip one')
                count_one(total)
                return
        except:
            logger.error('api error once')
            count_one(total)
            return
        try:
            err_msg = ''
            duration = 0
            if db != 'default':
                utils.exec_tidb_sql(conn, f'use {db}')
            start = time.time()
            # logger.info(query_id)
            # logger.info(tidb_sql)
            utils.exec_tidb_sql(conn, tidb_sql)
            duration = time.time() - start
        except Exception as e:
            err_msg = str(e)
            logger.error(f'{query_id} error')
    
        if len(impala_sql) > 1e6 or len(tidb_sql) > 1e6:
            impala_sql = tidb_sql = 'sql too large to store here'
        sql = 'insert into test.translate_test (query_id, tenant, db, impala_start_time, impala_sql, impala_duration, tidb_sql, tidb_duration, success, err_msg)' +\
            f'values("{query_id}", "{tenant}", "{db}", "{impala_start_time}", "{escape_string(impala_sql)}", \
                    {impala_duration}, "{escape_string(tidb_sql)}", {duration}, {0 if err_msg else 1}, \
                    "{escape_string(err_msg)}")' +\
                'on duplicate key update tidb_sql=values(tidb_sql), tidb_duration=values(tidb_duration), success=values(success), err_msg=values(err_msg)'
            
        # logger.error(sql)
        utils.exec_tidb_sql(conn, sql)

        sql = f'update dp_stat.impala_query_log set processed = true where query_id = "{query_id}"'
        utils.exec_impala_sql(cursor, sql)

        err_msg_lower = err_msg.lower()
        if 'duplicate entry' in err_msg_lower and 'insert into' in err_msg_lower:
            mark_ignore(conn, query_id)
        if "'null'" in err_msg_lower:
            mark_not_support(conn, query_id)
        count_one(total)
    finally:
        cursor.close()
        conn.close()

def test_batch(batch_size):
    sql = f"SELECT query_id, start_time, duration FROM dp_stat.impala_query_log where processed = false and start_time >= '2022-4-22' and start_time < '2022-4-29' limit {batch_size}"
    cursor = utils.get_impala_cursor()
    cursor.execute(sql)
    df = as_pandas(cursor)
    logger.info(f'get {len(df)} sqls')
    cursor.close()
    global finish_count
    finish_count = 0
    pool = ThreadPoolExecutor(max_workers=utils.thread_count)
    for i in range(len(df)):
        pool.submit(test_one, df.at[i, 'query_id'], df.at[i, 'start_time'].strftime('%Y-%m-%d %H:%M:%S'), df.at[i, 'duration'], batch_size)
    pool.shutdown(wait=True)
    return len(df)

def re_run_error_sql(conn, id, query_id):
    try:
        # 接口不稳定，偶发报错，跳过
        tenant, db, impala_sql, tidb_sql = get_sql_text(query_id)
    except:
        return False
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

    sql = f'update test.translate_test set tidb_sql="{escape_string(tidb_sql)}", tidb_duration={duration}, \
        success = {0 if err_msg else 1}, err_msg="{escape_string(err_msg)}" where id={id}'
    # logger.error(sql)
    utils.exec_tidb_sql(conn, sql)

    err_msg_lower = err_msg.lower()
    if 'duplicate entry' in err_msg_lower and 'insert into' in err_msg_lower:
        mark_ignore(conn, query_id)

    return err_msg == ''   

def re_run_error_sql_batch(start_id=0):
    batch_size = 100
    conn = utils.get_tidb_conn()
    sql = f'select id, query_id from test.`translate_error` where id > {start_id} order by id limit {batch_size}'
    df = utils.get_tidb_data(conn, sql)
    total_count = len(df)
    success_count = 0
    if total_count > 0:
        logger.info(f'get {total_count} sqls')
    last_id = 0
    for i in range(total_count):
        last_id = df.at[i, 'id']
        query_id = df.at[i, 'query_id']
        if re_run_error_sql(conn, last_id, query_id):
            success_count += 1
    return last_id, total_count, success_count

def run_new():
    batch_size = 1000
    execute_count = test_batch(batch_size)
    total_count = execute_count
    while execute_count > 0:
        logger.info(f'total finished: {total_count}')
        execute_count = test_batch(1000)
        total_count += execute_count

def run_err():
    last_id, total_count, success_count = re_run_error_sql_batch()
    while last_id > 0:
        last_id, c1, c2 = re_run_error_sql_batch(last_id)
        total_count += c1
        success_count += c2
    logger.error(f'run error sqls finish, {success_count} / {total_count} success now')


if __name__ == '__main__':
    verb = ''
    if len(sys.argv) == 2:
        verb = sys.argv[1]
    if verb == 'run_new':
        run_new()
    elif verb == 'run_err':
        run_err()
    else:
        logger.error('param shoud be: run_new / run_err')
