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
def run_new_one(query_id, hash_id, impala_start_time, impala_duration, total):
    conn = utils.get_tidb_conn()
    cursor = utils.get_impala_cursor()
    try:
        try:
            # 接口不稳定，偶发报错，跳过
            tenant, db, impala_sql, tidb_sql = get_sql_text(query_id)
            # 忽略一些sql
            if '_parquet_' in impala_sql or db == 'ai' or 'NotsupportFunctionError' in tidb_sql \
                or 'NDV(' in impala_sql or 'background:true' in impala_sql:
                sql = f'update dp_stat.impala_query_log set processed = true where query_id = "{query_id}"'
                utils.exec_impala_sql(cursor, sql)
                logger.info('skip one')
                count_one(total)
                return
        except Exception as e:
            logger.error(f'api error once {query_id}')
            # logger.error(str(e))
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
        sql = 'insert into test.translate_test (query_id, hash_id, tenant, db, impala_start_time, impala_sql, impala_duration, tidb_sql, tidb_duration, success, err_msg)' +\
            f'values("{query_id}", "{hash_id}", "{tenant}", "{db}", "{impala_start_time}", "{escape_string(impala_sql)}", \
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

def run_new_batch(batch_size):
    sql = f"SELECT query_id, hash_id, start_time, duration FROM dp_stat.impala_query_log \
            where processed = false and `user` not in ('etl', 'tableau') and start_time >= '2022-4-22' and start_time < '2022-4-29' \
            limit {batch_size}"
    cursor = utils.get_impala_cursor()
    cursor.execute(sql)
    df = as_pandas(cursor)
    logger.info(f'get {len(df)} sqls')
    cursor.close()
    global finish_count
    finish_count = 0
    pool = ThreadPoolExecutor(max_workers=utils.thread_count)
    for i in range(len(df)):
        pool.submit(run_new_one, df.at[i, 'query_id'], df.at[i, 'hash_id'], 
            df.at[i, 'start_time'].strftime('%Y-%m-%d %H:%M:%S'), df.at[i, 'duration'], batch_size)
    pool.shutdown(wait=True)
    return len(df)

def run_error_one(conn, id, query_id):
    try:
        # 接口不稳定，偶发报错，跳过
        tenant, db, impala_sql, tidb_sql = get_sql_text(query_id)
    except Exception as e:
        logger.error(f'api error: {str(e)}')
        time.sleep(1)
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

    if len(tidb_sql) > 1e6:
        tidb_sql = 'sql too large to store here'
    sql = f'update test.translate_test set tidb_sql="{escape_string(tidb_sql)}", tidb_duration={duration}, \
        success = {0 if err_msg else 1}, err_msg="{escape_string(err_msg)}" where id={id}'
    # logger.error(sql)
    utils.exec_tidb_sql(conn, sql)

    err_msg_lower = err_msg.lower()
    if 'duplicate entry' in err_msg_lower and 'insert into' in err_msg_lower:
        mark_ignore(conn, query_id)

    return err_msg == ''

def run_error_batch(batch_size, start_id=0):
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
        if run_error_one(conn, last_id, query_id):
            success_count += 1
    return last_id, total_count, success_count

@utils.thread_method
def run_slow_one(id, query_id, total):
    try:
        # 接口不稳定，偶发报错，跳过
        tenant, db, impala_sql, tidb_sql = get_sql_text(query_id)
    except Exception as e:
        logger.error(f'api error: {str(e)}')
        time.sleep(1)
        return
    try:
        conn = utils.get_tidb_conn()
        duration = 0
        err_msg = ''
        if db != 'default':
            utils.exec_tidb_sql(conn, f'use {db}')
        start = time.time()
        utils.exec_tidb_sql(conn, tidb_sql)
        duration = time.time() - start
    except Exception as e:
        err_msg = str(e)
    count_one(total)
    err_msg_lower = err_msg.lower()
    if 'duplicate entry' in err_msg_lower and 'insert into' in err_msg_lower:
        sql = f'update test.translate_test set slow_re_run=1 where id={id}'
        utils.exec_tidb_sql(conn, sql)
    elif err_msg:
        sql = f'update test.translate_test set tidb_sql="{escape_string(tidb_sql)}", success=0, err_msg="{escape_string(err_msg)}" where id={id}'
        utils.exec_tidb_sql(conn, sql)
        logger.error(f'{id} error')
    else:
        sql = f'update test.translate_test set tidb_sql="{escape_string(tidb_sql)}", tidb_duration={duration}, slow_re_run=1 where id={id}'
        utils.exec_tidb_sql(conn, sql)

def run_slow_batch(batch_size, start_id=0):
    sql = f'select id, query_id from test.`translate_test` \
            where id > {start_id} and tidb_duration > impala_duration and tidb_duration > 2 \
                and status is null and success = 1 and slow_re_run=0\
            order by id limit {batch_size}'
    conn = utils.get_tidb_conn()
    df = utils.get_tidb_data(conn, sql)
    logger.info(f'get {len(df)} sqls')
    conn.close()
    global finish_count
    finish_count = 0
    last_id = 0
    pool = ThreadPoolExecutor(max_workers=utils.thread_count)
    for i in range(len(df)):
        last_id = df.at[i, 'id']
        pool.submit(run_slow_one, last_id, df.at[i, 'query_id'], batch_size)
    pool.shutdown(wait=True)
    return len(df), last_id

def run_new():
    batch_size = 1000
    execute_count = run_new_batch(batch_size)
    total_count = execute_count
    while execute_count > 0:
        logger.info(f'total finished: {total_count}')
        execute_count = run_new_batch(batch_size)
        total_count += execute_count

def run_err():
    batch_size = 100
    last_id, total_count, success_count = run_error_batch(batch_size)
    while last_id > 0:
        last_id, c1, c2 = run_error_batch(batch_size, last_id)
        total_count += c1
        success_count += c2
    logger.error(f'run error sqls finish, {success_count} / {total_count} success now')

def run_slow():
    batch_size = 1000
    execute_count, last_id = run_slow_batch(batch_size)
    total_count = execute_count
    while execute_count > 0:
        logger.info(f'total finished: {total_count}')
        execute_count, last_id = run_slow_batch(batch_size, last_id)
        total_count += execute_count

if __name__ == '__main__':
    verb = ''
    if len(sys.argv) == 2:
        verb = sys.argv[1]
    if verb == 'run_new':
        run_new()
    elif verb == 'run_err':
        run_err()
    elif verb == 'run_slow':
        run_slow()
    else:
        logger.error('param shoud be: run_new / run_err / run_slow')
