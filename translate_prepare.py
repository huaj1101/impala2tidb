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

def count_one(total):
    global finish_count
    lock.acquire()
    finish_count += 1
    if finish_count % 100 == 0:
        logger.info(' %d / %d finish' % (finish_count, total))
    lock.release()

def save_big_sql(query_id, sql):
    logger.info(f'save big sql: {query_id}')
    with open(f'big_sqls/{query_id.replace(":", "-")}.sql', 'w', encoding='utf-8') as f:
        f.write(sql)

@utils.thread_method
def run_one(query_id, hash_id, impala_duration, total):
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
                # logger.info('skip one')
                count_one(total)
                return
        except Exception as e:
            logger.error(f'api error once {query_id}')
            # logger.error(str(e))
            count_one(total)
            time.sleep(2)
            return
    
        big_sql = ''
        if len(impala_sql) > 1e6 or len(tidb_sql) > 1e6:
            big_sql = tidb_sql
            impala_sql = tidb_sql = ''
        sql = 'insert into test.translate_sqls (query_id, hash_id, tenant, db, impala_sql, impala_duration, tidb_sql)' +\
            f'values("{query_id}", "{hash_id}", "{tenant}", "{db}", "{escape_string(impala_sql)}", \
                    {impala_duration}, "{escape_string(tidb_sql)}")' +\
                'on duplicate key update tidb_sql=values(tidb_sql)'
        try:
            utils.exec_tidb_sql(conn, sql)
            if big_sql:
                save_big_sql(query_id, big_sql)

            sql = f'update dp_stat.impala_query_log set processed = true where query_id = "{query_id}"'
            utils.exec_impala_sql(cursor, sql)
            count_one(total)
        except Exception as e:
            logger.error(f'save error: {query_id} \n {e}')
            count_one(total)
            time.sleep(2)
    finally:
        cursor.close()
        conn.close()

def run_batch(batch_size):
    sql = f"SELECT query_id, hash_id, duration FROM dp_stat.impala_query_log \
            where processed = false and `user` not in ('etl', 'tableau') and start_time >= '2022-4-22' and start_time < '2022-4-29' \
            limit {batch_size}"
    try:
        cursor = utils.get_impala_cursor()
        cursor.execute(sql)
        df = as_pandas(cursor)
        logger.info(f'get {len(df)} sqls')
        cursor.close()
    except:
        time.sleep(2)
        return 0
    global finish_count
    finish_count = 0
    pool = ThreadPoolExecutor(max_workers=utils.thread_count)
    for i in range(len(df)):
        pool.submit(run_one, df.at[i, 'query_id'], df.at[i, 'hash_id'], df.at[i, 'duration'], batch_size)
    pool.shutdown(wait=True)
    return len(df) if len(df) > 0 else -1

def run():
    batch_size = 1000
    execute_count = run_batch(batch_size)
    total_count = execute_count
    while execute_count >= 0:
        logger.info(f'total finished: {total_count}')
        execute_count = run_batch(batch_size)
        if execute_count > 0:
            total_count += execute_count

if __name__ == '__main__':
    run()
