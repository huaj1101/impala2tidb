from asyncore import read
from email.policy import default
import os
from re import S
import sys
import logging
from textwrap import indent
import threading
from impala.util import as_pandas
import time
from concurrent.futures import ThreadPoolExecutor
import utils
import json

logger = logging.getLogger(__name__)

sql_times = {}
lock = threading.Lock()
finish_count = 0

@utils.thread_method
def truncate_table_tidb(table_schema, total_count):
    sql = f'truncate {table_schema["table"]}'
    with utils.get_tidb_conn() as conn:
        conn.execute(sql)
    
    global finish_count
    lock.acquire()
    finish_count += 1
    if finish_count % 10 == 0:
        logger.info('%d / %d truncate' % (finish_count, total_count))
    lock.release()

@utils.thread_method
def analyze_table_tidb(table_schema, total_count):
    sql = f'ANALYZE TABLE {table_schema["table"]}'
    with utils.get_tidb_conn() as conn:
        conn.execute(sql)
    
    global finish_count
    lock.acquire()
    finish_count += 1
    if finish_count % 10 == 0:
        logger.info('%d / %d ANALYZE' % (finish_count, total_count))
    lock.release()

@utils.thread_method
def analyze_table_impala(table_schema, total_count):
    sql = f'compute stats {table_schema["table"]}'
    with utils.get_impala_cursor() as cursor:
        cursor.execute(sql)
    
    global finish_count
    lock.acquire()
    finish_count += 1
    if finish_count % 10 == 0:
        logger.info('%d / %d compute stats' % (finish_count, total_count))
    lock.release()

@utils.timeit
def run(engine, action):
    if engine == 'impala' and action == 'analyze':
        func = analyze_table_impala
    elif engine == 'tidb' and action == 'analyze':
        func = analyze_table_tidb
    # elif engine == 'tidb' and action == 'truncate':
    #     func = truncate_table_tidb
    else:
        raise Exception(f'unsupport param: {engine} {action}')
    files = []
    for file in os.listdir('schemas/'):
        if file.endswith('.json'):
            files.append(file)
    files.sort()
    # files = ['global_dw_1.json', 'global_dwb.json']
    tables_schema = []
    for file in files:
        with open(f'schemas/{file}', 'r', encoding='utf-8') as f:
            schema_text = f.read()
            db_schema = json.loads(schema_text)
            tables_schema.extend(db_schema)
    
    pool = ThreadPoolExecutor(max_workers=10)
    for table_schema in tables_schema:
        pool.submit(func, table_schema, len(tables_schema))
    pool.shutdown(wait=True)

if __name__ == '__main__':
    if len(sys.argv) == 3:
        engine = sys.argv[1]
        action = sys.argv[2]
    else:
        logger.error('param shoud be: impala/tidb truncate/analyze')
        sys.exit(1)
    run(engine, action)
