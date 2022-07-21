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
def truncate_table(db, table, total_count):
    sql = f'TRUNCATE TABLE {db}.{table}'
    with utils.get_tidb_conn() as conn:
        conn.execute(sql)
    
    global finish_count
    lock.acquire()
    finish_count += 1
    if finish_count % 10 == 0:
        logger.info('%d / %d truncate' % (finish_count, total_count))
    lock.release()

@utils.thread_method
def analyze_table(db, table, total_count):
    sql = f'ANALYZE TABLE {db}.{table}'
    with utils.get_tidb_conn() as conn:
        conn.execute(sql)
    
    global finish_count
    lock.acquire()
    finish_count += 1
    if finish_count % 10 == 0:
        logger.info('%d / %d analyze' % (finish_count, total_count))
    lock.release()

@utils.thread_method
def set_tiflash(db, table, total_count):
    sql = f'ALTER TABLE {db}.{table} SET TIFLASH REPLICA 3'
    with utils.get_tidb_conn() as conn:
        conn.execute(sql)
    
    global finish_count
    lock.acquire()
    finish_count += 1
    if finish_count % 10 == 0:
        logger.info('%d / %d set tiflash' % (finish_count, total_count))
    lock.release()

@utils.thread_method
def add_version(db, table, total_count):
    sql = f'ALTER TABLE {db}.{table} ADD COLUMN __version bigint(20) NOT NULL DEFAULT MCTECH_SEQUENCE ON UPDATE MCTECH_SEQUENCE'
    with utils.get_tidb_conn() as conn:
        conn.execute(sql)
    
    global finish_count
    lock.acquire()
    finish_count += 1
    if finish_count % 10 == 0:
        logger.info('%d / %d add version' % (finish_count, total_count))
    lock.release()

def get_tables_from_schemas_dir():
    result = []
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
            for table_schema in db_schema:
                table_full_name = table_schema['table']
                result.append(table_full_name.split('.'))
    return result

def get_tables(db):
    if db == 'from_schemas_dir':
        return get_tables_from_schemas_dir()
    result = []
    with utils.get_tidb_conn() as conn:
        tables = utils.get_tables_in_tidb_db(db, conn)
        for table in tables:
            result.append((db, table))
    return result

@utils.timeit
def run(db, action):
    db_and_tables = get_tables(db)
    if action == 'truncate':
        func = truncate_table
    elif action == 'tiflash':
        func = set_tiflash
    elif action == 'analyze':
        func = analyze_table
    elif action == 'add_version':
        func = add_version
    else:
        raise Exception(f'unsupport param: {db} {action}')
    
    pool = ThreadPoolExecutor(max_workers=10)
    global finish_count
    finish_count = 0
    for db, table in db_and_tables:
        pool.submit(func, db, table, len(db_and_tables))
    pool.shutdown(wait=True)

if __name__ == '__main__':
    if len(sys.argv) == 3:
        db = sys.argv[1]
        action = sys.argv[2]
    else:
        logger.error('param shoud be: db_name/from_schemas_dir truncate/tiflash/analyze/add_version')
        sys.exit(1)
    run(db, action)
