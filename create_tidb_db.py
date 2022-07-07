from asyncore import read
from email.policy import default
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
import translate_utils

logger = logging.getLogger(__name__)

sql_times = {}
lock = threading.Lock()
finish_count = 0

def create_one_table(table_schema, tidb_conn):
    sql = translate_utils.gen_create_table_sql(table_schema)
    tidb_conn.execute(sql)
    tidb_conn.execute(f'ALTER TABLE {table_schema["table"]} SET TIFLASH REPLICA 3')

@utils.thread_method
def recreate_one_db(db, db_schema, total_count):
    start = time.time()
    with utils.get_tidb_conn() as conn:
        conn.execute(f'DROP DATABASE IF EXISTS {db}')
        conn.execute(f'CREATE DATABASE {db}')
        for table_schema in db_schema:
            create_one_table(table_schema, conn)
        # break
    time_used = time.time() - start
    global finish_count
    lock.acquire()
    finish_count += 1
    logger.info('(%d / %d) %s finish in %.1f seconds' % (finish_count, total_count, db, time_used))
    lock.release()

@utils.thread_method
def drop_one_db(db, db_schema, total_count):
    start = time.time()
    with utils.get_tidb_conn() as conn:
        conn.execute(f'DROP DATABASE IF EXISTS {db}')
    time_used = time.time() - start
    global finish_count
    lock.acquire()
    finish_count += 1
    logger.info('(%d / %d) %s finish in %.1f seconds' % (finish_count, total_count, db, time_used))
    lock.release()

@utils.thread_method
def recreate_mismatch_table(table_schema, total_count):
    start = time.time()
    db = table_schema['table'].split('.')[0]
    table_fullname = table_schema["table"]

    with utils.get_tidb_conn() as conn:
        conn.execute(f'CREATE DATABASE IF NOT EXISTS {db}')
        conn.execute(f'DROP TABLE IF EXISTS {table_fullname}')
        create_one_table(table_schema, conn)

    time_used = time.time() - start
    global finish_count
    lock.acquire()
    finish_count += 1
    logger.info('(%d / %d) %s finish in %.1f seconds' % (finish_count, total_count, table_fullname, time_used))
    lock.release()

@utils.timeit
def run_mismatch():
    with open('mismatch_tables.txt', 'r') as f:
        lines = f.readlines()
    lines.sort()
    db_schemas = {}
    table_schemas = []
    for line in lines:
        db, table = line.strip('\n').split('.')
        if db not in db_schemas:
            file = f'schemas/{db}.json'
            with open(f'{file}', 'r', encoding='utf-8') as f:
                schema_text = f.read()
            db_schemas[db] = json.loads(schema_text)
        for table_schema in db_schemas[db]:
            if table_schema['table'] == f'`{db}`.`{table}`':
                table_schemas.append(table_schema)
                break
    pool = ThreadPoolExecutor(max_workers=15)
    for table_schema in table_schemas:
        pool.submit(recreate_mismatch_table, table_schema, len(table_schemas))
    pool.shutdown(wait=True)

def add_index():
    with utils.get_tidb_conn() as conn:
        logger.info('add index to global_mtlp.q_piece')
        conn.execute('alter table global_mtlp.q_piece add index (tenant, org_id, schedule_id, pro_line, piece_id)')
        logger.info('add index to global_mtlp.q_dosage')
        conn.execute('alter table global_mtlp.q_dosage add index (tenant, org_id, dosage_id, schedule_id, piece_id, pro_line)')
        logger.info('add index to global_mtlp.q_produce')
        conn.execute('alter table global_mtlp.q_produce add index (tenant, org_id, schedule_id, pro_line)')
        logger.info('add index to global_mtlp.m_gh_plan_check')
        conn.execute('alter table global_mtlp.m_gh_plan_check add index (tenant, org_id, ori_gh_id)')
        logger.info('add index to global_mtlp.q_inventory')
        conn.execute('alter table global_mtlp.q_inventory add index (tenant, org_id, item_bar_code)')

@utils.timeit
def run(action):
    files = []
    for file in os.listdir('schemas/'):
        if file.endswith('.json'):
            files.append(file)
    files.sort()
    # files = ['global_mtlp.json']
    func = drop_one_db if action == 'drop' else recreate_one_db
    pool = ThreadPoolExecutor(max_workers=15)
    for file in files:
        db = file.replace('.json', '')
        with open(f'schemas/{file}', 'r', encoding='utf-8') as f:
            schema_text = f.read()
            db_schema = json.loads(schema_text)
        pool.submit(func, db, db_schema, len(files))
    pool.shutdown(wait=True)
    # if action == 'recreate':
    #     add_index()

if __name__ == '__main__':
    action = ''
    if len(sys.argv) == 2:
        action = sys.argv[1]
    if action in ('drop', 'recreate'):
        run(action)
    elif action == 'mismatch':
        run_mismatch()
    else:
        logger.error('param shoud be: drop / recreate / mismatch')

