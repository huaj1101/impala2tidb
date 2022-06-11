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

thread_context = threading.local()
sql_times = {}
lock = threading.Lock()
finish_count = 0

def create_one_table(table_schema, tidb_conn):
    sql = translate_utils.gen_create_table_sql(table_schema)
    tidb_conn.execute(sql)
    tidb_conn.execute(f'ALTER TABLE {table_schema["table"]} SET TIFLASH REPLICA 3')

@utils.thread_method
def create_one_db(db, db_schema, total_count):
    start = time.time()
    if not hasattr(thread_context, 'tidb_conn'):
        thread_context.tidb_conn = utils.get_tidb_conn()
    thread_context.tidb_conn.execute(f'DROP DATABASE IF EXISTS {db}')
    thread_context.tidb_conn.execute(f'CREATE DATABASE {db}')
    for table_schema in db_schema:
        create_one_table(table_schema, thread_context.tidb_conn)
        # break
    time_used = time.time() - start
    global finish_count
    lock.acquire()
    finish_count += 1
    logger.info('(%d / %d) %s finish in %.1f seconds' % (finish_count, total_count, db, time_used))
    lock.release()

@utils.timeit
def main():
    files = []
    for file in os.listdir('schemas/'):
        if file.endswith('.json'):
            files.append(file)
    files.sort()
    # files = ['global_mtlp.json']
    pool = ThreadPoolExecutor(max_workers=15)
    for file in files:
        db = file.replace('.json', '')
        with open(f'schemas/{file}', 'r', encoding='utf-8') as f:
            schema_text = f.read()
            db_schema = json.loads(schema_text)
        pool.submit(create_one_db, db, db_schema, len(files))
    pool.shutdown(wait=True)

if __name__ == '__main__':
    main()

# alter table global_mtlp.q_piece add index (tenant, org_id, schedule_id, pro_line, piece_id)
# alter table global_mtlp.q_dosage add index (tenant, org_id, dosage_id, schedule_id, piece_id, pro_line)
# alter table global_mtlp.q_produce add index (tenant, org_id, schedule_id, pro_line)
# alter table global_mtlp.m_gh_plan_check add index (tenant, org_id, ori_gh_id)
# alter table global_mtlp.q_inventory add index (tenant, org_id, item_bar_code)