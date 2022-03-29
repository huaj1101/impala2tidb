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

logger = logging.getLogger(__name__)

thread_context = threading.local()
sql_times = {}
lock = threading.Lock()
finish_count = 0
dbs_created = set()

@utils.thread_method
def copy_to_text_table(db, table_schema, total_count):
    start = time.time()
    if not hasattr(thread_context, 'cursor'):
        thread_context.cursor = utils.get_impala_cursor()
    text_db = db + '_text'
    lock.acquire()
    if text_db not in dbs_created:
        thread_context.cursor.execute(f'drop database if exists {text_db} cascade')
        thread_context.cursor.execute(f'create database {text_db}')
        time.sleep(2)
        dbs_created.add(text_db)
    lock.release()

    ori_table = table_schema["table"]
    new_table = text_db + '.' + ori_table.split('.')[1]
    sql = f'/*& global:true*/ create table {new_table} stored as textfile as select * from {ori_table}'
    thread_context.cursor.execute(sql)
    thread_context.cursor.execute(f'refresh {new_table}')

    time_used = time.time() - start

    global finish_count
    lock.acquire()
    finish_count += 1
    logger.info('(%d / %d) %s finish in %.1f seconds' % (finish_count, total_count, ori_table, time_used))
    lock.release()

@utils.thread_method
def download_table_csv(db, table, total_count):
    start = time.time()
    if not hasattr(thread_context, 'hdfsclient'):
        thread_context.hdfsclient = utils.get_hdfs_client()
    hdfs_path = f'/user/hive/warehouse/{db}.db/{table}'
    csv_files = []
    for item in thread_context.hdfsclient.list(hdfs_path):
        if item.endswith('.'):
            csv_files.append(item)
    csv_files.sort()
    for i, file in enumerate(csv_files):
        hdfs_file = f'/user/hive/warehouse/{db}.db/{table}/{file}'
        local_file = f'data/{db.replace("_text", "")}.{table}.{i+1:0>3d}.csv'
        thread_context.hdfsclient.download(hdfs_file, local_file)

    time_used = time.time() - start
    global finish_count
    lock.acquire()
    finish_count += 1
    logger.info('(%d / %d) %s.%s finish in %.1f seconds' % (finish_count, total_count, db, table, time_used))
    lock.release()

def main():
    start = time.time()
    files = []
    for file in os.listdir('schemas/'):
        if file.endswith('.json'):
            files.append(file)
    files.sort()
    files = ['global_platform.json']
    threads = utils.conf.getint('sys', 'threads', fallback=1)
    pool = ThreadPoolExecutor(max_workers=threads)
    table_schemas = []
    for file in files:
        db = file.replace('.json', '')
        with open(f'schemas/{file}', 'r', encoding='utf-8') as f:
            schema_text = f.read()
            db_schema = json.loads(schema_text)
            table_schemas.extend(db_schema)

    # logger.info('copy_to_text_table')
    # for table_schema in table_schemas:
    #     db = table_schema['table'].split('.')[0].strip('`')
    #     pool.submit(copy_to_text_table, db, table_schema, len(table_schemas))
    # pool.shutdown(wait=True)

    logger.info('download_table_csv')
    for table_schema in table_schemas:
        db = table_schema['table'].split('.')[0].strip('`') + '_text'
        table = table_schema['table'].split('.')[1].strip('`')
        pool.submit(download_table_csv, db, table, len(table_schemas))
    pool.shutdown(wait=True)

    logger.info('finish in %.1f seconds' % (time.time() - start))


if __name__ == '__main__':
    main()