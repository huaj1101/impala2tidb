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
def get_table_data(table_schema, total_count):
    start = time.time()
    if not hasattr(thread_context, 'hdfsclient'):
        thread_context.hdfsclient = utils.get_hdfs_client()
    db = table_schema['table'].split('.')[0].strip('`')
    text_db = db + '_text'
    table = table_schema['table'].split('.')[1].strip('`')
    # if table_schema['type'] == 'kudu':
    #     return
    cursor = utils.get_impala_cursor()
    lock.acquire()
    if text_db not in dbs_created:
        cursor.execute(f'drop database if exists {text_db} cascade')
        cursor.execute(f'create database {text_db}')
        time.sleep(2)
        dbs_created.add(text_db)
    lock.release()

    # 拷贝到textfile格式的表中（以便产生csv）
    cols = [col['name'] for col in table_schema['columns']]
    sql = f'create table {text_db}.{table} ' +\
        'ROW FORMAT DELIMITED FIELDS TERMINATED BY "\u0006" ESCAPED BY "\\\\" LINES TERMINATED BY "\u0007" ' +\
        'stored as textfile ' +\
        f'as select {", ".join(cols)} from {db}.{table}'
    utils.exec_sql(cursor, sql)
    cursor.execute(f'refresh {text_db}.{table}')
    cursor.close()

    # 拷贝csv到本地
    hdfs_path = f'/user/hive/warehouse/{text_db}.db/{table}'
    csv_files = []
    for item in thread_context.hdfsclient.list(hdfs_path):
        if item.endswith('.'):
            csv_files.append(item)
    csv_files.sort()
    for i, file in enumerate(csv_files):
        hdfs_file = f'/user/hive/warehouse/{text_db}.db/{table}/{file}'
        local_file = f'data/{db}.{table}.{i+1:0>3d}.csv'
        thread_context.hdfsclient.download(hdfs_file, local_file, overwrite=True)

    # 删除textfile表（节省空间）
    cursor = utils.get_impala_cursor()
    cursor.execute(f'drop table {text_db}.{table} PURGE')
    cursor.close()

    time_used = time.time() - start

    global finish_count
    lock.acquire()
    finish_count += 1
    logger.info('(%d / %d) %s.%s finish in %.1f seconds' % (finish_count, total_count, db, table, time_used))
    lock.release()

@utils.timeit
def main():
    files = []
    for file in os.listdir('schemas/'):
        if file.endswith('.json'):
            files.append(file)
    files.sort()
    # files = ['global_dw_1.json', 'global_dw_2.json']
    pool = ThreadPoolExecutor(max_workers=utils.thread_count)
    table_schemas = []
    for file in files:
        db = file.replace('.json', '')
        with open(f'schemas/{file}', 'r', encoding='utf-8') as f:
            schema_text = f.read()
            db_schema = json.loads(schema_text)
            table_schemas.extend(db_schema)

    logger.info('get_table_data')
    for table_schema in table_schemas:
        pool.submit(get_table_data, table_schema, len(table_schemas))
    pool.shutdown(wait=True)

    logger.info('drop text databases')
    cursor = utils.get_impala_cursor()
    for db in dbs_created:
        logger.info(f'drop {db} ...')
        cursor.execute(f'drop database {db} cascade')
    cursor.close()

if __name__ == '__main__':
    main()