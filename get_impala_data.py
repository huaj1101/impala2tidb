from ast import Assign
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
import split_csv
from queue import Queue

logger = logging.getLogger(__name__)

thread_context = threading.local()
sql_times = {}
lock = threading.Lock()
finish_count = 0
dbs_created = {}
finished_tables = Queue()
scp_finish_count = 0
csv_all_ready = False

def clean_hdfs_trash():
    cmd = 'sudo -u hdfs hadoop fs -expunge'
    os.system(cmd)

def wait_for_database_ok(cursor, db, timeout=20):
    start = time.time()
    while time.time() - start < timeout:
        cursor.execute(f'show databases like "{db}"')
        df = as_pandas(cursor)
        if len(df) == 1:
            return True
        time.sleep(1)
    return False

@utils.thread_method
def get_table_data(table_schema, total_count):
    start = time.time()
    if not hasattr(thread_context, 'hdfsclient'):
        thread_context.hdfsclient = utils.get_hdfs_client()

    db = table_schema['table'].split('.')[0].strip('`')
    text_db = db + '_text'
    table = table_schema['table'].split('.')[1].strip('`')
    cursor = utils.get_impala_cursor()

    # 拷贝到textfile格式的表中（以便产生csv）
    cols = []
    for col in table_schema['columns']:
        col_name = col["name"]
        if col['type'] == 'boolean':
            cols.append(f'cast({col_name} as int) as {col_name}')
        else:
            cols.append(col_name)
    logger.info(f'start table {db}.{table}')
    sql = f'insert into {text_db}.{table} select {", ".join(cols)} from {db}.{table}'
    utils.exec_impala_sql(cursor, sql)
    logger.info(f'finish copy table data {db}.{table}')
    # 拷贝csv到本地
    copy_start = time.time()
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
    if len(csv_files) > 0:
        logger.info(f'finish download csv {db}.{table}')
        # 拆分csv
        split_csv.split_files('data', 'data_split', f'{db}.{table}', remove_src=True)
        logger.info(f'finish split csv {db}.{table}')
        # 插入到完成队列
        finished_tables.put(f'{db}.{table}')
        # 时间长了cursor可能会超时关闭，重新创建一个
        if time.time() - copy_start > 10:
            cursor = utils.get_impala_cursor()
        # 删除textfile表（节省空间）
        cursor.execute(f'truncate table {text_db}.{table}')
        cursor.close()
    else:
        logger.info(f'no csv {db}.{table}')

    time_used = time.time() - start

    global finish_count
    lock.acquire()
    finish_count += 1
    if finish_count % 100 == 0 or finish_count == total_count:
        clean_hdfs_trash()
    logger.info('(%d / %d) %s.%s finish in %.1f seconds' % (finish_count, total_count, db, table, time_used))
    lock.release()

@utils.thread_method
def prepare_table(table_schema, total_count):
    start = time.time()

    db = table_schema['table'].split('.')[0].strip('`')
    text_db = db + '_text'
    table = table_schema['table'].split('.')[1].strip('`')
    cursor = utils.get_impala_cursor()
    need_wait_db = False
    lock.acquire()
    if text_db not in dbs_created:
        create_sql = f'create database if not exists {text_db}'
        logger.info(create_sql)
        cursor.execute(create_sql)
        wait_for_database_ok(cursor, text_db)
        dbs_created[text_db] = time.time()
    else:
        if time.time() - dbs_created[text_db] < 5:
            need_wait_db = True
    lock.release()

    if need_wait_db:
        wait_for_database_ok(cursor, text_db)

    cols = []
    for col in table_schema['columns']:
        col_name = col["name"]
        if col['type'] == 'boolean':
            cols.append(f'cast({col_name} as int) as {col_name}')
        else:
            cols.append(col_name)
    sql = f'create table if not exists {text_db}.{table} ' +\
        'ROW FORMAT DELIMITED FIELDS TERMINATED BY "\u0006" ESCAPED BY "\\\\" LINES TERMINATED BY "\u0007" ' +\
        'stored as textfile ' +\
        f'as select {", ".join(cols)} from {db}.{table} limit 1'
    utils.exec_impala_sql(cursor, sql)
    cursor.execute(f'refresh {text_db}.{table}')
    cursor.execute(f'truncate {text_db}.{table}')

    time_used = time.time() - start

    global finish_count
    lock.acquire()
    finish_count += 1
    if finish_count % 500 == 0 or finish_count == total_count:
        clean_hdfs_trash()
    logger.info('(%d / %d) %s.%s finish in %.1f seconds' % (finish_count, total_count, db, table, time_used))
    lock.release()

def do_clean_text_dbs():
    cursor = utils.get_impala_cursor()
    cursor.execute('show databases')
    df = as_pandas(cursor)
    for i in range(len(df)):
        db = df.at[i, 'name']
        if db.endswith('_text'):
            sql = f'drop database {db} cascade'
            cursor.execute(sql)
            logger.info(sql + ' done')
    cursor.close()
    clean_hdfs_trash()

def scp_files():
    while True:
        if finished_tables.qsize() == 0:
            if csv_all_ready:
                break
            time.sleep(0.1)
            continue
        table = finished_tables.get()
        cmd = f'scp data_split/{table}.* tidb@10.200.40.8://csv-data1/csv/'
        os.system(cmd)
        cmd = f'ssh tidb@10.200.40.8 -C "touch /csv-data1/csv/{table}.finish"'
        os.system(cmd)

        global scp_finish_count
        lock.acquire()
        scp_finish_count += 1
        logger.info(f'{scp_finish_count} tables scp finish, {finished_tables.qsize()} waiting')
        lock.release()

@utils.timeit
def main(action):
    if action == 'clean':
        do_clean_text_dbs()
        return

    files = []
    for file in os.listdir('schemas/'):
        if file.endswith('.json'):
            files.append(file)
    files.sort()
    # files = ['global_ipm.json']

    if action == 'prepare':
        func = prepare_table
        get_data_threads = 10
        scp_pool = None
    else:
        func = get_table_data
        get_data_threads = 6
        scp_pool = ThreadPoolExecutor(max_workers=3)
        for i in range(3):
            scp_pool.submit(scp_files)

    get_data_pool = ThreadPoolExecutor(max_workers=get_data_threads)
    table_schemas = []
    for file in files:
        with open(f'schemas/{file}', 'r', encoding='utf-8') as f:
            schema_text = f.read()
            db_schema = json.loads(schema_text)
            table_schemas.extend(db_schema)

    for table_schema in table_schemas:
        get_data_pool.submit(func, table_schema, len(table_schemas))
    get_data_pool.shutdown(wait=True)

    global csv_all_ready
    csv_all_ready = True
    if scp_pool:
        scp_pool.shutdown(wait=True)

if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] in ('clean', 'prepare', 'run'):
        action = sys.argv[1]
    else:
        logger.error('param shoud be: clean / prepare / run')
        sys.exit(1)
    main(action)