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
import random
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
    utils.exec_impala_sql(cursor, sql, {'KUDU_READ_MODE': 'READ_LATEST'})
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
    cursor.close()
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

def get_table_csv_size(table):
    size = 0
    for file in os.listdir('data_split'):
        if file.startswith(f'{table}.') and file.endswith('.csv'):
            file_size = os.path.getsize(f'data_split/{file}')
            size += file_size
    return size

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
        csv_size = get_table_csv_size(table)
        cmd = f'ssh tidb@10.200.40.8 -C "echo {csv_size} > /csv-data1/csv/{table}.finish"'
        os.system(cmd)

        global scp_finish_count
        lock.acquire()
        scp_finish_count += 1
        logger.info(f'{scp_finish_count} tables scp finish, {finished_tables.qsize()} waiting')
        lock.release()

def get_all_table_schemas():
    dbs = []
    table_schemas = []
    files = []
    for file in os.listdir('schemas/'):
        if file.endswith('.json'):
            files.append(file)
            dbs.append(file.replace('.json', ''))
    # files = ['global_ipm.json']
    files.sort()
    dbs.sort()
    for file in files:
        with open(f'schemas/{file}', 'r', encoding='utf-8') as f:
            schema_text = f.read()
            db_schema = json.loads(schema_text)
            table_schemas.extend(db_schema)
    return dbs, table_schemas

def get_mismatch_table_schemas():
    with open('mismatch_tables.txt', 'r') as f:
        lines = f.readlines()
    lines.sort()
    dbs = []
    db_schemas = {}
    table_schemas = []
    for line in lines:
        db, table = line.strip('\n').split('.')
        dbs.append(db)
        if db not in db_schemas:
            file = f'schemas/{db}.json'
            with open(f'{file}', 'r', encoding='utf-8') as f:
                schema_text = f.read()
            db_schemas[db] = json.loads(schema_text)
        for table_schema in db_schemas[db]:
            if table_schema['table'] == f'`{db}`.`{table}`':
                table_schemas.append(table_schema)
                break
    return dbs, table_schemas

def do_prepare_tables():
    dbs, table_schemas = get_all_table_schemas()
    cursor = utils.get_impala_cursor()
    for db in dbs:
        sql = f'create database if not exists {db}_text'
        logger.info(sql)
        cursor.execute(sql)
    cursor.close()
    pool = ThreadPoolExecutor(max_workers=10)
    for table_schema in table_schemas:
        pool.submit(prepare_table, table_schema, len(table_schemas))
    pool.shutdown(wait=True)
    clean_hdfs_trash()

def do_get_data(only_mismatch):
    # scp的线程
    scp_pool = ThreadPoolExecutor(max_workers=3)
    for i in range(3):
        scp_pool.submit(scp_files)
    
    dbs, table_schemas = get_mismatch_table_schemas() if only_mismatch else get_all_table_schemas()

    table_schemas.sort(key=lambda item: item['record_count'], reverse=True)
    big_tables = table_schemas[:10]
    other_tables = table_schemas[10:]
    random.shuffle(other_tables)

    logger.info(f'get big tables data start, count: {len(big_tables)}')
    get_data_pool_big = ThreadPoolExecutor(max_workers=3)
    for table_schema in big_tables:
        get_data_pool_big.submit(get_table_data, table_schema, len(table_schemas))

    logger.info(f'get other tables data start, count: {len(other_tables)}')
    get_data_pool = ThreadPoolExecutor(max_workers=7)
    for table_schema in other_tables:
        get_data_pool.submit(get_table_data, table_schema, len(table_schemas))

    get_data_pool_big.shutdown(wait=True)
    get_data_pool.shutdown(wait=True)
    clean_hdfs_trash()

    global csv_all_ready
    csv_all_ready = True
    if scp_pool:
        scp_pool.shutdown(wait=True)

@utils.timeit
def main(action):
    if action == 'clean':
        do_clean_text_dbs()
    elif action == 'prepare':
        do_prepare_tables()
    else:
        do_get_data(action == 'mismatch')

if __name__ == '__main__':
    if len(sys.argv) == 2 and sys.argv[1] in ('clean', 'prepare', 'run', 'mismatch'):
        action = sys.argv[1]
    else:
        logger.error('param shoud be: clean / prepare / run / mismatch')
        sys.exit(1)
    main(action)
