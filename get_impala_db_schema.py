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

def get_table_schema(db, table, cursor):
    cursor.execute(f'describe {db}.{table}')
    df = as_pandas(cursor)
    primary_keys = []
    columns = []
    str_columns = []
    for i in range(len(df)):
        name = f'`{df.at[i, "name"]}`'
        type = df.at[i, 'type']
        if type == 'string':
            str_columns.append(name)
        nullable = df.at[i, 'nullable']
        default_value = df.at[i, 'default_value']
        primary_key = df.at[i, 'primary_key']
        if primary_key == 'true':
            primary_keys.append(name)
        column_schema = {
            'name': name,
            'type': type,
            'len': 0,
            'nullable': nullable,
            'default_value': default_value
        }
        columns.append(column_schema)
    if str_columns:
        sql = f'/*& global:true*/ select {",".join([f"ifnull(max(length({col})), 0) as {col}" for col in str_columns])} from {db}.{table}'
        cursor.execute(sql)
        df = as_pandas(cursor)
        for i in range(len(df.columns)):
            name = df.columns[i]
            value = df.at[0, name]
            for cs in columns:
                if cs['name'] == name:
                    cs['len'] = int(value)
    return {
        'table': f'`{db}`.`{table}`', 
        'columns': columns,
        'primary_keys': ','.join(primary_keys)
        }

@utils.thread_method
def get_db_schema(db, total_count):
    if not hasattr(thread_context, 'cursor'):
        thread_context.cursor = utils.get_impala_cursor()
    start = time.time()
    tables = utils.get_tables_in_impala_db(db, thread_context.cursor)
    table_schemas = []
    for table in tables:
        ts = get_table_schema(db, table, thread_context.cursor)
        table_schemas.append(ts)
    text = json.dumps(table_schemas, indent=2, ensure_ascii=False)
    with open(f'schemas/{db}.json', 'w', encoding='utf-8') as f:
        f.write(text)
    time_used = time.time() - start

    global finish_count
    lock.acquire()
    finish_count += 1
    logger.info('(%d / %d) %s finish in %.1f seconds' % (finish_count, total_count, db, time_used))
    lock.release()

def main():
    start = time.time()
    special_dbs = ['public_data']
    cursor = utils.get_impala_cursor()
    cursor.execute('show databases')
    threads = utils.conf.getint('sys', 'threads', fallback=1)
    dbs = []
    df = as_pandas(cursor)
    for i in range(len(df)):
        db = df.at[i, 'name']
        if db.startswith('global_') or db.startswith('asset_') or db.endswith('_custom') or db in special_dbs:
            dbs.append(db)
    # dbs = ['cr19_custom']
    pool = ThreadPoolExecutor(max_workers=threads)
    for db in dbs:
        pool.submit(get_db_schema, db, len(dbs))
    pool.shutdown(wait=True)
    logger.info('finish in %.1f seconds' % (time.time() - start))

if __name__ == '__main__':
    main()