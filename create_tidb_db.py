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

logger = logging.getLogger(__name__)

thread_context = threading.local()
sql_times = {}
lock = threading.Lock()
finish_count = 0

def translate_str_length(length):
    if length <= 50:
        return 50
    if length <= 255:
        return 255
    if length <= 2000:
        return 2000
    if length <= 16383: # varchar类型最大长度
        return 16383
    if length <= 65535: # text类型最大长度
        return 65535
    return 4294967295 # longtext类型

def translate_type(impala_type, length):
    tidb_type = ''
    if impala_type == 'string':
        tidb_len = translate_str_length(length)
        if tidb_len <= 16383:
            tidb_type = f'varchar({tidb_len}) character set utf8mb4'
        elif tidb_len <= 65535:
            tidb_type = f'text({tidb_len}) character set utf8mb4'
        else:
            tidb_type = 'longtext character set utf8mb4'
    elif impala_type == 'bigint':
        tidb_type = 'bigint'
    elif impala_type == 'int':
        tidb_type = 'int'
    elif impala_type == 'tinyint':
        tidb_type = 'tinyint'
    elif impala_type == 'double':
        tidb_type = 'double'
    elif impala_type == 'timestamp':
        tidb_type = 'datetime'
    elif impala_type == 'boolean':
        tidb_type = 'boolean'
    elif impala_type.startswith('decimal'):
        tidb_type = impala_type
    else:
        raise Exception(f'unprocessed type {impala_type}')
    return tidb_type

def translate_default_value(tidb_type, default_value):
    if default_value and tidb_type.startswith('varchar'):
        default_value = f'"{default_value}"'
    elif tidb_type == 'datetime':
        if default_value == '0':
            return '"1970-01-01"'
        return ''
    elif tidb_type.startswith('text'):
        return ''
    return default_value


def create_one_table(table_schema, tidb_conn):
    table_name = table_schema['table']
    columns = table_schema['columns']
    primary_keys = table_schema['pk_unique_subset']
    create_sql_lines = []
    create_sql_lines.append(f'create table {table_name}')
    create_sql_lines.append('(')
    for column in columns:
        col_name = column["name"]
        col_type = translate_type(column['type'], column['len'])
        nullable = column['nullable']
        default_value = translate_default_value(col_type, column['default_value'])
        null_statment = 'null' if nullable == 'true' else 'not null'
        default_statment = '' if default_value == '' else f'default {default_value}'
        column_statement = f'\t{col_name: <25s}{col_type: <40s}{null_statment: <15s}{default_statment: <20s},'
        create_sql_lines.append(column_statement)
    if primary_keys:
        create_sql_lines.append(f'\tPRIMARY KEY ({primary_keys}) /*T![clustered_index] CLUSTERED */')
    else:
        create_sql_lines[-1] = create_sql_lines[-1].strip(',')
    create_sql_lines.append(')')
    tidb_conn.execute('\n'.join(create_sql_lines))
    tidb_conn.execute(f'ALTER TABLE {table_name} SET TIFLASH REPLICA 3')
    # print('\n'.join(create_sql_lines))

@utils.thread_method
def create_one_db(db, db_schema, total_count):
    start = time.time()
    if not hasattr(thread_context, 'tidb_conn'):
        thread_context.tidb_engine = utils.get_tidb_engine()
        thread_context.tidb_conn = thread_context.tidb_engine.connect()
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
    files = ['global_dwb.json']
    pool = ThreadPoolExecutor(max_workers=utils.thread_count)
    for file in files:
        db = file.replace('.json', '')
        with open(f'schemas/{file}', 'r', encoding='utf-8') as f:
            schema_text = f.read()
            db_schema = json.loads(schema_text)
        pool.submit(create_one_db, db, db_schema, len(files))
    pool.shutdown(wait=True)

if __name__ == '__main__':
    main()