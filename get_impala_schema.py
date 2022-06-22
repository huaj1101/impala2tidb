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

def get_pk_unique_subset(db, table, pk, cursor):
    new_keys = []
    if pk[0] == '`tenant`':
        if '`id`' in pk:
            new_keys.append('`tenant`')
            new_keys.append('`id`')
        else:
            new_keys = pk
    elif '`id`' in pk:
        new_keys = ['`id`']
    else:
        new_keys = pk
    sql = f'SELECT count(*) FROM {db}.{table} group by {",".join(new_keys)} having count(*) > 1 limit 1'
    utils.exec_impala_sql(cursor, sql)
    df = as_pandas(cursor)
    if len(df) == 0:
        return new_keys
    return pk

def get_table_schema_kudu(db, table, cursor, df_columns):
    primary_keys = []
    columns = []
    str_columns = []
    for i in range(len(df_columns)):
        name = f'`{df_columns.at[i, "name"]}`'
        type = df_columns.at[i, 'type']
        if type == 'string':
            str_columns.append(name)
        nullable = df_columns.at[i, 'nullable']
        default_value = df_columns.at[i, 'default_value']
        primary_key = df_columns.at[i, 'primary_key']
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
        sql = f'select {",".join([f"ifnull(max(length({col})), 0) as {col}" for col in str_columns])} from {db}.{table}'
        utils.exec_impala_sql(cursor, sql)
        df_columns = as_pandas(cursor)
        for i in range(len(df_columns.columns)):
            name = df_columns.columns[i]
            value = df_columns.at[0, name]
            name = f'`{name}`'
            for cs in columns:
                # 这些字段经常长度不够用，做大一点
                if cs['name'] == '`excel_task_id`':
                    cs['len'] = 1000
                if cs['name'] == '`remark`':
                    cs['len'] = 2000
                elif cs['name'] == name:
                    cs['len'] = int(value)
    return {
        'table': f'`{db}`.`{table}`', 
        'type': 'kudu',
        'columns': columns,
        'primary_keys': ','.join(primary_keys),
        'pk_unique_subset': ','.join(get_pk_unique_subset(db, table, primary_keys, cursor))
        }

def get_table_schema_parquet(db, table, cursor, df_columns):
    columns = []
    str_columns = []
    for i in range(len(df_columns)):
        name = f'`{df_columns.at[i, "name"]}`'
        type = df_columns.at[i, 'type']
        if type == 'string' and name != '`tenant`':
            str_columns.append(name)
        column_schema = {
            'name': name,
            'type': type,
            'len': 0,
            'nullable': 'true',
            'default_value': ""
        }
        columns.append(column_schema)
    tenant_column = None
    for cs in columns:
        if cs['name'] == '`tenant`':
            tenant_column = cs
            break
    if tenant_column:
        tenant_column['len'] = 50
        columns.remove(tenant_column)
        columns.insert(0, tenant_column)
    if str_columns:
        sql = f'select {",".join([f"ifnull(max(length({col})), 0) as {col}" for col in str_columns])} from {db}.{table}'
        utils.exec_impala_sql(cursor, sql)
        df_columns = as_pandas(cursor)
        for i in range(len(df_columns.columns)):
            name = df_columns.columns[i]
            value = df_columns.at[0, name]
            name = f'`{name}`'
            for cs in columns:
                if cs['name'] == name:
                    cs['len'] = int(value)
    return {
        'table': f'`{db}`.`{table}`', 
        'type': 'parquet',
        'columns': columns,
        'primary_keys': '',
        'pk_unique_subset': ''
        }

def get_table_schema(db, table, cursor):
    cursor.execute(f'describe {db}.{table}')
    df = as_pandas(cursor)
    if len(df.columns) == 3:
        ts = get_table_schema_parquet(db, table, cursor, df)
    else:
        ts = get_table_schema_kudu(db, table, cursor, df)
    sql = f'select count(*) as cnt from {db}.{table}'
    utils.exec_impala_sql(cursor, sql)
    df = as_pandas(cursor)
    ts['record_count'] = int(df.at[0, 'cnt'])
    return ts


@utils.thread_method
def get_db_schema(db, total_count):
    if not hasattr(thread_context, 'cursor'):
        thread_context.cursor = utils.get_impala_cursor()
    start = time.time()
    tables = utils.get_tables_in_impala_db(db, thread_context.cursor)
    # tables = ['project_entry_work']
    table_schemas = []
    for table in tables:
        if not utils.filter_table(db, table):
            logger.info(f'skip {db}.{table}')
            continue
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

@utils.timeit
def main():
    dbs = utils.get_impala_dbs()
    # dbs = ['dp_stat']
    pool = ThreadPoolExecutor(max_workers=15)
    for db in dbs:
        pool.submit(get_db_schema, db, len(dbs))
    pool.shutdown(wait=True)

if __name__ == '__main__':
    main()