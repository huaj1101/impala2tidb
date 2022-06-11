import os
import sys
import logging
from textwrap import indent
import threading
from impala.util import as_pandas
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List
import pandas as pd
import utils
import json

logger = logging.getLogger(__name__)

thread_context = threading.local()
sql_times = {}
lock = threading.Lock()
finish_count = 0
mismatch = False

class TableInfo:
    def __init__(self, db, table) -> None:
        self.db = db
        self.table = table
        self.full_name = db + '.' + table
        self.schema_match = True
        self.data_match = True
        self.cols = []
        self.is_parquet = False
        self.has_id = None

tables: List[TableInfo] = []

@utils.timeit
def compare_dbs(dbs_impala, dbs_tidb):
    logger.info(f'  impala dbs: {len(dbs_impala)}')
    logger.info(f'  tidb dbs: {len(dbs_tidb)}')
    global mismatch
    for db_impala in dbs_impala:
        if db_impala not in dbs_tidb:
            logger.error(f'  {db_impala} not found in tidb')
            mismatch = True
    for db_tidb in dbs_tidb:
        if db_tidb not in dbs_impala:
            logger.error(f'  {db_tidb} not found in impala')
            mismatch = True

@utils.thread_method
def compare_tables_in_one_db(db, total_count):
    start = time.time()
    if not hasattr(thread_context, 'impala_cursor'):
        thread_context.impala_cursor = utils.get_impala_cursor()
    if not hasattr(thread_context, 'tidb_conn'):
        thread_context.tidb_conn = utils.get_tidb_conn()
    tables_impala = utils.get_tables_in_impala_db(db, thread_context.impala_cursor)
    tables_tidb = utils.get_tables_in_tidb_db(db, thread_context.tidb_conn)
    # print(len(tables_impala))
    # print(len(tables_tidb))
    tables_impala_set = set(tables_impala)
    tables_tidb_set = set(tables_tidb)
    global mismatch
    for table_impala in tables_impala:
        if table_impala not in tables_tidb_set:
            logger.error(f'  {db}.{table_impala} not found in tidb')
            mismatch = True
    for table_tidb in tables_tidb:
        if table_tidb not in tables_impala_set:
            logger.error(f'  {db}.{table_tidb} not found in impala')
            mismatch = True
    global finish_count
    lock.acquire()
    if not mismatch:
        tables.extend([TableInfo(db, table) for table in tables_impala])
    finish_count += 1
    logger.info('  %d / %d compare_tables in %s finish in %.1f seconds--' % (finish_count, total_count, db, time.time() - start))
    lock.release()

@utils.timeit
def compare_tables(dbs):
    global finish_count
    finish_count = 0
    pool = ThreadPoolExecutor(max_workers=10)
    for db in dbs:
        pool.submit(compare_tables_in_one_db, db, len(dbs))
    pool.shutdown(wait=True)

@utils.thread_method
def compare_one_table_schema(table: TableInfo, total_count):
    if not hasattr(thread_context, 'impala_cursor'):
        thread_context.impala_cursor = utils.get_impala_cursor()
    if not hasattr(thread_context, 'tidb_conn'):
        thread_context.tidb_conn = utils.get_tidb_conn()

    sql = f'describe {table.full_name}'
    
    thread_context.impala_cursor.execute(sql)
    df_impala = as_pandas(thread_context.impala_cursor)
    if len(df_impala.columns) == 3:
        table.is_parquet = True
    col_cnt_impala = len(df_impala)

    df_tidb = pd.read_sql_query(sql, thread_context.tidb_conn)
    col_cnt_tidb = len(df_tidb)
    global mismatch
    if col_cnt_impala != col_cnt_tidb:
        logger.error(f'  {table.full_name} col count mismatch, impala {col_cnt_impala} tidb {col_cnt_tidb}')
        mismatch = True

    cols_impala = set()
    for i in range(len(df_impala)):
        cols_impala.add(df_impala.at[i, 'name'])
    cols_tidb = set()
    for i in range(len(df_tidb)):
        cols_tidb.add(df_tidb.at[i, 'Field'])

    for col_impala in cols_impala:
        if col_impala not in cols_tidb:
            logger.error(f'  {table.full_name}.{col_impala} not found in impala')
            mismatch = True
    for col_tidb in cols_tidb:
        if col_tidb not in cols_tidb:
            logger.error(f'  {table.full_name}.{col_tidb} not found in tidb')
            mismatch = True

    if mismatch:
        table.schema_match = False
    else:
        table.cols = cols_impala

    global finish_count
    lock.acquire()
    finish_count += 1
    if finish_count % 100 == 0:
        logger.info(f'  {finish_count} / {total_count} compare table schema finished')
    lock.release()

@utils.timeit
def compare_tables_schema():
    global finish_count
    finish_count = 0
    pool = ThreadPoolExecutor(max_workers=10)
    for table in tables:
        pool.submit(compare_one_table_schema, table, len(tables))
    pool.shutdown(wait=True)
    error_count = 0
    for table in tables:
        if not table.schema_match:
            error_count += 1
    if error_count > 0:
        global mismatch
        mismatch = True
        logger.error(f'  table schema mismatch count: {error_count}')

@utils.thread_method
def compare_one_table_data(table: TableInfo, total_count):
    if not hasattr(thread_context, 'impala_cursor'):
        thread_context.impala_cursor = utils.get_impala_cursor()
    if not hasattr(thread_context, 'tidb_conn'):
        thread_context.tidb_conn = utils.get_tidb_conn()
    # check record count
    sql = f'select count(*) as cnt from {table.db}.`{table.table}`'
    utils.exec_impala_sql(thread_context.impala_cursor, sql)
    df_impala = as_pandas(thread_context.impala_cursor)
    cnt_impala = df_impala.at[0, 'cnt']

    df_tidb = pd.read_sql_query(sql, thread_context.tidb_conn)
    cnt_tidb = df_tidb.at[0, 'cnt']
    if cnt_impala != cnt_tidb:
        logger.error(f'  {table.full_name} record count mismatch, impala: {cnt_impala}, tidb: {cnt_tidb}')
        table.data_match = False
    else:
        # check max column (if exists)
        if 'id' in table.cols:
            compare_field_max_value(thread_context.impala_cursor, thread_context.tidb_conn, table, 'id', False)
        if 'version' in table.cols:
            compare_field_max_value(thread_context.impala_cursor, thread_context.tidb_conn, table, 'version', False)
        if 'updated_at' in table.cols:
            compare_field_max_value(thread_context.impala_cursor, thread_context.tidb_conn, table, 'updated_at', True)

    global finish_count
    lock.acquire()
    finish_count += 1
    if finish_count % 100 == 0:
        logger.info(f'  {finish_count} / {total_count} compare table data finished')
    lock.release()

def compare_field_max_value(impala_cursor, tidb_conn, table: TableInfo, field, is_timestamp):
    if is_timestamp:
        sql_impala = f'select max(cast(round(cast({field} as double)) as TIMESTAMP)) as max_value from {table.db}.`{table.table}`'
    else:
        sql_impala = f'select max({field}) as max_value from {table.db}.`{table.table}`'
    utils.exec_impala_sql(impala_cursor, sql_impala)
    df_impala = as_pandas(impala_cursor)
    impala_value = df_impala.at[0, 'max_value']

    sql_tidb = f'select max({field}) as max_value from {table.db}.`{table.table}`'
    df_tidb = pd.read_sql_query(sql_tidb, tidb_conn)
    tidb_value = df_tidb.at[0, 'max_value']
    if impala_value != tidb_value:
        logger.error(f'  {table.full_name} max {field} mismatch, impala: {impala_value}, tidb: {tidb_value}')
        table.data_match = False

@utils.timeit
def compare_tables_data():
    global finish_count
    finish_count = 0
    pool = ThreadPoolExecutor(max_workers=10)
    for table in tables:
        pool.submit(compare_one_table_data, table, len(tables))
    pool.shutdown(wait=True)
    error_count = 0
    for table in tables:
        if not table.data_match:
            error_count += 1
    if error_count > 0:
        global mismatch
        mismatch = True
        logger.error(f'  table data mismatch count: {error_count}')

@utils.timeit
def main():
    dbs_impala = utils.get_impala_dbs()
    dbs_tidb = utils.get_tidb_dbs()
    # 比较数据是否匹配
    compare_dbs(dbs_impala, dbs_tidb)
    if mismatch: 
        logger.error('dbs mismatch')
        return
    # 比较表是否匹配
    # dbs_impala = ['global_platform']
    compare_tables(dbs_impala)
    if mismatch: 
        logger.error('tables mismatch')
        return

    # 比较每张表的schema
    compare_tables_schema()
    if mismatch: 
        logger.error('tables schema mismatch')
        return
    # 比较每张表的data
    compare_tables_data()
    if mismatch: 
        logger.error('tables data mismatch')
        return

if __name__ == '__main__':
    main()