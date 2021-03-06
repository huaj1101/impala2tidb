import configparser
from enum import auto
import impala.dbapi
from pymysql import paramstyle
import sqlalchemy
import logging
import sys
import os
import time
import pandas as pd
import threading
import traceback
from functools import wraps
from impala.util import as_pandas
import hdfs
from hdfs.client import InsecureClient
from multiprocessing_logging import install_mp_handler


_config_path = os.path.dirname(__file__)
_config_file = _config_path + '/config.ini' if _config_path != '' else 'config.ini'
conf = configparser.ConfigParser()
conf.read(_config_file, encoding='utf-8')

logging.root.setLevel('INFO')
_log_format = conf.get('logging', 'log_format')
_log_file = conf.get('logging', 'file_name')

_file_haddler = logging.FileHandler(_log_file, encoding="utf-8")
_file_log_level = conf.get('logging', 'file_log_level')
_file_haddler.setLevel(_file_log_level)
_file_haddler.setFormatter(logging.Formatter(_log_format))
logging.root.addHandler(_file_haddler)

_console_handler = logging.StreamHandler(sys.stdout)
_console_log_level = conf.get('logging', 'console_log_level')
_console_handler.setLevel(_console_log_level)
_console_handler.setFormatter(logging.Formatter(_log_format))
logging.root.addHandler(_console_handler)

install_mp_handler()

logging.getLogger('impala').setLevel('ERROR')
hdfs.client._logger.setLevel('ERROR')

logger = logging.getLogger(__name__)

# 线程方法如果发生异常，必须在线程内捕获并记录，主线程得不到通知
def thread_method(fn):
    def fn_proxy(*args):
        global has_error
        try:
            fn(*args)
        except Exception as e:
            msg = '%s error:\n%s' % (fn.__module__, e)
            trace = traceback.format_exc()
            logger.error(msg + '\n' + trace)
            has_error = True
    return fn_proxy

_tidb_engine = None
_tiflash_only_tidb_engine = None
def get_tidb_conn(tiflash_only=False) -> sqlalchemy.engine.Connection:
    global _tidb_engine, _tiflash_only_tidb_engine
    if _tidb_engine is None:
        host = conf.get('tidb', 'host')
        port = conf.get('tidb', 'port')
        user = conf.get('tidb', 'user')
        pwd = conf.get('tidb', 'pwd')
        db = conf.get('tidb', 'db')
        con_str = f'mysql+mysqldb://{user}:{pwd}@{host}:{port}/{db}?charset=utf8'
        _tidb_engine = sqlalchemy.create_engine(con_str, pool_size=20, max_overflow=20)
        _tiflash_only_tidb_engine = sqlalchemy.create_engine(con_str, pool_size=10, max_overflow=20)
    if tiflash_only:
        conn = _tiflash_only_tidb_engine.connect()
        conn.execute(f'set @@session.tidb_isolation_read_engines = "tidb,tiflash"')
    else:
        conn = _tidb_engine.connect()
    return conn

def _pre_process_tidb_sql(sql):
    sql = sql.replace(r'%', r'%%')
    return sql

def exec_tidb_sql(conn: sqlalchemy.engine.Connection, sql: str, timeout=0):
    sql = _pre_process_tidb_sql(sql)
    # logger.info(sql)
    if timeout == 0:
        conn.execute(sql)
    else:
        try:
            try:
                conn.execute(f'set @@session.max_execution_time = {timeout * 1000}')
                conn.execute(sql)
            except Exception as e:
                if 'Query execution was interrupted' in str(e):
                    raise TimeoutError(f'timeout in {timeout} seconds')
        finally:
            conn.execute('set @@session.max_execution_time = 0')

def get_tidb_data(conn: sqlalchemy.engine.Connection, sql: str):
    sql = _pre_process_tidb_sql(sql)
    # logger.info(sql)
    return pd.read_sql_query(sql, conn)

def get_impala_cursor():
    conn = impala.dbapi.connect(host=conf.get('impala', 'host'), port=conf.getint('impala', 'port'), database=conf.get('impala', 'db'), 
        auth_mechanism='PLAIN', user=conf.get('impala', 'user'), password=conf.get('impala', 'pwd'))
    cursor = conn.cursor()
    return cursor

def get_impala_cursor_prod():
    conn = impala.dbapi.connect(host=conf.get('impala-prod', 'host'), port=conf.getint('impala-prod', 'port'), 
        database=conf.get('impala-prod', 'db'), auth_mechanism='PLAIN', user=conf.get('impala-prod', 'user'), 
        password=conf.get('impala-prod', 'pwd'))
    cursor = conn.cursor()
    return cursor

def filter_biz_db(db):
    # special_dbs = ['public_data', 'ai', 'dp_stat']
    special_dbs = ['public_data', 'dp_stat']
    ignore_dbs = []
    # ignore_dbs = ['global_dw_1', 'global_dw_2', 'global_dwb']
    return not db.startswith('___') and db not in ignore_dbs and not db.endswith('_text') and \
        (db.startswith('global_') or db.startswith('asset_') or db.endswith('_custom') or db in special_dbs)

def filter_table(db, table):
    if db == 'dp_stat':
        if table.endswith('_history') or table.startswith('impala_query_log'):
            return False
    return True

def get_impala_dbs(filter=filter_biz_db):
    cursor = get_impala_cursor()
    cursor.execute('show databases')
    dbs = []
    df = as_pandas(cursor)
    for i in range(len(df)):
        db = df.at[i, 'name']
        if filter == None or filter(db):
            dbs.append(db)
    cursor.close()
    return dbs

def get_tidb_dbs(filter=filter_biz_db):
    conn = get_tidb_conn()
    df = pd.read_sql_query('show databases', conn)
    dbs = []
    for i in range(len(df)):
        db = df.at[i, 'Database']
        if filter == None or filter(db):
            dbs.append(db)
    conn.close()
    return dbs

def get_tables_in_impala_db(db, cursor):
    cursor.execute(f'show tables in {db}')
    df = as_pandas(cursor)
    tables = []
    for i in range(len(df)):
        table = df.at[i, 'name']
        if filter_table(db, table):
            tables.append(table)
    return tables

def get_tables_in_tidb_db(db, conn):
    sql = f"select TABLE_NAME from INFORMATION_SCHEMA.`TABLES` where table_schema = '{db}' and table_type = 'BASE TABLE'"
    df = get_tidb_data(conn, sql)
    tables = []
    for i in range(len(df)):
        table = df.at[i, 'TABLE_NAME']
        if filter_table(db, table):
            tables.append(table)
    return tables

def exec_impala_sql(cursor, sql, query_options=None):
    sql = "/*& global:true */ \n" + sql
    cursor.execute(sql, configuration=query_options)
    execute_logs = cursor.get_log().split('\n')
    if len(execute_logs) > 2:
        err_msg = '\n'.join(execute_logs[1:len(execute_logs) - 1])
        # print(sql)
        raise Exception(err_msg)


def get_hdfs_client():
    return InsecureClient(conf.get('hdfs', 'name_node_url'), user=conf.get('hdfs', 'hdfs_user'))

def timeit(fn):
    @wraps(fn)
    def fn_proxy(*args, **kwargs):
        logger.info('call %s start' % fn.__name__)
        start = time.time()
        result = fn(*args, **kwargs)
        logger.info('call %s success in %.2f seconds' % (fn.__name__, time.time() - start))
        return result
    return fn_proxy