import configparser
import impala.dbapi
from pymysql import paramstyle
import sqlalchemy
import logging
import sys
import os
import time
import pandas as pd
from functools import wraps
from impala.util import as_pandas
from hdfs.client import InsecureClient

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

logging.getLogger('impala').setLevel('ERROR')

logger = logging.getLogger(__name__)
thread_count = conf.getint('sys', 'threads', fallback=1)

def get_tidb_engine():
    host = conf.get('tidb', 'host')
    port = conf.get('tidb', 'port')
    user = conf.get('tidb', 'user')
    pwd = conf.get('tidb', 'pwd')
    db = conf.get('tidb', 'db')
    con_str = f'mysql+mysqldb://{user}:{pwd}@{host}:{port}/{db}?charset=utf8'
    return sqlalchemy.create_engine(con_str)

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
    special_dbs = ['public_data']
    ignore_dbs = []
    # ignore_dbs = ['global_dw_1', 'global_dw_2', 'global_dwb']
    return not db.startswith('___') and db not in ignore_dbs and not db.endswith('_text') and \
        (db.startswith('global_') or db.startswith('asset_') or db.endswith('_custom') or db in special_dbs)

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
    engine = get_tidb_engine()
    conn = engine.connect()
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
        tables.append(df.at[i, 'name'])
    return tables

def get_tables_in_tidb_db(db, conn):
    df = pd.read_sql_query(f'show tables in {db}', conn)
    tables = []
    for i in range(len(df)):
        tables.append(df.at[i, f'Tables_in_{db}'])
    return tables

def exec_sql(cursor, sql, query_options=None):
    sql = "/*& global:true */ \n" + sql
    # 极少数时候会出现偶发异常：[Errno 104] Connection reset by peer
    # 加入一次重试
    try:
        cursor.execute(sql, configuration=query_options)
    except:
        time.sleep(0.1)
        cursor.execute(sql, configuration=query_options)
    execute_logs = cursor.get_log().split('\n')
    if len(execute_logs) > 2:
        err_msg = '\n'.join(execute_logs[1:len(execute_logs) - 1])
        # print(sql)
        raise Exception(err_msg)

# 线程方法如果发生异常，必须在线程内捕获并记录，主线程得不到通知
def thread_method(fn):
    def fn_proxy(*args):
        global has_error
        try:
            fn(*args)
        except Exception as e:
            msg = '%s error:\n%s' % (fn.__module__, e)
            logger.error(msg)
            has_error = True
    return fn_proxy

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