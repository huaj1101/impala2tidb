import configparser
import impala.dbapi
import sqlalchemy
import logging
import sys
import os
from impala.util import as_pandas

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

def get_mysql_engine():
    host = conf.get('mysql', 'host')
    port = conf.get('mysql', 'port')
    user = conf.get('mysql', 'user')
    pwd = conf.get('mysql', 'pwd')
    db = conf.get('mysql', 'db')
    con_str = f'mysql+mysqldb://{user}:{pwd}@{host}:{port}/{db}?charset=utf8'
    return sqlalchemy.create_engine(con_str)

def get_impala_cursor():
    conn = impala.dbapi.connect(host=conf.get('impala', 'host'), port=conf.getint('impala', 'port'), database=conf.get('impala', 'db'), 
        auth_mechanism='PLAIN', user=conf.get('impala', 'user'), password=conf.get('impala', 'pwd'))
    cursor = conn.cursor()
    return cursor

def get_tables_in_impala_db(db, cursor):
    cursor.execute(f'show tables in {db}')
    df = as_pandas(cursor)
    tables = []
    for i in range(len(df)):
        tables.append(df.at[i, 'name'])
    return tables

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