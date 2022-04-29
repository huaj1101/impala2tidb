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
import re
import pandas as pd
import pymysql

logger = logging.getLogger(__name__)

def insert_into_impala():
    cursor = utils.get_impala_cursor_prod()
    sql = '''
SELECT db, user, statement, hash_id
FROM dp_stat.impala_sql_statistics 
where type = 'slow' 
      and query_state = 'FINISHED' 
      and duration < 30
      and statement not like '%...%' 
order by created_at, hash_id
'''
    cursor.execute(sql)
    df = as_pandas(cursor)
    cursor.close()
    pattern_user = "(.*)_internal_(write|read)"
    pattern_statement = r'(/\*&.*\*/)'
    count = 0
    cursor = utils.get_impala_cursor()
    cursor.execute('delete from default.slow_sqls')

    for i in range(len(df)):
        db = df.at[i, 'db']
        user = df.at[i, 'user']
        hash_id = df.at[i, 'hash_id']
        statement = df.at[i, 'statement']
        statement = re.sub(pattern_statement, '', statement)
        if '{{tenant}}' in statement:
            match = re.search(pattern_user, user)
            if match:
                tenant = match.group(1)
            elif db.endswith('_custom'):
                tenant = db.replace('_custom', '')
            else:
                print(f'skip one:\n{statement}')
                continue
            statement = statement.replace('{{tenant}}', tenant)
        sql = 'insert into default.slow_sqls (id, db, hash_id, sql_impala) values (?, ?, ?, ?)'
        cursor.execute(sql, [i + 1, db, statement, hash_id], {'paramstyle': 'qmark'})
        count += 1
        if count % 100 == 0:
            print(f'{count} / {len(df)} inserted to impala default.slow_sqls')
    cursor.execute('update default.slow_sqls set sql_tidb = sql_impala')
    cursor.close()
    if count % 100 != 0:
        print(f'{count} / {len(df)} inserted to impala default.slow_sqls')

def insert_into_tidb():
    cursor = utils.get_impala_cursor()
    utils.exec_impala_sql(cursor, 'select * from default.slow_sqls')
    df = as_pandas(cursor)
    print(f'{len(df)} loaded from impala default.slow_sqls')
    cursor.close()
    with utils.get_tidb_conn() as tidb_conn:
        df.to_sql('slow_sqls', tidb_conn, 'test', if_exists='append', index=False)
    print(f'{len(df)} inserted to tidb test.slow_sqls')

# 处理cast(xxx as string)
def process_tidb_sql_1():
    tidb_conn = utils.get_tidb_conn()
    
    df = pd.read_sql_query("SELECT id, sql_tidb FROM test.`slow_sqls` WHERE lower(sql_tidb) RLIKE '.*cast\((.+) as string\).*' AND enabled=1", tidb_conn)
    # print(len(df))
    for i in range(len(df)):
        id = df.at[i, 'id']
        tidb_sql = df.at[i, 'sql_tidb']
        tidb_sql = re.sub('cast\((.+?) as string\)', 'concat(\\1)', tidb_sql, flags=re.I)
        tidb_sql = pymysql.escape_string(tidb_sql)  
        # logger.info(tidb_sql)
        sql =f'update test.slow_sqls set sql_tidb = "{tidb_sql}" where id = {id}'
        tidb_conn.execute(sql)
        print(f'{i+1}/{len(df)}')

    

if __name__ == '__main__':
    # insert_into_impala()
    # insert_into_tidb()
    process_tidb_sql_1()