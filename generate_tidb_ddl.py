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
import translate_utils

logger = logging.getLogger(__name__)

sql_times = {}
lock = threading.Lock()
finish_count = 0

def create_one_table(table_schema, tidb_conn):
    sql = translate_utils.gen_create_table_sql(table_schema)
    tidb_conn.execute(sql)
    tidb_conn.execute(f'ALTER TABLE {table_schema["table"]} SET TIFLASH REPLICA 3')

@utils.thread_method
def recreate_one_db(db, db_schema, total_count):
    start = time.time()
    with utils.get_tidb_conn() as conn:
        conn.execute(f'DROP DATABASE IF EXISTS {db}')
        conn.execute(f'CREATE DATABASE {db}')
        for table_schema in db_schema:
            create_one_table(table_schema, conn)
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
    output_f = open('tidb_ddl.sql', 'w', encoding='utf-8')
    index = 0
    for file in files:
        index += 1
        with open(f'schemas/{file}', 'r', encoding='utf-8') as f:
            schema_text = f.read()
            db_schema = json.loads(schema_text)
            for table_schema in db_schema:
                sql = translate_utils.gen_create_table_sql(table_schema)
                output_f.write(sql)
                output_f.write(';\n')
        logger.info(f'{index} / {len(files)} finished')
    output_f.close()

if __name__ == '__main__':
    main()

