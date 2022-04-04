from asyncore import read
from email.policy import default
import os
from re import S
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

@utils.thread_method
def truncate_table(table_schema, total_count):
    # start = time.time()
    if not hasattr(thread_context, 'mysql_conn'):
        thread_context.mysql_engine = utils.get_mysql_engine()
        thread_context.mysql_conn = thread_context.mysql_engine.connect()
    sql = f'truncate {table_schema["table"]}'
    # print(sql)
    thread_context.mysql_conn.execute(sql)
    
    # time_used = time.time() - start
    global finish_count
    lock.acquire()
    finish_count += 1
    if finish_count % 100 == 0:
        logger.info('%d / %d truncate' % (finish_count, total_count))
    lock.release()

@utils.timeit
def main():
    files = []
    for file in os.listdir('schemas/'):
        if file.endswith('.json'):
            files.append(file)
    files.sort()
    # files = ['crssg_custom.json']
    tables_schema = []
    for file in files:
        db = file.replace('.json', '')
        with open(f'schemas/{file}', 'r', encoding='utf-8') as f:
            schema_text = f.read()
            db_schema = json.loads(schema_text)
            tables_schema.extend(db_schema)
    
    pool = ThreadPoolExecutor(max_workers=utils.thread_count)
    for table_schema in tables_schema:
        pool.submit(truncate_table, table_schema, len(tables_schema))
    pool.shutdown(wait=True)

if __name__ == '__main__':
    main()