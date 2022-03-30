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
dbs_created = set()
BATCH_SIZE = 1e6

def save_to_csv(df, file_name):
    df.to_csv(f'data/{file_name}',sep='\u0001',na_rep='\\N',line_terminator='\u0002', header=False,index=False)

@utils.thread_method
def export_to_csv(table_schema, total_count):
    start = time.time()
    if not hasattr(thread_context, 'cursor'):
        thread_context.cursor = utils.get_impala_cursor()
    table_full_name = table_schema['table']
    db = table_full_name.split('.')[0].strip('`')
    table = table_full_name.split('.')[1].strip('`')

    batch_count = 1
    has_id_field = False
    for cs in table_schema['columns']:
        if cs['name'] == '`id`' and cs['type'] == 'bigint':
            has_id_field = True
    if has_id_field:
        sql = f'/*& global:true*/ select count(*) as cnt from {table_full_name}'
        thread_context.cursor.execute(sql)
        df = as_pandas(thread_context.cursor)
        record_count = df.at[0, 'cnt']
        batch_count = int(record_count / BATCH_SIZE) + 1
    if batch_count == 1:
        sql = f'/*& global:true*/ select * from {table_full_name}'
        thread_context.cursor.execute(sql)
        df = as_pandas(thread_context.cursor)
        save_to_csv(df, f'{db}.{table}.001.csv')
    else:
        for i in range(batch_count):
            sql = f'/*& global:true*/ select * from {table_full_name} where id % {batch_count} = {i}'
            thread_context.cursor.execute(sql)
            df = as_pandas(thread_context.cursor)
            save_to_csv(df, f'{db}.{table}.{i+1:0>3d}.csv')
    time_used = time.time() - start

    global finish_count
    lock.acquire()
    finish_count += 1
    logger.info('(%d / %d) %s finish in %.1f seconds' % (finish_count, total_count, table, time_used))
    lock.release()

def main():
    start = time.time()
    files = []
    for file in os.listdir('schemas/'):
        if file.endswith('.json'):
            files.append(file)
    files.sort()
    files = ['global_platform.json']
    threads = utils.conf.getint('sys', 'threads', fallback=1)
    pool = ThreadPoolExecutor(max_workers=threads)
    table_schemas = []
    for file in files:
        with open(f'schemas/{file}', 'r', encoding='utf-8') as f:
            schema_text = f.read()
            db_schema = json.loads(schema_text)
            table_schemas.extend(db_schema)

    logger.info('export_to_csv')
    for table_schema in table_schemas:
        pool.submit(export_to_csv, table_schema, len(table_schemas))
    pool.shutdown(wait=True)

    logger.info('finish in %.1f seconds' % (time.time() - start))


if __name__ == '__main__':
    main()
