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

sql_times = {}
lock = threading.Lock()
finish_count = 0

@utils.thread_method
def set_replica(db, table, total_count):
    sql = f'ALTER TABLE {db}.{table} SET TIFLASH REPLICA 3;'
    with utils.get_tidb_conn() as conn:
        conn.execute(sql)
    
    global finish_count
    lock.acquire()
    finish_count += 1
    logger.info(f'{finish_count} / {total_count} {db}.{table} set_replica done')
    lock.release()

@utils.timeit
def run(db):
    with utils.get_tidb_conn() as conn:
        tables = utils.get_tables_in_tidb_db(db, conn)

    pool = ThreadPoolExecutor(max_workers=10)
    for table in tables:
        pool.submit(set_replica, db, table, len(tables))
    pool.shutdown(wait=True)

if __name__ == '__main__':
    if len(sys.argv) == 2:
        db = sys.argv[1]
    else:
        logger.error('param shoud be: db_name')
        sys.exit(1)
    run(db)
