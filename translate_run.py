import os
import sys
import logging
import threading
from typing import List
from impala.util import as_pandas
import time
import datetime
import utils
import json
import re
import pandas as pd
import pymysql
import translate_utils
from pymysql.converters import escape_string
from multiprocessing import Process, Queue, Manager, Lock

logger = logging.getLogger(__name__)

_start_id = 0
_date = utils.conf.get('translate', 'date')
_limit_count = utils.conf.getint('translate', 'limit_count', fallback=0)
_task_count_th = 3000
_exsits_batch = []

_task_queue = Queue()
_finish_task_queue = Queue()

_fill_task_proc: Process = None
_exec_task_procs: List[Process] = []
_clean_task_procs: List[Process] = []

_share_dict = None
_lock = Lock()

class Task:
    def __init__(self, query_id, order_id, db, sql, sql_type, hash_id, tiflash_only) -> None:
        self.query_id: str = query_id
        self.order_id: int = order_id
        self.db: str = db
        self.sql: str = sql
        self.sql_type: str = sql_type
        self.hash_id: str = hash_id
        self.tiflash_only: str = tiflash_only
        self.exec_result: int = -1
        self.exec_duration: float = 0
        self.exec_time: str = ''
        self.err_msg: str = ''
        self.catalog: str = ''

    def __str__(self) -> str:
        return str(self.query_id)

    def __repr__(self) -> str:
        return str(self)

def get_big_sql(query_id):
    logger.info(f'load big sql: {query_id}')
    sql = ''
    try:
        with open(f'big_sqls/{query_id.replace(":", "-")}.sql', 'r', encoding='utf-8') as f:
            sql = f.read()
    except Exception as e:
        logger.error(f'load big sql error: {e}')
    return sql

def get_new_tasks(second_time, batch_size=1000) -> List[Task]:
    if second_time:
        exists_batch_ids = ','.join([f'"{query_id}"' for query_id in _exsits_batch])
        if not exists_batch_ids:
            exists_batch_ids = '""'
        # sql = f'select query_id, hash_id, sql_type, order_id, db, tidb_sql2 as tidb_sql, tiflash_only from test.translate_sqls \
        #         where sql_date = "{_date}" and execute_result2 is null and tidb_sql2 != ""  \
	    #         and query_id not in \
        #         (select query_id from test.`translate_err` where catalog in ("timeout", "delay") or catalog like "modify_%") \
        #         and query_id not in ({exists_batch_ids}) \
        #         limit {batch_size}'
        sql = f'select query_id, hash_id, sql_type, order_id, db, tidb_sql2 as tidb_sql, tiflash_only from test.translate_sqls \
                where sql_date = "{_date}" and execute_result2 is null and order_id > {_start_id} \
                    and tidb_sql2 != "" order by order_id limit {batch_size}'
    else:
        sql = f'select query_id, hash_id, sql_type, order_id, db, tidb_sql, tiflash_only  \
                from test.`translate_sqls` \
                where query_id in ( \
                    select query_id from test.`translate_sqls` \
                    where sql_date = "{_date}" and execute_result is null and order_id > {_start_id}  \
                    order by order_id limit {2 * batch_size} \
                ) and tidb_sql != ""  \
                order by order_id \
                limit {batch_size}'
        # sql = f'select query_id, hash_id, sql_type, order_id, db, tidb_sql, tiflash_only from test.translate_sqls \
        #         where sql_date = "{_date}" and execute_result is null and order_id > {_start_id} \
        #             and tidb_sql != "" order by order_id limit {batch_size}'

    conn = utils.get_tidb_conn()
    try:
        df = utils.get_tidb_data(conn, sql)
    finally:
        conn.close()
    result = []
    for i in range(len(df)):
        task = Task(
            df.at[i, 'query_id'],
            df.at[i, 'order_id'],
            df.at[i, 'db'],
            df.at[i, 'tidb_sql'],
            df.at[i, 'sql_type'],
            df.at[i, 'hash_id'],
            df.at[i, 'tiflash_only']
            )
        if task.sql == 'big_sql':
            task.sql = get_big_sql(task.query_id)
        if task.sql and task.db:
            result.append(task)
        if second_time:
            _exsits_batch.append(task.query_id)
    if second_time and len(_exsits_batch) > batch_size:
        for i in range(batch_size // 2):
            del _exsits_batch[0]
    return result

def fill_task_action(share_dict, lock, task_queue: Queue):
    global _start_id
    second_time = share_dict['second_time']
    while True:
        if task_queue.qsize() < _task_count_th:
            try:
                # logger.info('get new task start')
                tasks = get_new_tasks(second_time)
                # logger.info('get new task done')
            except Exception as e:
                logger.error(f'get tasks error: {e}')
                time.sleep(1)
                continue
            for task in tasks:
                task_queue.put(task)
            if len(tasks) > 0:
                _start_id = tasks[-1].order_id
                # logger.info(f'fill {len(tasks)} tasks, queue size: {task_queue.qsize()}')
            else:
                time.sleep(1)
        else:
            time.sleep(1)

def exec_task_action(share_dict, lock, task_queue: Queue, finish_task_queue: Queue):
    while True:
        try:
            task: Task = task_queue.get(block=True)
        except Exception as e:
            logger.error(str(e))
            time.sleep(3)
            continue
        err_msg = ''
        duration = 0
        # logger.info(f'task_start: {task.query_id}')
        if task.sql_type == 'Query':
            conn = utils.get_tidb_conn(auto_commit=False, tiflash_only=task.tiflash_only)
        else:
            conn = utils.get_tidb_conn()
        try:
            try:
                if task.db != 'default':
                    utils.exec_tidb_sql(conn, f'use {task.db}')
                start = time.time()

                task_sql = task.sql#.replace('mctech_encrypt', 'upper').replace('mctech_decrypt', 'upper').replace('mctech_sequence()', 'nextval(test.seq)')

                utils.exec_tidb_sql(conn, task_sql, 20)
                duration = time.time() - start
            except TimeoutError as e:
                err_msg = str(e)
                task.catalog = 'timeout'
                conn = None # conn被占住无法释放了
            except Exception as e:
                err_msg = str(e)
        finally:
            if conn != None:
                conn.close()
        # logger.info(f'task_executed: {task}')
        task.exec_duration = duration
        task.exec_result = 0 if err_msg else 1
        task.err_msg = err_msg
        task.exec_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        finish_task_queue.put(task)

def clean_task_action(share_dict, lock, task_queue: Queue, finish_task_queue: Queue):
    second_time = share_dict['second_time']
    col1 = 'tidb_duration2' if second_time else 'tidb_duration'
    col2 = 'execute_result2' if second_time else 'execute_result'
    err_table = 'translate_err2' if second_time else 'translate_err'
    while True:
        conn = utils.get_tidb_conn()
        start = time.time()
        wait_so_long = False
        try:
            try:
                task: Task = finish_task_queue.get(block=True)
                wait_so_long = time.time() - start > 10
                sql = f'update test.translate_sqls set \
                        {col1} = {task.exec_duration}, \
                        {col2} = {task.exec_result}, \
                        execute_time = "{task.exec_time}" \
                        where query_id = "{task.query_id}"'
                utils.exec_tidb_sql(conn, sql)
                # logger.info(f'task_cleaned: {task}')
                if task.exec_result == 0:
                    if not task.catalog:
                        task.catalog = translate_utils.get_error_catalog(task.sql, task.err_msg)
                    if task.catalog not in ('ignore_duplicate_insert', 'ignore_etl'):
                        sql = f'insert into test.{err_table} (query_id, sql_date, hash_id, err_msg, catalog) \
                                values ("{task.query_id}", "{_date}", "{task.hash_id}", "{escape_string(task.err_msg)}", "{task.catalog}") \
                                on duplicate key update err_msg=values(err_msg), catalog=values(catalog)'
                        utils.exec_tidb_sql(conn, sql)
                        with lock:
                            share_dict['err_count'] = share_dict['err_count'] + 1
                    # logger.info(f'task_log_error: {task}')
            except Exception as e:
                logger.error(f'clean task {task.query_id} error: {str(e)}')
        finally:
            conn.close()
        with lock:
            if _limit_count > 0 and share_dict['finish_count'] > _limit_count:
                sys.exit(0)
            share_dict['finish_count'] = share_dict['finish_count'] + 1
            if share_dict['finish_count'] % 100 == 0 or wait_so_long:
                msg = f'finish: {share_dict["finish_count"]}, '
                msg = msg + f'tps: {round(share_dict["finish_count"] / (time.time() - share_dict["start_time"]))}, '
                msg = msg + f'fail: {share_dict["err_count"]}, '
                msg = msg + f'queue: {task_queue.qsize()}, '
                msg = msg + f'to_clean: {finish_task_queue.qsize()}'
                logger.info(msg)

def start_fill_task_proc():
    global _fill_task_proc
    _fill_task_proc = Process(target=fill_task_action, name='proc-fill-task', 
        args=(_share_dict, _lock, _task_queue))
    _fill_task_proc.daemon = True
    _fill_task_proc.start()

def start_exec_task_procs(thread_count):
    for i in range(thread_count):
        proc = Process(target=exec_task_action, name=f'proc-exec-task-{i}',
            args=(_share_dict, _lock, _task_queue, _finish_task_queue))
        proc.daemon = True
        _exec_task_procs.append(proc)
    for proc in _exec_task_procs:
        proc.start()

def start_clean_task_procs(thread_count):
    for i in range(thread_count):
        proc = Process(target=clean_task_action, name=f'proc-clean-task-{i}', 
            args=(_share_dict, _lock, _task_queue, _finish_task_queue))
        proc.daemon = True
        _clean_task_procs.append(proc)
    for proc in _clean_task_procs:
        proc.start()

def run(second_time):
    global _share_dict
    manager = Manager()
    _share_dict = manager.dict()
    _share_dict['finish_count'] = 0
    _share_dict['err_count'] = 0
    _share_dict['second_time'] = second_time
    start_fill_task_proc()
    while _task_queue.qsize() == 0:
        time.sleep(0.1)
    _share_dict['start_time'] = time.time()
    start_exec_task_procs(15)
    start_clean_task_procs(5)
    try:
        while True:
            if _limit_count > 0 and _share_dict['finish_count'] > _limit_count:
                break
            time.sleep(1)
    except KeyboardInterrupt:
        _fill_task_proc.terminate()
        for proc in _exec_task_procs:
            proc.terminate()
        for proc in _clean_task_procs:
            proc.terminate()
        logger.info('KeyboardInterrupt')

if __name__ == '__main__':
    second_time = False
    if len(sys.argv) == 2 and sys.argv[1] == 'second':
        second_time = True
    run(second_time)