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
_task_count_th = 3000

_task_queue = Queue()
_finish_task_queue = Queue()

_fill_task_proc: Process = None
_exec_task_procs: List[Process] = []
_clean_task_procs: List[Process] = []

_share_dict = None
_lock = Lock()

class Task:
    def __init__(self, query_id, order_id, db, sql) -> None:
        self.query_id: str = query_id
        self.order_id: int = order_id
        self.db: str = db
        self.sql: str = sql
        self.exec_result: int = -1
        self.exec_duration: float = 0
        self.exec_time: str = ''
        self.err_msg: str = ''

def get_big_sql(query_id):
    logger.info(f'load big sql: {query_id}')
    sql = ''
    try:
        with open(f'big_sqls/{query_id.replace(":", "-")}.sql', 'r', encoding='utf-8') as f:
            sql = f.read()
    except Exception as e:
        logger.error(f'load big sql error: {e}')
    return sql

def get_new_tasks(batch_size=300) -> List[Task]:
    sql = f'select query_id, order_id, db, tidb_sql from test.translate_sqls \
            where execute_result is null and order_id > {_start_id} and tidb_sql != "" order by order_id limit {batch_size}'
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
            df.at[i, 'tidb_sql']
            )
        if task.sql == 'big_sql':
            task.sql = get_big_sql(task.query_id)
        if task.sql and task.db:
            result.append(task) 
    return result

def fill_task_action(share_dict, lock, task_queue: Queue):
    global _start_id
    while True:
        if task_queue.qsize() < _task_count_th:
            try:
                tasks = get_new_tasks()
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
        conn = utils.get_tidb_conn()
        try:
            try:
                if task.db != 'default':
                    utils.exec_tidb_sql(conn, f'use {task.db}')
                start = time.time()
                utils.exec_tidb_sql(conn, task.sql)
                duration = time.time() - start
            except Exception as e:
                err_msg = str(e)
                with lock:
                    share_dict['err_count'] = share_dict['err_count'] + 1
                # logger.error(f'{task.query_id} error')
        finally:
            conn.close()
        task.exec_duration = duration
        task.exec_result = 0 if err_msg else 1
        task.err_msg = err_msg
        task.exec_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        finish_task_queue.put(task)

def clean_task_action(share_dict, lock, task_queue: Queue, finish_task_queue: Queue):
    conn = utils.get_tidb_conn()
    while True:
        task: Task = finish_task_queue.get(block=True)
        sql = f'update test.translate_sqls set \
                  tidb_duration = {task.exec_duration}, \
                  execute_result = {task.exec_result}, \
                  execute_time = "{task.exec_time}" \
                where query_id = "{task.query_id}"'
        utils.exec_tidb_sql(conn, sql)
        if task.exec_result == 0:
            catalog = translate_utils.get_error_catalog(task.sql, task.err_msg)
            if catalog not in ('ignore_duplicate_insert', 'ignore_etl'):
                sql = f'insert into test.execute_err (query_id, err_msg, catalog) \
                        values ("{task.query_id}", "{escape_string(task.err_msg)}", "{catalog}") \
                        on duplicate key update err_msg=values(err_msg), catalog=values(catalog)'
                utils.exec_tidb_sql(conn, sql)
        with lock:
            share_dict['finish_count'] = share_dict['finish_count'] + 1
            if share_dict['finish_count'] % 100 == 0:
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

def run():
    global _share_dict
    manager = Manager()
    _share_dict = manager.dict({'finish_count': 0, 'err_count': 0})
    start_fill_task_proc()
    time.sleep(1)
    _share_dict['start_time'] = time.time()
    start_exec_task_procs(15)
    start_clean_task_procs(5)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        _fill_task_proc.terminate()
        for proc in _exec_task_procs:
            proc.terminate()
        for proc in _clean_task_procs:
            proc.terminate()
        logger.info('KeyboardInterrupt')

if __name__ == '__main__':
    run()