import os
import sys
import logging
from textwrap import indent
import threading
from impala.util import as_pandas
import time
import utils
import json
from typing import List
import pandas as pd
import pymysql
import requests
from multiprocessing import Process, Queue, Manager, Lock
from pymysql.converters import escape_string

logger = logging.getLogger(__name__)

_task_count_th = 3000

_task_queue = Queue()
_finish_task_queue = Queue()

_fill_task_proc: Process = None
_exec_task_procs: List[Process] = []
_clean_task_proc: Process = None

_share_dict = None
_lock = Lock()

_batch_size = utils.conf.getint('translate-api', 'batch')
_date = utils.conf.get('translate-api', 'date')

def get_new_tasks():
    host = utils.conf.get('translate-api', 'host')
    url = f'{host}/translate?batch_size={_batch_size}&table_postfix={_date}'
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(response.text)
    json_str = '[' + response.text.replace('}\n{', '},\n{') + ']'
    result = json.loads(json_str)
    return result

def fill_task_action(share_dict, lock, task_queue: Queue):
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
            # if len(tasks) > 0:
            #     logger.info(f'fill {len(tasks)} tasks, queue size: {task_queue.qsize()}')
        else:
            time.sleep(1)

def save_big_sql(query_id, sql):
    logger.info(f'save big sql: {query_id}')
    with open(f'big_sqls/{query_id.replace(":", "-")}.sql', 'w', encoding='utf-8') as f:
        f.write(sql)

def exec_task_action(share_dict, lock, task_queue: Queue, finish_task_queue: Queue):
    conn = utils.get_tidb_conn()
    while True:
        try:
            task = task_queue.get(block=True)
            query_id = task['queryId']
            impala_sql = task['impalaSql']
            if '_parquet_' in impala_sql or 'NDV(' in impala_sql or 'background:true' in impala_sql:
                finish_task_queue.put(query_id)
                continue
            tenant = task['tenant']
            db = task['db']
            big_sql = ''
            if task['success']:
                hash_id = task['hash']['value'] if task['hash']['valid'] else ''
                tidb_sql = task['tidbSql']
                sql_type = task['sqlType']
                impala_duration = task['duration']
                if len(impala_sql) > 1e6 or len(tidb_sql) > 1e6:
                    big_sql = tidb_sql
                    impala_sql = tidb_sql = ''
                sql = 'insert into test.translate_sqls (query_id, hash_id, tenant, db, sql_type, impala_sql, impala_duration, tidb_sql)' +\
                     f'values("{query_id}", "{hash_id}", "{tenant}", "{db}", "{sql_type}", "{escape_string(impala_sql)}", \
                            {impala_duration}, "{escape_string(tidb_sql)}")' +\
                      'on duplicate key update tidb_sql=values(tidb_sql)'
            else:
                err_msg = task['error']
                sql = 'insert into test.translate_err (query_id, tenant, db, impala_sql, err_msg)' +\
                     f'values("{query_id}", "{tenant}", "{db}", "{escape_string(impala_sql)}", "{escape_string(err_msg)}")' +\
                      'on duplicate key update err_msg=values(err_msg)'
        except Exception as e:
            logger.error(f'parse api response {query_id} error: {task}')
            continue
        try:
            utils.exec_tidb_sql(conn, sql)
            if big_sql:
                save_big_sql(query_id, big_sql)
            finish_task_queue.put(query_id)
        except Exception as e:
            logger.error(f'save tidb error: {query_id} \n {e}')

def clean_task_action(share_dict, lock, task_queue: Queue, finish_task_queue: Queue):
    cursor = utils.get_impala_cursor() 
    finish_count = 0
    while True:
        if finish_task_queue.qsize() < _batch_size:
            time.sleep(0.1)
            continue
        query_ids = []
        for i in range(_batch_size):
            query_ids.append(f'"{finish_task_queue.get()}"')
        sql = f'update dp_stat.impala_query_log_{_date} set saved_in_tidb = true where query_id in ({",".join(query_ids)})'
        try:
            utils.exec_impala_sql(cursor, sql)
            finish_count += _batch_size
        except Exception as e:
            logger.error(f'save impala result error: {e}')
            time.sleep(1)
        if finish_count % 200 == 0:
            msg = f'finish: {finish_count}, '
            msg = msg + f'tps: {round(finish_count / (time.time() - share_dict["start_time"]))}, '
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

def start_clean_task_proc():
    global _clean_task_proc
    _clean_task_proc = Process(target=clean_task_action, name='proc-clean-task', 
        args=(_share_dict, _lock, _task_queue, _finish_task_queue))
    _clean_task_proc.daemon = True
    _clean_task_proc.start()

def do_clean():
    sql = f'update dp_stat.impala_query_log_{_date} set processed = false where processed = true and saved_in_tidb = false'
    with utils.get_impala_cursor() as cursor:
        utils.exec_impala_sql(cursor, sql)

def run():
    do_clean()
    global _share_dict
    manager = Manager()
    _share_dict = manager.dict()
    _share_dict['start_time'] = time.time()
    start_fill_task_proc()
    start_exec_task_procs(5)
    start_clean_task_proc()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        _fill_task_proc.terminate()
        for proc in _exec_task_procs:
            proc.terminate()
        _clean_task_proc.terminate()
        logger.info('KeyboardInterrupt')

if __name__ == '__main__':
    run()
