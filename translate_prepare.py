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

_task_count_th = 30000

_task_queue = Queue()
_finish_task_queue = Queue()

_fill_task_procs: List[Process] = []
_exec_task_procs: List[Process] = []
_clean_task_proc: Process = None

_share_dict = None
_lock = Lock()

_batch_size = utils.conf.getint('translate', 'batch')
_date = utils.conf.get('translate', 'date')

def get_new_tasks():
    start = time.time()
    host = utils.conf.get('translate', 'api-host')
    url = f'{host}/translate?batch_size={_batch_size}&table_postfix={_date}'
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(response.text)
    # logger.info(f'get task once in {time.time() - start} s')
    start = time.time()
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
                if task['db'] != 'ghatg_gsbim':
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
    sql_date = share_dict['sql_date']
    second_time = share_dict['second_time']
    while True:
        try:
            task = task_queue.get(block=True)
            query_id = task['queryId']
            tenant = task['tenant']
            db = task['db']
            hash_id = task['hash']['value'] if 'hash' in task and task['hash']['valid'] else ''
            sql_type = task['sqlType'] if 'sqlType' in task else 'unknown'
            impala_duration = task['duration']
            impala_sql = task['impalaSql']
            tidb_sql = task['tidbSql'] if task['success'] else ''
            if len(impala_sql) > 1e6 or len(tidb_sql) > 1e6:
                big_sql = tidb_sql
                impala_sql = tidb_sql = 'big_sql'
            else:
                big_sql = ''
            if '_parquet_' in impala_sql or 'NDV(' in impala_sql or 'background:true' in impala_sql \
                or 'impala_query_log' in impala_sql:
                finish_task_queue.put(query_id)
                continue
            if second_time:
                col = 'tidb_sql2'
            else:
                col = 'tidb_sql'
            sql = f'insert into test.translate_sqls (query_id, order_id, hash_id, tenant, db, sql_type, impala_sql, impala_duration, {col}, sql_date)' +\
                    f'values("{query_id}", nextval(test.seq), "{hash_id}", "{tenant}", "{db}", "{sql_type}", "{escape_string(impala_sql)}", \
                        {impala_duration}, "{escape_string(tidb_sql)}", "{sql_date}")' +\
                    f'on duplicate key update {col}=values({col})'
            sql_err = ''
            if not task['success']:
                err_msg = task['error']
                sql_err = 'insert into test.translate_err (query_id, sql_date, hash_id, err_msg, catalog)' +\
                    f'values("{query_id}", "{_date}", "{hash_id}", "{escape_string(err_msg)}", "translate_error")' +\
                    'on duplicate key update err_msg=values(err_msg)'
                # ?????????????????????????????????
                err_msg_lower = err_msg.lower()
                if '????????????????????????' in err_msg_lower and (\
                    'regexp_replace' in err_msg_lower or \
                    'regexp_extract' in err_msg_lower or \
                    'instr' in err_msg_lower):
                    sql = ''
                    sql_err = ''
                    big_sql = ''
            # tableau??????????????????????????????????????????sql?????????
            if 'x___' in impala_sql or '/*& requestId' in impala_sql:
                sql = ''
                sql_err = ''
                big_sql = ''
        except Exception as e:
            logger.error(f'parse api response {query_id} error: {task}')
            continue
        try:
            if sql:
                utils.exec_tidb_sql(conn, sql)
            if sql_err:
                utils.exec_tidb_sql(conn, sql_err)
                with lock:
                    share_dict['error_count'] = share_dict['error_count'] + 1
            if big_sql:
                save_big_sql(query_id, big_sql)
            finish_task_queue.put(query_id)
        except Exception as e:
            logger.error(f'save tidb error: {query_id} \n {e}')

def clean_task_action(share_dict, lock, task_queue: Queue, finish_task_queue: Queue):
    cursor = utils.get_impala_cursor() 
    finish_count = 0
    batch_size = 1000
    max_wait_sec = 3
    while True:
        start = time.time()
        while finish_task_queue.qsize() == 0 or \
            (finish_task_queue.qsize() < batch_size and (time.time() - start) < max_wait_sec):
            time.sleep(0.1)
        query_ids = []
        real_count = 0
        for i in range(min(batch_size, finish_task_queue.qsize())):
            query_ids.append(f'"{finish_task_queue.get()}"')
            real_count += 1
        sql = f'update dp_stat.impala_query_log_{_date} set saved_in_tidb = true where query_id in ({",".join(query_ids)})'
        try:
            utils.exec_impala_sql(cursor, sql)
            finish_count += real_count
        except Exception as e:
            logger.error(f'save impala result error: {e}')
            time.sleep(1)
        msg = f'finish: {finish_count}, fail: {share_dict["error_count"]}, '
        msg = msg + f'tps: {round(finish_count / (time.time() - share_dict["start_time"]))}, '
        msg = msg + f'queue: {task_queue.qsize()}, '
        msg = msg + f'to_clean: {finish_task_queue.qsize()}'
        logger.info(msg)

def start_fill_task_procs(thread_count):
    for i in range(thread_count):
        proc = Process(target=fill_task_action, name=f'proc-fill-task-{i}', 
            args=(_share_dict, _lock, _task_queue))
        proc.daemon = True
        _fill_task_procs.append(proc)
    for proc in _fill_task_procs:
        proc.start()

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

def run(second_time):
    do_clean()
    global _share_dict
    manager = Manager()
    _share_dict = manager.dict()
    _share_dict['sql_date'] = _date
    _share_dict['second_time'] = second_time
    _share_dict['error_count'] = 0
    logger.info('waiting for the first batch......')
    start_fill_task_procs(12)
    while _task_queue.qsize() == 0:
        time.sleep(0.1)
    _share_dict['start_time'] = time.time()
    start_exec_task_procs(10)
    start_clean_task_proc()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        for proc in _fill_task_procs:
            proc.terminate()
        for proc in _exec_task_procs:
            proc.terminate()
        _clean_task_proc.terminate()
        logger.info('KeyboardInterrupt')

if __name__ == '__main__':
    second_time = False
    if len(sys.argv) == 2 and sys.argv[1] == 'second':
        second_time = True
    run(second_time)
