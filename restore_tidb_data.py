import os
import time
import sys
import logging
import utils

logger = logging.getLogger(__name__)
#scp_csv_folder = '/csv-data1/csv'
#restore_csv_folder = '/csv-data1/csv-batch'
scp_csv_folder = '/data2/translate/csv'
restore_csv_folder = '/data2/translate/csv-batch'
size_th = 50 * 1024 * 1024 * 1024
count_th = 500
time_th = 5 * 60

tables_loaded = set()
tables_to_restore = []
last_restore_batch = time.time()
total_restore_count = 0

def calc_batch():
    total_size = 0
    total_count = 0
    batch = []
    for table, size in tables_to_restore:
        total_size += size
        total_count += 1
        batch.append(table)
    if total_size > size_th:
        logger.info('prepare to run a batch, file_count: %d, total_size: %.2f g' % 
            (total_count, total_size / 1024 / 1024 / 1024))
        return batch
    if total_count >= count_th:
        logger.info('prepare to run a batch, file_count: %d, total_size: %.2f g' % 
            (total_count, total_size / 1024 / 1024 / 1024))
        return batch
    if batch and time.time() - last_restore_batch > time_th:
        logger.info('prepare to run a batch, wait too long, file_count: %d, total_size: %.2f g' % 
            (total_count, total_size / 1024 / 1024 / 1024))
        return batch
    return []

def run_batch(batch):
    global last_restore_batch, total_restore_count
    last_restore_batch = time.time()
    for table in batch:
        for i in range(len(tables_to_restore) - 1, -1, -1):
            if tables_to_restore[i][0] == table:
                del tables_to_restore[i]
        cmd = f'mv {scp_csv_folder}/{table}.* {restore_csv_folder}/'
        os.system(cmd)
    cmd = 'tiup tidb-lightning -config tidb-lightning.toml'
    result = os.system(cmd)
    cmd = f'mv {restore_csv_folder}/* {scp_csv_folder}/'
    os.system(cmd)
    if result != 0:
        logger.error(f'lightning error, result: {result}, end program')
        sys.exit(1)
    total_restore_count += len(batch)
    logger.info(f'{total_restore_count} tables restored')

def restore():
    global last_restore_batch
    while True:
        # 从扫描到第一个文件开始计时
        if len(tables_loaded) == 0:
            last_restore_batch = time.time()
        for file in os.listdir(scp_csv_folder):
            if file.endswith('.finish'):
                table = file.replace('.finish', '')
                if table in tables_loaded:
                    continue
                with open(os.path.join(scp_csv_folder, file), 'r') as f:
                    s_size = f.readline()
                size = int(s_size)
                tables_to_restore.append((table, size))
                tables_loaded.add(table)
                logger.info(f'{table} scp ready')
        tables_to_restore.sort(key=lambda item: item[1], reverse=True)
        batch = calc_batch()
        if batch:
            run_batch(batch)
        else:
            time.sleep(5)

def init():
    finish_files = {}
    for file in os.listdir(scp_csv_folder):
        if file.endswith('.finish'):
            table = file.replace('.finish', '')
            finish_files[table] = 0
    for file in os.listdir(scp_csv_folder):
        if file.endswith('.csv'):
            table = '.'.join(file.split('.')[:2])
            size = os.path.getsize(os.path.join(scp_csv_folder, file))
            finish_files[table] = finish_files[table] + size
    for table in finish_files:
        with open(os.path.join(scp_csv_folder, table + '.finish'), 'w') as f:
            f.write(str(finish_files[table]))
        print(f'{table}: {finish_files[table]}')

if __name__ == '__main__':
    restore()