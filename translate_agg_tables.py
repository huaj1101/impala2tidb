from email import charset
import os
import sys
import logging
from textwrap import indent
import threading
from impala.util import as_pandas
import time
import utils
import datetime
from typing import List
import pandas as pd
import translate_utils
import requests
from pymysql.converters import escape_string

logger = logging.getLogger(__name__)

def translate_sql(impala_sql):
    host = utils.conf.get('translate', 'api-host')
    url = f'{host}/transfer-by-text'
    try:
        response = requests.post(url, data=impala_sql.encode('utf-8'), headers={"Content-Type": "text/plain"})
    except Exception as e:
        logger.error(f'api error: {e}')
    if response.status_code != 200:
        logger.error(response.text)
    return response.text


def main():
    hdfs_client = utils.get_hdfs_client()
    agg_sql_path = utils.conf.get('hdfs', 'agg_sql_path')
    for file_name in hdfs_client.list(agg_sql_path):
        if file_name.endswith('.sql'):
            output_f = open(f'agg_tables/{file_name}', 'w', encoding='utf-8')
            file_path = agg_sql_path + '/' + file_name
            with hdfs_client.read(file_path, encoding='utf-8') as reader:
                sql_text = reader.read()
            for impala_sql in sql_text.split(';'):
                if not impala_sql: continue
                tidb_sql = translate_sql(impala_sql).replace('global_dw_1', 'global_dw')
                output_f.write(tidb_sql)
                output_f.write(';\n')
            output_f.close()

if __name__ == '__main__':
    main()
