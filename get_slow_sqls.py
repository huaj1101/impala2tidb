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
import re

logger = logging.getLogger(__name__)

def main():
    cursor = utils.get_impala_cursor_prod()
    sql = '''
SELECT db, user, statement, hash_id
FROM dp_stat.impala_sql_statistics 
where type = 'slow' 
      and query_state = 'FINISHED' 
      and duration < 30
      and statement not like '%...%' 
order by created_at, hash_id
'''
    cursor.execute(sql)
    df = as_pandas(cursor)
    cursor.close()
    pattern_user = "(.*)_internal_(write|read)"
    pattern_statement = r'(/\*&.*\*/)'
    count = 0
    cursor = utils.get_impala_cursor()
    cursor.execute('delete from default.slow_sqls')
    for i in range(len(df)):
        db = df.at[i, 'db']
        user = df.at[i, 'user']
        hash_id = df.at[i, 'hash_id']
        statement = df.at[i, 'statement']
        statement = re.sub(pattern_statement, '', statement)
        if '{{tenant}}' in statement:
            match = re.search(pattern_user, user)
            if match:
                tenant = match.group(1)
            elif db.endswith('_custom'):
                tenant = db.replace('_custom', '')
            else:
                print(f'skip one:\n{statement}')
                continue
            statement = statement.replace('{{tenant}}', tenant)
        sql = 'insert into default.slow_sqls (id, db, statement, hash_id) values (?, ?, ?, ?)'
        cursor.execute(sql, [i + 1, db, statement, hash_id], {'paramstyle': 'qmark'})
        count += 1
        if count % 100 == 0:
            print(f'{count} / {len(df)} inserted')

    cursor.close()
    if count % 100 != 0:
        print(f'{count} / {len(df)} inserted')


if __name__ == '__main__':
    main()