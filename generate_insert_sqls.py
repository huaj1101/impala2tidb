import logging
from impala.util import as_pandas
import utils

logger = logging.getLogger(__name__)

def cell_to_str(cellvalue, type):
    if cellvalue is None:
        return 'NULL'
    if type == 'string':
        return f'"{cellvalue}"'
    if 'int' in type or type in ['double', 'boolean'] or type.startswith('decimal'):
        return f'{cellvalue}'
    if type == 'timestamp':
        time_str = cellvalue.strftime("%Y-%m-%d %H:%M:%S")
        return f'"{time_str}"'

def gen_insert_sql(table, sql):
    cursor = utils.get_impala_cursor()
    utils.exec_impala_sql(cursor, f'describe {table}')
    df_schema = as_pandas(cursor)
    utils.exec_impala_sql(cursor, sql)
    df_data = as_pandas(cursor)

    cols = []
    types = []
    for i in range(len(df_schema)):
        cols.append(df_schema.at[i, 'name'])
        types.append(df_schema.at[i, 'type'])

    cols_str = ', '.join([f'`{col}`' for col in cols])
    sql_insert = f'insert into {table} ({cols_str}) values \n'
    sql_values = []
    for i in range(len(df_data)):
        col_values = []
        for j, col in enumerate(cols):
            cellvalue = df_data.at[i, col]
            col_values.append(cell_to_str(cellvalue, types[j]))
            # print(f'{col}  {type}  {cellvalue} {"NULL" if cellvalue is None else "not Null"}')
        sql_values.append(f'({", ".join(col_values)})')
    return sql_insert + ',\n'.join(sql_values)

def main():
    lines = open('error_data.txt', 'r', encoding='utf-8').readlines()
    sqls = []
    for i in range(0, len(lines), 2):
        if not lines[i]: break
        table = lines[i]
        sql = lines[i + 1]
        sqls.append(gen_insert_sql(table, sql))
    with open('data/data_by_sqls.txt', 'w', encoding='utf-8') as f:
        f.write('\n;\n'.join(sqls))

if __name__ == '__main__':
    main()