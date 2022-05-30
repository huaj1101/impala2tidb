import logging
import json
import utils

logger = logging.getLogger(__name__)

def _translate_str_length(length):
    if length <= 50:
        return 50
    if length <= 255:
        return 255
    if length <= 2000:
        return 2000
    if length <= 16383: # varchar类型最大长度
        return 16383
    if length <= 65535: # text类型最大长度
        return 65535
    return 4294967295 # longtext类型

def _translate_type(impala_type, length):
    tidb_type = ''
    if impala_type == 'string':
        tidb_len = _translate_str_length(length)
        if tidb_len <= 16383:
            tidb_type = f'varchar({tidb_len}) character set utf8mb4'
        elif tidb_len <= 65535:
            tidb_type = f'text({tidb_len}) character set utf8mb4'
        else:
            tidb_type = 'longtext character set utf8mb4'
    elif impala_type == 'bigint':
        tidb_type = 'bigint'
    elif impala_type == 'int':
        tidb_type = 'int'
    elif impala_type == 'tinyint':
        tidb_type = 'tinyint'
    elif impala_type == 'double':
        tidb_type = 'double'
    elif impala_type == 'timestamp':
        tidb_type = 'datetime'
    elif impala_type == 'boolean':
        tidb_type = 'boolean'
    elif impala_type.startswith('decimal'):
        tidb_type = impala_type
    else:
        raise Exception(f'unprocessed type {impala_type}')
    return tidb_type

def _translate_default_value(tidb_type, default_value):
    if default_value and tidb_type.startswith('varchar'):
        default_value = f'"{default_value}"'
    elif tidb_type == 'datetime':
        if default_value == '0':
            return '"1970-01-01"'
        return ''
    elif tidb_type.startswith('text'):
        return ''
    return default_value

def gen_create_table_sql(table_schema):
    table_name = table_schema['table']
    columns = table_schema['columns']
    primary_keys = table_schema['pk_unique_subset']
    create_sql_lines = []
    create_sql_lines.append(f'create table {table_name}')
    create_sql_lines.append('(')
    for column in columns:
        col_name = column["name"]
        col_type = _translate_type(column['type'], column['len'])
        nullable = column['nullable']
        default_value = _translate_default_value(col_type, column['default_value'])
        null_statment = 'null' if nullable == 'true' else 'not null'
        default_statment = '' if default_value == '' else f'default {default_value}'
        column_statement = f'\t{col_name: <25s}{col_type: <40s}{null_statment: <15s}{default_statment: <20s},'
        create_sql_lines.append(column_statement)
    if primary_keys:
        create_sql_lines.append(f'\tPRIMARY KEY ({primary_keys}) /*T![clustered_index] CLUSTERED */')
    else:
        create_sql_lines[-1] = create_sql_lines[-1].strip(',')
    create_sql_lines.append(')')
    sql = '\n'.join(create_sql_lines)
    return sql

def get_error_catalog(sql: str, err_msg: str):
    sql = sql.lower()
    err_msg = err_msg.lower()
    if 'duplicate entry' in err_msg and 'insert into' in err_msg:
        return "ignore_duplicate_insert"
    if 'only_full_group_by' in err_msg:
        return 'modify_group_by'
    if 'incorrect datetime value' in err_msg:
        return 'modify_date_format'
    if 'full join' in sql or 'full outer join' in sql:
        return 'modify_full_join'
    if 'cannot be null' in err_msg:
        return 'modify_wrong_sql'
    if 'invalid transaction' in err_msg:
        return 'delay'
    if 'regexp_replace' in sql or 'regexp_extract' in sql or 'instr(' in sql:
        return 'modify_func_not_support'
    if 'background:true' in sql:
        return 'ignore_etl'
    if 'x___' in sql or '/*& requestId' in sql:
        return 'ignore_tableau'
    if 'table' in err_msg and "doesn't exist" in err_msg:
        return 'ignore_schema_mismatch'
    if 'sql文本长度超过最大解析长度' in err_msg:
        return 'modify_sql_too_big'
    return 'not_processed'

def _test():
    with open(f'schemas/global_mtlp.json', 'r', encoding='utf-8') as f:
        schema_text = f.read()
        db_schema = json.loads(schema_text)
    for table_schema in db_schema:
        if table_schema['table'] == "`global_mtlp`.`c_gd_link_gh`":
            sql = gen_create_table_sql(table_schema)
            print(sql)
            break
