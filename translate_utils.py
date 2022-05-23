import logging

logger = logging.getLogger(__name__)

def get_error_catalog(sql: str, err_msg: str):
    sql = sql.lower()
    err_msg = err_msg.lower()
    if 'duplicate entry' in err_msg and 'insert into' in err_msg:
        return "ignore_duplicate_insert"
    if 'only_full_group_by' in err_msg:
        return 'unsupport_group_by'
    if 'incorrect datetime value' in err_msg:
        return 'unsupport_date_format'
    if 'full join' in sql or 'full outer join' in sql:
        return 'unsupport_full_join'
    if 'cannot be null' in err_msg:
        return 'unsupport_null_value'
    if 'invalid transaction' in err_msg:
        return 'delay_too_many_union'
    if 'regexp_replace' in sql or 'regexp_extract' in sql or 'instr(' in sql:
        return 'unsupport_func'
    if 'background:true' in sql:
        return 'ignore_etl'
    if 'x__' in sql:
        return 'ignore_tableau'
    if 'table' in err_msg and "doesn't exist" in err_msg:
        return 'ignore_schema_mismatch'
    return 'not_processed'
