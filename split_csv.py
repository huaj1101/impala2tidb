import os
import utils
import logging

logger = logging.getLogger(__name__)

MAX_SIZE = 256 * 1024 * 1024
SEP = 7

def _find_sep_pos(buf, sep):
    for i in range(len(buf)):
        if buf[i] == sep:
            return i
    return -1

def _splie_file(file, target_dir, prefix, post_fix_index):
    buf_size = 1024 * 1024
    block_size = 0
    index = post_fix_index
    parts = 1
    target_f = open(os.path.join(target_dir, f'{prefix}{index:0>3d}.csv'), 'wb')
    with open(file, 'rb') as src_f:
        while True:
            buf = src_f.read(buf_size)
            buf_len = len(buf)
            if buf_len == 0:
                target_f.close()
                break
            if block_size + buf_len * 2 < MAX_SIZE:
                block_size += buf_len
                target_f.write(buf)
            else:
                pos = _find_sep_pos(buf, SEP) + 1
                if pos == 0:
                    raise Exception('sep not found in buffer')
                target_f.write(buf[:pos])
                target_f.close()
                parts += 1
                index += 1
                block_size = buf_len - pos
                target_f = open(os.path.join(target_dir, f'{prefix}{index:0>3d}.csv'), 'wb')
                target_f.write(buf[pos:])
    return parts

def split_files(src_dir, target_dir, prefix, remove_src=False):
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    if not prefix.endswith('.'):
        prefix = prefix + '.'
    files = []
    for file in os.listdir(src_dir):
        if file.startswith(prefix):
            files.append(file)
    files.sort()
    post_fix_index = 1
    for file in files:
        file_with_path = os.path.join(src_dir, file)
        file_size = os.path.getsize(file_with_path)
        if file_size <= MAX_SIZE:
            target_file = os.path.join(target_dir, file)
            if remove_src:
                os.system(f'mv {file_with_path} {target_file}')
            else:
                os.system(f'cp {file_with_path} {target_file}')
            split_count = 1
        else:
            split_count = _splie_file(file_with_path, target_dir, prefix, post_fix_index)
            if remove_src:
                os.system(f'rm {file_with_path}')
        post_fix_index += split_count
    return post_fix_index - 1

@utils.timeit
def split_dir(src_dir, target_dir, remove_src=False):
    prefixes = []
    for file in os.listdir(src_dir):
        if file.endswith('.csv'):
            parts = file.split('.')
            prefixes.append(parts[0] + '.' + parts[1])
    prefixes = list(set(prefixes))
    total_count = len(prefixes)
    for i, prefix in enumerate(prefixes):
        split_count = split_files(src_dir, target_dir, prefix, remove_src)
        logger.info(f'{i + 1} / {total_count} split {prefix} into {split_count} done')

# prefix = 'global_mtlp.m_gh_plan_check'
# prefix = 'global_ipm.formula'
# split_files('data2', 'data_split', prefix)
# split_dir('data2', 'data_split', False)