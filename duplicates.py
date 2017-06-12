#!/usr/bin/python3

import sys
import os
import hashlib
import pprint
import stat
import asyncio

BUFSIZE = 8 * 1024
BYTEORDER = sys.byteorder

CURR_DIR = os.getcwd()
FILES_DIR = ''

pp = pprint.PrettyPrinter(indent=4)


def hash_file(path):
    """
    """
    block_size = BUFSIZE
    hasher = hashlib.md5()
    with open(path, 'rb', buffering=0) as the_file:
        while True:
            data = the_file.read(block_size)
            if not data:
                break
            hasher.update(data)
    return hasher.hexdigest()


def generate_hashes(_paths: list = []):
    """
    """
    mapping = dict()
    for item in _paths:
        unique_hash = hash_file(item)
        if unique_hash not in mapping:
            mapping[unique_hash] = []
        mapping[unique_hash].append(item)
    else:
        pass
    return mapping


def get_dups_based_on_os_stat():
    """
    """
    out = dict()
    for item in iter_files():
        hasher = hashlib.md5()

        hasher.update(b'%r' % item.get('size'))
        hasher.update(b'%r' % stat.S_IFMT(item.get('mode')))
        hasher.update(b'%r' % item.get('blocks'))

        key = hasher.hexdigest()
        if key not in out:
            out[key] = []
        out[key].append(item)
    return dict((k, v) for k, v in iter(out.items()) if len(v) > 1)


def iter_files():
    """
    """
    for item in os.listdir(FILES_DIR):
        path = os.path.join(FILES_DIR, item)
        if not (os.path.isfile(path)):
            raise ValueError('The file: %r does not exist.' % path)
        _st = os.stat(path)
        yield dict(
            path=path,
            blocks=_st.st_blocks,
            blocksize=_st.st_blksize,
            ino=_st.st_ino,
            device=_st.st_dev,
            hard_links=_st.st_nlink,
            mode=_st.st_mode,
            size=_st.st_size
        )


def main():
    """
    """
    duplicates = get_dups_based_on_os_stat()

    for _, _list in iter(duplicates.items()):
        print('The following files seem to be similar: \n')
        for item in _list:
            print('\t\t\t%s' % os.path.split(item.get('path'))[1])

        # todo(): implement comparison on the files hashes to see if they
        # are identical bit by bit. This does not work.
        # hashes = generate_hashes(
        #     _paths=(item.get('path') for item in _list))
        # pp.pprint(hashes)


if __name__ == '__main__':
    try:
        _files = sys.argv[1]
    except IndexError:
        raise RuntimeError('A path to a directory with files is missing.')
    else:
        FILES_DIR = os.path.join(CURR_DIR, _files)
    finally:
        main()
