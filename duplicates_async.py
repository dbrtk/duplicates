#!/usr/bin/python3

import sys
import os
import hashlib
import pprint
import stat
import asyncio
import argparse

BUFSIZE = 8 * 1024
BYTEORDER = sys.byteorder

CURR_DIR = os.getcwd()
FILES_DIR = ''

pp = pprint.PrettyPrinter(indent=4)


@asyncio.coroutine
def hash_file(path):
    """
    """
    block_size = BUFSIZE
    hasher = hashlib.md5()
    with open(path, 'rb') as the_file:
        while True:
            data = the_file.read(block_size)
            if not data:
                break
            hasher.update(data)
    return path, hasher.hexdigest()


@asyncio.coroutine
def generate_hashes(_paths: list = []):
    """
    """
    mapping = dict()
    subtasks = [asyncio.Task(hash_file(_path)) for _path in _paths]
    results = yield from asyncio.gather(*subtasks)
    for path, unique_hash in results:
        if unique_hash not in mapping:
            mapping[unique_hash] = []
        mapping[unique_hash].append(dict(path=path))
    return mapping


@asyncio.coroutine
def verify_duplicates(duplicates):
    """
    """
    coroutines = [generate_hashes([item.get('path') for item in items])
                  for key, items in iter(duplicates.items())]
    results = yield from asyncio.gather(*coroutines)
    return dict((k, v) for obj in results for
                k, v in iter(obj.items()) if len(v) >= 2)


def get_dups_based_on_os_stat():
    """ This method uses os.stat to analyse the file, but not their content..
    This is more getting similarities between files.
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


def main(check_content=False):
    """
    """
    loop = asyncio.get_event_loop()

    duplicates = get_dups_based_on_os_stat()
    if check_content:
        duplicates = loop.run_until_complete(verify_duplicates(duplicates))

    for _, _list in iter(duplicates.items()):
        print('The following files seem to be similar: \n')
        for item in _list:
            print('\t\t\t%s' % os.path.split(item.get('path'))[1])
        else:
            print('\n')
    if not duplicates:
        print('\nNo duplicates found.\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'directory', help='The directory with files.',
        type=str)
    parser.add_argument('--check-content', default=False, action='store_true',
                        help='Use in order to compare duplicates based on'
                        ' their content.',
                        dest='check_content')
    args = vars(parser.parse_args())
    directory = args['directory']
    check_content = args['check_content']
    try:
        _files = directory
    except IndexError:
        raise RuntimeError('A path to a directory with files is missing.')
    else:
        FILES_DIR = os.path.join(CURR_DIR, _files)
    finally:
        main(check_content=check_content)
