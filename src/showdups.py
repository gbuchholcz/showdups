import argparse
import sqlite3
from pathlib import Path
import glob
import hashlib
import base64
from more_itertools import chunked
import pdb
import signal
import sys
from timeit import default_timer as timer
import math

stop_signal = False


class UserAbortException(Exception):
    pass


def signal_handler(sig, frame):
    global stop_signal
    stop_signal = True
    print('Closing the process...')


signal.signal(signal.SIGINT, signal_handler)

# showdups [rootFolder] --project --eval-only --overwrite


def scan(root_folder, project, overwrite):
    start = timer()

    project_exists = Path.is_file(project)

    if overwrite and project_exists:
        Path.unlink(project)

    try:
        db_connection = create_connection(project)
        create_database(db_connection)
        files_to_scan_count = store_files_to_scan_in_db(
            db_connection, root_folder)
        print(f'Total files found in root folder: {files_to_scan_count}')
        remove_deleted_fileitems_from_db(db_connection)
        scan_stats = store_file_hashes(db_connection)
        print(
            f'Total files scanned: {scan_stats["file-count"]}; Total file size: {scan_stats["total-file-size"]}')
    except UserAbortException:
        print('\nScan aborted by user')
    finally:
        end = timer()
        ellapsed_time = math.floor(end-start)
        hours = math.floor(ellapsed_time / 3600)
        mins = math.floor((ellapsed_time - hours * 3600) / 60)
        secs = (ellapsed_time - hours * 3600 - mins * 60)
        print(f'Scan ran for {hours}:{mins}:{secs}')
        if db_connection:
            db_connection.close()


def store_file_hashes(db_connection):
    global stop_signal

    stats = {'file-count': 0, 'total-file-size': 0}
    files = db_connection.execute('''
        SELECT si.FullPath
        FROM ScanItem si
                LEFT JOIN FileItem fi
                    ON si.FullPath = fi.FullPath
        WHERE fi.Id IS NULL
    ''')
    chunk_size = 8192
    previous_path = ''
    for file in files:
        if stop_signal:
            raise UserAbortException()
        full_path = file[0]
        hasher = hashlib.sha256()
        with open(full_path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if chunk:
                    hasher.update(chunk)
                else:
                    break
        file_info = Path(full_path)
        hash_value = base64.b64encode(hasher.digest())
        file_size = file_info.stat().st_size
        filename = file_info.name
        path = str(file_info.parent)
        db_connection.execute('INSERT INTO FileItem (FullPath, FileName, Path, Hash, SizeInBytes) VALUES(?, ?, ?, ?, ?);',
                              (full_path, filename, path, hash_value, file_size))
        if not path == previous_path:
            print(f'\rProcessing folder: {path}', end='\r')
            previous_path = path
        stats['total-file-size'] += file_size
        stats['file-count'] += 1
        db_connection.commit()
    return stats


def remove_deleted_fileitems_from_db(db_connection):
    db_connection.execute('''
        DELETE FROM FileItem
        WHERE FullPath IN (
            SELECT si.FullPath
            FROM ScanItem si
                    LEFT JOIN FileItem fi
                        ON fi.FullPath = si.FullPath
            WHERE fi.FullPath IS NULL
        );
    ''')
    db_connection.commit()


def store_files_to_scan_in_db(db_connection, root_folder):
    global stop_signal

    file_count = 0
    for file_batch in chunked(filter(lambda f: Path(f).is_file(), glob.iglob(str(root_folder) + '/**', recursive=True)), 100):
        if stop_signal:
            raise UserAbortException()
        file_batch_elements = [(f,) for f in file_batch]
        file_count += len(file_batch_elements)
        db_connection.executemany(
            'INSERT INTO ScanItem(FullPath) VALUES(?)', file_batch_elements)
        db_connection.commit()
    return file_count


def eval(project):
    pass


def create_database(db_connection):
    db_connection.executescript('''
        CREATE TABLE IF NOT EXISTS FileItem (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            FullPath TEXT NOT NULL,
            FileName TEXT NOT NULL,
            Path TEXT NOT NULL,
            Hash TEXT NOT NULL,
            SizeInBytes INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ScanItem (
            FullPath TEXT PRIMARY KEY
        );
        CREATE INDEX IF NOT EXISTS IX_FileItem_FullPath ON FileItem (FullPath);
        CREATE INDEX IF NOT EXISTS IX_FileItem_Path ON FileItem (Path);
        CREATE INDEX IF NOT EXISTS IX_FileItem_Hash ON FileItem (Hash);
    ''')
    db_connection.commit()


def create_connection(db_file):
    if not db_file:
        db_file = ':memory:'
    return sqlite3.connect(db_file)


def check_directory(value):
    if not Path(value).is_dir():
        raise argparse.ArgumentTypeError("%s is not a folder" % value)
    return Path(value)


def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'root_folder',
        help='The root folder of the scan.',
        metavar='FOLDER',
        type=check_directory,
        nargs='?')
    parser.add_argument(
        '--project',
        help='Project file that stores the scan result (default: in memory).',
        type=lambda v: Path(v),
        nargs='?')
    parser.add_argument(
        '--eval-only',
        help='No scan is executed, only the given project is evaluated.',
        nargs='?',
        default=False,
        const=True,
        type=str2bool
    )
    parser.add_argument(
        '--overwrite',
        help='Given an existing project, if this flag is set, the previously stored data is deiscarded',
        nargs='?',
        default=False,
        const=True,
        type=str2bool)
    args = parser.parse_args()
    print(args)
    if args.eval_only:
        eval(args.project)
    else:
        scan(args.root_folder, args.project, args.overwrite)
