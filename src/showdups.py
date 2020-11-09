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
import repository as repo

_stop_signal = False


class UserAbortException(Exception):
    pass


def signal_handler(sig, frame):
    global _stop_signal
    _stop_signal = True
    print('Closing the process...')


signal.signal(signal.SIGINT, signal_handler)

# showdups [rootFolder] --project --eval-only --overwrite


def scan(root_folder, project, overwrite):
    start = timer()

    project_exists = project and Path.is_file(project)

    if overwrite and project_exists:
        Path.unlink(project)

    try:
        repo.initialize_database(project)
        repo.create_database()
        files_to_scan_count = collect_all_scan_items(root_folder)
        print(f'Total files found in root folder: {files_to_scan_count}')
        repo.delete_unmatched_file_items()
        scan_stats = store_file_hashes()
        print(
            f'Total files scanned: {scan_stats["file-count"]}; Total file size: {scan_stats["total-file-size"]}')
    except UserAbortException:
        print('\nScan aborted by user')
    finally:
        repo.close_database()
        end = timer()
        ellapsed_time = math.floor(end-start)
        hours = math.floor(ellapsed_time / 3600)
        mins = math.floor((ellapsed_time - hours * 3600) / 60)
        secs = (ellapsed_time - hours * 3600 - mins * 60)
        print(f'Scan ran for {hours}:{mins}:{secs}')


def store_file_hashes():
    global _stop_signal
    processed_file_count = 0
    processed_file_size = 0
    unprocessed_file_count = repo.count_unprocessed_scan_items()
    with repo.query_unprocessed_scan_items() as files_resultset:
        for file in files_resultset:
            if _stop_signal:
                raise UserAbortException()
            full_path = file[0]
            hash_value = hash_file(full_path)
            file_info = Path(full_path)
            file_size = file_info.stat().st_size
            filename = file_info.name
            path = str(file_info.parent)
            repo.insert_file_item(full_path, filename,
                                  path, hash_value, file_size)
            processed_file_count += 1
            processed_file_size += file_size
            print(
                f'Processing {processed_file_count:7}/{unprocessed_file_count:7}', end='\r')
    print()
    return {'file-count': processed_file_count, 'total-file-size': processed_file_size}


def hash_file(full_path):
    chunk_size = 8192
    hasher = hashlib.sha256()
    with open(full_path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if chunk:
                hasher.update(chunk)
            else:
                break
    return base64.b64encode(hasher.digest())


def collect_all_scan_items(root_folder):
    global _stop_signal
    file_count = 0
    for scan_item_batch in chunked(filter(lambda f: Path(f).is_file(), glob.iglob(str(root_folder) + '/**', recursive=True)), 100):
        if _stop_signal:
            raise UserAbortException()
        file_count += repo.insert_scan_item(scan_item_batch)
    return file_count


def eval(project):
    pass


def check_directory(value):
    if not value or not Path(value).is_dir():
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
