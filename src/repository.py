import sqlite3

_db_connection = None


class RepositoryException(Exception):
    def __init__(self, message):
        self.message = message


def initialize_database(db_file):
    global _db_connection
    if not db_file:
        db_file = ':memory:'
    _db_connection = sqlite3.connect(db_file)


def close_database():
    global _db_connection
    if not _db_connection:
        raise RepositoryException(
            'Call initialize_database before accessing the Db')
    if _db_connection:
        _db_connection.close()


def create_database():
    global _db_connection
    if not _db_connection:
        raise RepositoryException(
            'Call initialize_database before accessing the Db')
    cursor = _db_connection.cursor()
    cursor.executescript('''
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
    _db_connection.commit()


def delete_unmatched_file_items():
    global _db_connection
    if not _db_connection:
        raise RepositoryException(
            'Call initialize_database before accessing the Db')
    cursor = _db_connection.cursor()
    cursor.execute('''
        DELETE FROM FileItem
        WHERE FullPath IN (
            SELECT si.FullPath
            FROM ScanItem si
                    LEFT JOIN FileItem fi
                        ON fi.FullPath = si.FullPath
            WHERE fi.FullPath IS NULL
        );
    ''')
    _db_connection.commit()


def insert_scan_item(scan_item_batch):
    global _db_connection
    if not _db_connection:
        raise RepositoryException(
            'Call initialize_database before accessing the Db')
    scan_items = [(f,) for f in scan_item_batch]
    cursor = _db_connection.cursor()
    cursor.executemany(
        'INSERT INTO ScanItem(FullPath) VALUES(?)', scan_items)
    _db_connection.commit()
    return len(scan_item_batch)


def insert_file_item(full_path, filename, path, hash_value, file_size):
    global _db_connection
    if not _db_connection:
        raise RepositoryException(
            'Call initialize_database before accessing the Db')
    cursor = _db_connection.cursor()
    cursor.execute('INSERT INTO FileItem (FullPath, FileName, Path, Hash, SizeInBytes) VALUES(?, ?, ?, ?, ?);',
                   (full_path, filename, path, hash_value, file_size))
    _db_connection.commit()


def query_unprocessed_scan_items():
    global _db_connection
    if not _db_connection:
        raise RepositoryException(
            'Call initialize_database before accessing the Db')
    cursor = _db_connection.cursor()
    cursor.execute('''
            SELECT si.FullPath
            FROM ScanItem si
                    LEFT JOIN FileItem fi
                        ON si.FullPath = fi.FullPath
            WHERE fi.Id IS NULL
    ''')
    return cursor


def count_unprocessed_scan_items():
    global _db_connection
    if not _db_connection:
        raise RepositoryException(
            'Call initialize_database before accessing the Db')
    cursor = _db_connection.cursor()
    cursor.execute('''
            SELECT COUNT(*)
            FROM ScanItem si
                    LEFT JOIN FileItem fi
                        ON si.FullPath = fi.FullPath            
            WHERE fi.Id IS NULL
    ''')
    return cursor.fetchone()[0]
