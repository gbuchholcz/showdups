import unittest
from src import repository as repo
import pdb


class ConnectionTests(unittest.TestCase):

    def tearDown(self):
        repo.close_connection(suppress_error=True)

    def test_create_database_without_initialized_connection(self):
        with self.assertRaises(repo.RepositoryException):
            repo.create_database()

    def test_close_without_initialized_connection(self):
        with self.assertRaises(repo.RepositoryException):
            repo.close_connection()

    def test_close_without_initialized_connection_and_suppressed_errors(self):
        repo.close_connection(suppress_error=True)

    def test_query_unprocessed_scan_items_without_initialized_connection(self):
        with self.assertRaises(repo.RepositoryException):
            repo.query_unprocessed_scan_items()

    def test_count_unprocessed_scan_items_without_initialized_connection(self):
        with self.assertRaises(repo.RepositoryException):
            repo.count_unprocessed_scan_items()

    def test_insert_file_item_without_initialized_connection(self):
        with self.assertRaises(repo.RepositoryException):
            repo.insert_file_item(full_path='', filename='',
                                  path='', hash_value='', file_size=0)

    def test_insert_scan_item_without_initialized_connection(self):
        with self.assertRaises(repo.RepositoryException):
            repo.insert_scan_item([''])

    def test_remove_deleted_fileitems_from_db_without_initialized_connection(self):
        with self.assertRaises(repo.RepositoryException):
            repo.delete_unmatched_file_items()

    def test_initialize_connection_twice_fails(self):
        repo.initialize_connection()
        with self.assertRaises(repo.RepositoryException):
            repo.initialize_connection()


if __name__ == '__main__':
    unittest.main()
