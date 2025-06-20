import datetime
import unittest
from unittest.mock import MagicMock, patch

import pendulum
import singer
from singer import Schema

from tap_mysql.connection import MySQLConnection
from tap_mysql.sync_strategies import incremental


class TestIncrementalSyncStrategy(unittest.TestCase):

    def setUp(self):
        self.maxDiff = None

    def tearDown(self):
        pass

    def test_sync_table_with_no_replication_key(self):
        """Test sync_table when there's no replication key defined."""
        mysql_conn = MagicMock(spec=MySQLConnection)
        catalog_entry = MagicMock()
        catalog_entry.tap_stream_id = 'test-stream'
        catalog_entry.stream = 'test_stream'
        catalog_entry.schema = Schema(
            properties={
                'id': Schema(inclusion='available', type=['null', 'integer']),
                'val': Schema(inclusion='available', type=['null', 'string'])
            }
        )
        catalog_entry.metadata = [
            {'breadcrumb': (), 'metadata': {'selected': True, 'database-name': 'test_db'}}
        ]

        state = {}
        columns = ['id', 'val']
        config = {'batch_size': 1000}

        # Mock connect_with_backoff and cursor
        mock_open_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__.return_value = mock_cursor
        mock_open_conn.__enter__.return_value = mock_open_conn
        mock_open_conn.cursor.return_value = mock_cursor

        with patch('tap_mysql.sync_strategies.incremental.connect_with_backoff') as mock_connect:
            mock_connect.return_value = mock_open_conn

            # Mock common.sync_query
            with patch('tap_mysql.sync_strategies.common.sync_query') as mock_sync_query:
                mock_sync_query.return_value = 1  # Mock return value doesn't matter here

                incremental.sync_table(mysql_conn, catalog_entry, state, columns, config)

                # Assert that sync_query was called with the right parameters for the full table sync
                mock_sync_query.assert_called_once()
                args = mock_sync_query.call_args[0]
                assert len(args) == 8  # Check number of args (added config parameter)
                assert args[0] == mock_cursor  # cursor
                assert args[1] == catalog_entry  # catalog_entry
                # Remaining args: state, select_sql, columns, stream_version, params, config

    @patch('singer.write_message')
    def test_sync_table_with_batching(self, mock_write_message):
        """Test batched sync with a replication key."""
        mysql_conn = MagicMock(spec=MySQLConnection)
        catalog_entry = MagicMock()
        catalog_entry.tap_stream_id = 'test-stream'
        catalog_entry.stream = 'test_stream'
        catalog_entry.schema = Schema(
            properties={
                'id': Schema(inclusion='available', type=['null', 'integer']),
                'val': Schema(inclusion='available', type=['null', 'string']),
                'updated_at': Schema(inclusion='available', type=['null', 'string'], format='date-time')
            }
        )
        catalog_entry.metadata = [
            {'breadcrumb': (), 'metadata': {'selected': True, 'database-name': 'test_db', 'replication-key': 'updated_at'}}
        ]

        state = {
            'bookmarks': {
                'test-stream': {
                    'replication_key': 'updated_at',
                    'replication_key_value': '2022-01-01T00:00:00+00:00',
                    'version': 1
                }
            }
        }
        columns = ['id', 'val', 'updated_at']
        config = {'batch_size': 2}  # Small batch size to test pagination

        # Mock connect_with_backoff and cursor
        mock_open_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__.return_value = mock_cursor
        mock_open_conn.__enter__.return_value = mock_open_conn
        mock_open_conn.cursor.return_value = mock_cursor

        # Setup mock to return batches using fetchmany pattern
        batch1 = [
            (1, 'value1', datetime.datetime(2022, 1, 2, 0, 0, 0)),
            (2, 'value2', datetime.datetime(2022, 1, 3, 0, 0, 0))
        ]
        batch2 = [
            (3, 'value3', datetime.datetime(2022, 1, 4, 0, 0, 0))
        ]

        # Mock fetchmany to return batches in sequence
        def mock_fetchmany(size):
            if mock_cursor.fetchmany.call_count == 1:
                return batch1
            elif mock_cursor.fetchmany.call_count == 2:
                return []  # End first query
            elif mock_cursor.fetchmany.call_count == 3:
                return batch2
            else:
                return []  # End second query

        mock_cursor.fetchmany.side_effect = mock_fetchmany

        with patch('tap_mysql.sync_strategies.incremental.connect_with_backoff') as mock_connect:
            mock_connect.return_value = mock_open_conn

            # Mock row_to_singer_record
            with patch('tap_mysql.sync_strategies.common.row_to_singer_record') as mock_row_to_singer_record:
                mock_record_messages = [
                    singer.RecordMessage(stream='test_stream', record={'id': 1, 'val': 'value1', 'updated_at': '2022-01-02T00:00:00+00:00'}, version=1),
                    singer.RecordMessage(stream='test_stream', record={'id': 2, 'val': 'value2', 'updated_at': '2022-01-03T00:00:00+00:00'}, version=1),
                    singer.RecordMessage(stream='test_stream', record={'id': 3, 'val': 'value3', 'updated_at': '2022-01-04T00:00:00+00:00'}, version=1),
                ]
                mock_row_to_singer_record.side_effect = mock_record_messages

                incremental.sync_table(mysql_conn, catalog_entry, state, columns, config)

                # Check that execute was called with the expected number of times (2: two batches, algorithm stops when batch has fewer records than batch_size)
                self.assertEqual(mock_cursor.execute.call_count, 2)

                # Verify record messages were written
                record_message_writes = [c for c in mock_write_message.call_args_list if isinstance(c[0][0], singer.RecordMessage)]
                self.assertEqual(len(record_message_writes), 3)

                # Check that state messages were written (one after each non-empty batch)
                state_message_writes = [c for c in mock_write_message.call_args_list if isinstance(c[0][0], singer.StateMessage)]
                self.assertEqual(len(state_message_writes), 2)  # One after each non-empty batch

                # Verify final state reflects latest replication key value
                self.assertEqual(state['bookmarks']['test-stream']['replication_key_value'], '2022-01-04T00:00:00+00:00')

    @patch('singer.write_message')
    def test_sync_table_pagination(self, mock_write_message):
        """Test pagination with batches smaller than batch_size."""
        mysql_conn = MagicMock(spec=MySQLConnection)
        catalog_entry = MagicMock()
        catalog_entry.tap_stream_id = 'test-stream'
        catalog_entry.stream = 'test_stream'
        catalog_entry.schema = Schema(
            properties={
                'id': Schema(inclusion='available', type=['null', 'integer']),
                'val': Schema(inclusion='available', type=['null', 'string']),
                'updated_at': Schema(inclusion='available', type=['null', 'integer'])  # Integer replication key
            }
        )
        catalog_entry.metadata = [
            {'breadcrumb': (), 'metadata': {'selected': True, 'database-name': 'test_db', 'replication-key': 'updated_at'}}
        ]

        state = {
            'bookmarks': {
                'test-stream': {
                    'replication_key': 'updated_at',
                    'replication_key_value': 100,
                    'version': 1
                }
            }
        }
        columns = ['id', 'val', 'updated_at']
        config = {'batch_size': 2}  # Small batch size to test pagination

        # Mock connect_with_backoff and cursor
        mock_open_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__.return_value = mock_cursor
        mock_open_conn.__enter__.return_value = mock_open_conn
        mock_open_conn.cursor.return_value = mock_cursor

        # Setup mock to return multiple batches to test pagination using fetchmany pattern
        batch1 = [(1, 'value1', 101), (2, 'value2', 102)]  # Full batch
        batch2 = [(3, 'value3', 103)]  # Partial batch (should end pagination)

        # Mock fetchmany to return batches in sequence for pagination test
        def mock_fetchmany_pagination(size):
            if mock_cursor.fetchmany.call_count == 1:
                return batch1
            elif mock_cursor.fetchmany.call_count == 2:
                return []  # End first query
            elif mock_cursor.fetchmany.call_count == 3:
                return batch2
            else:
                return []  # End second query

        mock_cursor.fetchmany.side_effect = mock_fetchmany_pagination

        with patch('tap_mysql.sync_strategies.incremental.connect_with_backoff') as mock_connect:
            mock_connect.return_value = mock_open_conn

            # Mock row_to_singer_record
            with patch('tap_mysql.sync_strategies.common.row_to_singer_record') as mock_row_to_singer_record:
                mock_record_messages = [
                    singer.RecordMessage(stream='test_stream', record={'id': 1, 'val': 'value1', 'updated_at': 101}, version=1),
                    singer.RecordMessage(stream='test_stream', record={'id': 2, 'val': 'value2', 'updated_at': 102}, version=1),
                    singer.RecordMessage(stream='test_stream', record={'id': 3, 'val': 'value3', 'updated_at': 103}, version=1),
                ]
                mock_row_to_singer_record.side_effect = mock_record_messages

                incremental.sync_table(mysql_conn, catalog_entry, state, columns, config)

                # Verify cursor.execute was called twice (once for each batch)
                self.assertEqual(mock_cursor.execute.call_count, 2)

                # Both calls should use cursor-based pagination with WHERE clause
                first_call = mock_cursor.execute.call_args_list[0]
                self.assertIn('WHERE `updated_at` >', first_call[0][0])
                self.assertIn('LIMIT 2', first_call[0][0])

                # Second call should also use cursor-based pagination
                second_call = mock_cursor.execute.call_args_list[1]
                self.assertIn('WHERE `updated_at` >', second_call[0][0])
                self.assertIn('LIMIT 2', second_call[0][0])

                # Verify final state has the latest replication key value
                self.assertEqual(state['bookmarks']['test-stream']['replication_key_value'], 103)

    @patch('singer.write_message')
    def test_sync_table_date_parsing(self, mock_write_message):
        """Test that date-time replication keys are properly parsed."""
        mysql_conn = MagicMock(spec=MySQLConnection)
        catalog_entry = MagicMock()
        catalog_entry.tap_stream_id = 'test-stream'
        catalog_entry.stream = 'test_stream'
        catalog_entry.schema = Schema(
            properties={
                'id': Schema(inclusion='available', type=['null', 'integer']),
                'updated_at': Schema(inclusion='available', type=['null', 'string'], format='date-time')
            }
        )
        catalog_entry.metadata = [
            {'breadcrumb': (), 'metadata': {'selected': True, 'database-name': 'test_db', 'replication-key': 'updated_at'}}
        ]

        state = {
            'bookmarks': {
                'test-stream': {
                    'replication_key': 'updated_at',
                    'replication_key_value': '2023-01-01T00:00:00+00:00',
                    'version': 1
                }
            }
        }
        columns = ['id', 'updated_at']
        config = {'batch_size': 100}

        mock_open_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.__enter__.return_value = mock_cursor
        mock_open_conn.__enter__.return_value = mock_open_conn
        mock_open_conn.cursor.return_value = mock_cursor

        # Return a single record using fetchmany pattern
        def mock_fetchmany_date_parsing(size):
            if mock_cursor.fetchmany.call_count == 1:
                return [(1, datetime.datetime(2023, 1, 2, 0, 0, 0))]
            else:
                return []  # End query

        mock_cursor.fetchmany.side_effect = mock_fetchmany_date_parsing

        with patch('tap_mysql.sync_strategies.incremental.connect_with_backoff') as mock_connect:
            mock_connect.return_value = mock_open_conn

            # Mock row_to_singer_record
            with patch('tap_mysql.sync_strategies.common.row_to_singer_record') as mock_row_to_singer_record:
                mock_record_messages = [
                    singer.RecordMessage(stream='test_stream', record={'id': 1, 'updated_at': '2023-01-02T00:00:00+00:00'}, version=1),
                ]
                mock_row_to_singer_record.side_effect = mock_record_messages

                with patch('pendulum.parse') as mock_parse:
                    # Mock pendulum.parse to return a parsed datetime
                    mock_parse.return_value = pendulum.datetime(2023, 1, 1, 0, 0, 0)

                    incremental.sync_table(mysql_conn, catalog_entry, state, columns, config)

                    # Verify pendulum.parse was called with the replication key value
                    mock_parse.assert_called_with('2023-01-01T00:00:00+00:00')

                    # Verify SQL call included the parsed datetime
                    mock_cursor.execute.assert_called()

                    # Verify final state has the new replication key value
                    self.assertEqual(state['bookmarks']['test-stream']['replication_key_value'], '2023-01-02T00:00:00+00:00')
