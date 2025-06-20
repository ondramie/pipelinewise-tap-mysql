#!/usr/bin/env python3
# pylint: disable=too-many-locals,missing-function-docstring

import copy

import singer
from singer import metadata

from tap_mysql.connection import connect_with_backoff
from tap_mysql.sync_strategies import binlog, common

LOGGER = singer.get_logger('tap_mysql')


def generate_bookmark_keys(catalog_entry):
    md_map = metadata.to_map(catalog_entry.metadata)
    stream_metadata = md_map.get((), {})
    replication_method = stream_metadata.get('replication-method')

    base_bookmark_keys = {'last_pk_fetched', 'max_pk_values', 'version', 'initial_full_table_complete'}

    if replication_method == 'FULL_TABLE':
        bookmark_keys = base_bookmark_keys
    else:
        bookmark_keys = base_bookmark_keys.union(binlog.BOOKMARK_KEYS)

    return bookmark_keys


def pks_are_auto_incrementing(mysql_conn, catalog_entry):
    database_name = common.get_database_name(catalog_entry)
    key_properties = common.get_key_properties(catalog_entry)

    if not key_properties:
        return False

    sql = """SELECT 1
               FROM information_schema.columns
              WHERE table_schema = '{}'
                AND table_name = '{}'
                AND column_name = '{}'
                AND extra LIKE '%auto_increment%'
    """

    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            for primary_key in key_properties:
                cur.execute(sql.format(database_name,
                                       catalog_entry.table,
                                       primary_key))

                result = cur.fetchone()

                if not result:
                    return False

    return True


def get_max_pk_values(cursor, catalog_entry):
    database_name = common.get_database_name(catalog_entry)
    escaped_db = common.escape(database_name)
    escaped_table = common.escape(catalog_entry.table)

    key_properties = common.get_key_properties(catalog_entry)
    escaped_columns = [common.escape(c) for c in key_properties]

    sql = """SELECT {}
               FROM {}.{}
              ORDER BY {}
              LIMIT 1
    """

    select_column_clause = ", ".join(escaped_columns)
    order_column_clause = ", ".join([primary_key + " DESC" for primary_key in escaped_columns])

    cursor.execute(sql.format(select_column_clause,
                              escaped_db,
                              escaped_table,
                              order_column_clause))
    result = cursor.fetchone()

    if result:
        max_pk_values = dict(zip(key_properties, result))
    else:
        max_pk_values = {}

    return max_pk_values


def generate_pk_clause(catalog_entry, state):
    key_properties = common.get_key_properties(catalog_entry)
    escaped_columns = [common.escape(c) for c in key_properties]

    max_pk_values = singer.get_bookmark(state,
                                        catalog_entry.tap_stream_id,
                                        'max_pk_values')

    last_pk_fetched = singer.get_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'last_pk_fetched')

    if last_pk_fetched:
        # For cursor-based pagination, use > for the last fetched value
        # This is more efficient than OFFSET for large datasets
        if len(key_properties) == 1:
            # Single primary key - simple cursor-based pagination
            pk = key_properties[0]
            escaped_pk = common.escape(pk)
            sql = f' WHERE {escaped_pk} > {last_pk_fetched[pk]} AND {escaped_pk} <= {max_pk_values[pk]} ORDER BY {escaped_pk} ASC'
        else:
            # Composite primary key - use tuple comparison for cursor-based pagination
            pk_comparisons = []
            for pk in key_properties:
                escaped_pk = common.escape(pk)
                pk_comparisons.append(f"{escaped_pk} <= {max_pk_values[pk]}")

            # Create tuple comparison for cursor-based pagination
            pk_tuple = f"({', '.join([common.escape(pk) for pk in key_properties])})"
            last_pk_tuple = f"({', '.join([str(last_pk_fetched[pk]) for pk in key_properties])})"

            sql = f' WHERE {pk_tuple} > {last_pk_tuple} AND {" AND ".join(pk_comparisons)} ORDER BY {", ".join(escaped_columns)} ASC'
    else:
        # First batch
        pk_comparisons = [f"{common.escape(pk)} <= {max_pk_values[pk]}" for pk in key_properties]
        sql = f' WHERE {" AND ".join(pk_comparisons)} ORDER BY {", ".join(escaped_columns)} ASC'

    return sql


def sync_table(mysql_conn, catalog_entry, state, columns, stream_version, config=None):  # pylint: disable=too-many-arguments
    common.whitelist_bookmark_keys(generate_bookmark_keys(catalog_entry), catalog_entry.tap_stream_id, state)

    bookmark = state.get('bookmarks', {}).get(catalog_entry.tap_stream_id, {})
    version_exists = 'version' in bookmark

    initial_full_table_complete = singer.get_bookmark(state,
                                                      catalog_entry.tap_stream_id,
                                                      'initial_full_table_complete')

    state_version = singer.get_bookmark(state,
                                        catalog_entry.tap_stream_id,
                                        'version')

    activate_version_message = singer.ActivateVersionMessage(
        stream=catalog_entry.stream,
        version=stream_version
    )

    # For the initial replication, emit an ACTIVATE_VERSION message
    # at the beginning so the records show up right away.
    if not initial_full_table_complete and not (version_exists and state_version is None):
        singer.write_message(activate_version_message)

    key_props_are_auto_incrementing = pks_are_auto_incrementing(mysql_conn, catalog_entry)

    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            select_sql = common.generate_select_sql(catalog_entry, columns)

            # Get batch size from config for memory-efficient processing
            batch_size = int(config.get('batch_size', 10000)) if config else 10000

            if key_props_are_auto_incrementing:
                LOGGER.info("Detected auto-incrementing primary key(s) - will replicate incrementally with cursor-based pagination")
                max_pk_values = singer.get_bookmark(state,
                                                    catalog_entry.tap_stream_id,
                                                    'max_pk_values') or get_max_pk_values(cur, catalog_entry)

                if not max_pk_values:
                    LOGGER.info("No max value for auto-incrementing PK found for table %s", catalog_entry.table)
                    params = {}
                    # Use regular sync_query for tables without data
                    common.sync_query(cur,
                                      catalog_entry,
                                      state,
                                      select_sql,
                                      columns,
                                      stream_version,
                                      params,
                                      config)
                else:
                    state = singer.write_bookmark(state,
                                                  catalog_entry.tap_stream_id,
                                                  'max_pk_values',
                                                  max_pk_values)

                    # Use cursor-based pagination for efficient large table processing
                    processed_count = 0
                    more_data = True

                    while more_data:
                        pk_clause = generate_pk_clause(catalog_entry, state)
                        batch_sql = select_sql + pk_clause + f" LIMIT {batch_size}"

                        LOGGER.info("Processing batch with cursor-based pagination, limit %s", batch_size)

                        params = {}
                        cur.execute(batch_sql, params)

                        # Get fetch_size from config for memory-efficient processing
                        from tap_mysql.constants import (
                            DEFAULT_FETCH_SIZE,  # pylint: disable=import-outside-toplevel
                        )
                        fetch_size = config.get('fetch_size', DEFAULT_FETCH_SIZE) if config else DEFAULT_FETCH_SIZE

                        batch_row_count = 0
                        time_extracted = singer.utils.now()

                        # Process records in smaller chunks using fetchmany for memory efficiency
                        while True:
                            rows = cur.fetchmany(fetch_size)
                            if not rows:
                                break

                            for row in rows:
                                batch_row_count += 1
                                processed_count += 1

                                record_message = common.write_record_message(catalog_entry, stream_version, row, columns, time_extracted)

                                # Update last_pk_fetched for cursor-based pagination
                                key_properties = common.get_key_properties(catalog_entry)
                                last_pk_fetched = {k: v for k, v in record_message.record.items() if k in key_properties}

                                state = singer.write_bookmark(state,
                                                              catalog_entry.tap_stream_id,
                                                              'last_pk_fetched',
                                                              last_pk_fetched)

                                # Write state periodically to enable resumption
                                if processed_count % 1000 == 0:
                                    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

                        LOGGER.info('Processed %s records in this batch, %s total', batch_row_count, processed_count)

                        # Write state after each batch
                        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

                        # If we got fewer records than requested, we're done
                        if batch_row_count < batch_size:
                            more_data = False

                    LOGGER.info('Full table sync complete. Processed %s records.', processed_count)
            else:
                # For tables without auto-incrementing keys, use regular sync_query
                LOGGER.info("No auto-incrementing primary key detected - using standard sync")
                params = {}
                common.sync_query(cur,
                                  catalog_entry,
                                  state,
                                  select_sql,
                                  columns,
                                  stream_version,
                                  params,
                                  config)

    # clear max pk value and last pk fetched upon successful sync
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, 'max_pk_values')
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, 'last_pk_fetched')

    singer.write_message(activate_version_message)
