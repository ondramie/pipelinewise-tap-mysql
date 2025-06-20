#!/usr/bin/env python3
# pylint: disable=missing-function-docstring

import pendulum
import singer
from singer import metadata

from tap_mysql.connection import connect_with_backoff
from tap_mysql.sync_strategies import common

BOOKMARK_KEYS = {'replication_key', 'replication_key_value', 'version'}
LOGGER = singer.get_logger('tap_mysql')


def sync_table(mysql_conn, catalog_entry, state, columns, config):  # pylint: disable=too-many-statements
    batch_size = int(config.get('batch_size', 1000))
    common.whitelist_bookmark_keys(BOOKMARK_KEYS, catalog_entry.tap_stream_id, state)

    catalog_metadata = metadata.to_map(catalog_entry.metadata)
    stream_metadata = catalog_metadata.get((), {})

    replication_key_metadata = stream_metadata.get('replication-key')
    replication_key_state = singer.get_bookmark(state,
                                                catalog_entry.tap_stream_id,
                                                'replication_key')

    replication_key_value = None

    if replication_key_metadata == replication_key_state:
        replication_key_value = singer.get_bookmark(state,
                                                    catalog_entry.tap_stream_id,
                                                    'replication_key_value')
    else:
        state = singer.write_bookmark(state,
                                      catalog_entry.tap_stream_id,
                                      'replication_key',
                                      replication_key_metadata)
        state = singer.clear_bookmark(state, catalog_entry.tap_stream_id, 'replication_key_value')

    LOGGER.info('Starting incremental sync with replication_key: %s, value: %s',
                replication_key_metadata, replication_key_value)

    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    state = singer.write_bookmark(state,
                                  catalog_entry.tap_stream_id,
                                  'version',
                                  stream_version)

    activate_version_message = singer.ActivateVersionMessage(
        stream=catalog_entry.stream,
        version=stream_version
    )

    singer.write_message(activate_version_message)

    with connect_with_backoff(mysql_conn) as open_conn:
        with open_conn.cursor() as cur:
            # Track whether we need to continue processing batches
            processed_count = 0
            more_data = True
            current_replication_key_value = replication_key_value

            if replication_key_metadata is None:
                # If no replication key exists, just do a full table sync
                LOGGER.info("No replication key found for stream %s, doing full table sync", catalog_entry.stream)
                select_sql = common.generate_select_sql(catalog_entry, columns)
                params = {}
                common.sync_query(cur,
                                catalog_entry,
                                state,
                                select_sql,
                                columns,
                                stream_version,
                                params,
                                config)
                LOGGER.info('Incremental sync complete. Processed %s records.', processed_count)
                return

            # Main cursor-based pagination loop (more efficient than OFFSET for large datasets)
            while more_data:
                select_sql = common.generate_select_sql(catalog_entry, columns)
                params = {}

                # Use cursor-based pagination instead of OFFSET for better performance
                if current_replication_key_value is not None:
                    # Check if replication_key_value is already a DateTime object or needs parsing
                    if (catalog_entry.schema.properties[replication_key_metadata].format == 'date-time' and
                            not isinstance(current_replication_key_value, pendulum.DateTime)):
                        current_replication_key_value = pendulum.parse(current_replication_key_value)

                    # Use > for cursor-based pagination to avoid duplicates
                    select_sql += f" WHERE `{replication_key_metadata}` > %(replication_key_value)s " \
                                f"ORDER BY `{replication_key_metadata}` ASC " \
                                f"LIMIT {batch_size}"

                    params['replication_key_value'] = current_replication_key_value

                    LOGGER.info('Processing batch: replication_key %s > %s, limit %s (cursor-based)',
                                replication_key_metadata, current_replication_key_value, batch_size)
                else:
                    # First batch - use >= to include the starting value
                    select_sql += f' ORDER BY `{replication_key_metadata}` ASC ' \
                                f' LIMIT {batch_size}'

                    LOGGER.info('Processing first batch: replication_key %s, limit %s',
                                replication_key_metadata, batch_size)

                # Execute query and process with fetchmany for memory efficiency
                cur.execute(select_sql, params)

                # Get fetch_size from config for memory-efficient processing
                from tap_mysql.constants import (
                    DEFAULT_FETCH_SIZE,  # pylint: disable=import-outside-toplevel
                )
                fetch_size = config.get('fetch_size', DEFAULT_FETCH_SIZE) if config else DEFAULT_FETCH_SIZE

                batch_row_count = 0
                time_extracted = singer.utils.now()
                last_replication_key_value = current_replication_key_value

                # Process records in smaller chunks using fetchmany for memory efficiency
                while True:
                    rows = cur.fetchmany(fetch_size)
                    if not rows:
                        break

                    for row in rows:
                        batch_row_count += 1
                        processed_count += 1

                        record_message = common.write_record_message(catalog_entry,
                                                                     stream_version,
                                                                     row,
                                                                     columns,
                                                                     time_extracted)

                        # Update the cursor for next batch
                        last_replication_key_value = record_message.record[replication_key_metadata]

                        # Store the current replication key value in state
                        state = singer.write_bookmark(state,
                                                    catalog_entry.tap_stream_id,
                                                    'replication_key',
                                                    replication_key_metadata)

                        state = singer.write_bookmark(state,
                                                    catalog_entry.tap_stream_id,
                                                    'replication_key_value',
                                                    last_replication_key_value)

                        # Write state periodically to enable resumption
                        if processed_count % 1000 == 0:
                            singer.write_message(singer.StateMessage(value=state))

                LOGGER.info('Processed %s records in this batch, %s total', batch_row_count, processed_count)

                # Write state after each batch
                singer.write_message(singer.StateMessage(value=state))

                # If we got fewer records than requested, we're done
                if batch_row_count < batch_size:
                    more_data = False
                else:
                    # Update cursor for next batch
                    current_replication_key_value = last_replication_key_value

            LOGGER.info('Incremental sync complete. Processed %s records.', processed_count)
