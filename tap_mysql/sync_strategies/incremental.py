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
            offset = 0
            more_data = True

            # Main batch processing loop
            while more_data:
                select_sql = common.generate_select_sql(catalog_entry, columns)
                params = {}

                if replication_key_metadata is None:
                    # If no replication key exists, just do a full table sync
                    LOGGER.info("No replication key found for stream %s, doing full table sync", catalog_entry.stream)
                    common.sync_query(cur,
                                    catalog_entry,
                                    state,
                                    select_sql,
                                    columns,
                                    stream_version,
                                    params,
                                    config)
                    break

                # Use >= to include records with identical replication key values
                if replication_key_value is not None:
                    # Check if replication_key_value is already a DateTime object or needs parsing
                    if (catalog_entry.schema.properties[replication_key_metadata].format == 'date-time' and
                            not isinstance(replication_key_value, pendulum.DateTime)):
                        replication_key_value = pendulum.parse(replication_key_value)

                    select_sql += f" WHERE `{replication_key_metadata}` >= %(replication_key_value)s " \
                                f"ORDER BY `{replication_key_metadata}` ASC " \
                                f"LIMIT {batch_size} OFFSET {offset}"

                    params['replication_key_value'] = replication_key_value
                else:
                    select_sql += f' ORDER BY `{replication_key_metadata}` ASC ' \
                                f' LIMIT {batch_size} OFFSET {offset}'

                LOGGER.info('Processing batch: replication_key %s >= %s, limit %s, offset %s',
                            replication_key_metadata, replication_key_value, batch_size, offset)

                # Process the records in this batch
                cur.execute(select_sql, params)

                rows = cur.fetchall()
                rows_count = len(rows)

                if rows_count == 0:
                    LOGGER.info('No records returned, ending sync')
                    more_data = False
                    continue

                # Process each row individually (instead of using sync_query) to avoid state updates
                # messing with our logic
                time_extracted = singer.utils.now()
                for row in rows:
                    record_message = common.write_record_message(catalog_entry,
                                                                 stream_version,
                                                                 row,
                                                                 columns,
                                                                 time_extracted)

                    # Store the current replication key value in state
                    if replication_key_metadata:
                        state = singer.write_bookmark(state,
                                                    catalog_entry.tap_stream_id,
                                                    'replication_key',
                                                    replication_key_metadata)

                        state = singer.write_bookmark(state,
                                                    catalog_entry.tap_stream_id,
                                                    'replication_key_value',
                                                    record_message.record[replication_key_metadata])

                # Update our counters
                processed_count += rows_count
                LOGGER.info('Processed %s records in this batch, %s total', rows_count, processed_count)

                # Write state after each batch
                singer.write_message(singer.StateMessage(value=state))

                # If we got fewer records than requested, we're done
                if rows_count < batch_size:
                    more_data = False
                else:
                    # Otherwise, increase the offset and get the next batch
                    offset += batch_size

            LOGGER.info('Incremental sync complete. Processed %s records.', processed_count)
