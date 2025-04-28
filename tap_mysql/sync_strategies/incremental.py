#!/usr/bin/env python3
# pylint: disable=missing-function-docstring

import pendulum
import singer
from singer import metadata

from tap_mysql.connection import connect_with_backoff
from tap_mysql.sync_strategies import common


BOOKMARK_KEYS = {'replication_key', 'replication_key_value', 'version'}


def sync_table(mysql_conn, catalog_entry, state, columns, config):
    batch_size = config['batch_size']
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
            has_more_data = True
            records_processed = 0
            
            # Continue fetching batches until no more data is available
            while has_more_data:
                select_sql = common.generate_select_sql(catalog_entry, columns)
                params = {}
                
                if replication_key_metadata is None:
                    # If no replication key exists, we just do one batch and exit
                    has_more_data = False
                    common.sync_query(cur,
                                     catalog_entry,
                                     state,
                                     select_sql,
                                     columns,
                                     stream_version,
                                     params)
                    continue
                
                # If we have a replication key, we can do proper batching
                if replication_key_value is not None:
                    if catalog_entry.schema.properties[replication_key_metadata].format == 'date-time':
                        replication_key_value = pendulum.parse(replication_key_value)

                    select_sql += f" WHERE `{replication_key_metadata}` > %(replication_key_value)s " \
                                 f"ORDER BY `{replication_key_metadata}` ASC" \
                                 f" LIMIT {batch_size}"

                    params['replication_key_value'] = replication_key_value
                else:
                    select_sql += f' ORDER BY `{replication_key_metadata}` ASC' \
                                 f' LIMIT {batch_size}'
                
                # Execute the query and process the batch
                singer.get_logger('tap_mysql').info(f'Processing batch with replication_key_value: {replication_key_value}')
                rows_fetched = common.sync_query(cur,
                                              catalog_entry,
                                              state,
                                              select_sql,
                                              columns,
                                              stream_version,
                                              params)
                
                records_processed += rows_fetched
                singer.get_logger('tap_mysql').info(f'Processed {rows_fetched} records in this batch, {records_processed} records total')
                
                # Get updated replication key value from state
                new_replication_key_value = singer.get_bookmark(state,
                                                          catalog_entry.tap_stream_id,
                                                          'replication_key_value')
                
                # If we got fewer records than batch_size or no records, we're done
                if rows_fetched < batch_size:
                    has_more_data = False
                    singer.get_logger('tap_mysql').info('Completed processing all batches')
                            # If the replication key value didn't change, we're stuck in a loop
                elif new_replication_key_value == replication_key_value:
                    has_more_data = False
                    singer.get_logger('tap_mysql').warning('Replication key value did not change, stopping to avoid infinite loop')
                else:
                    # Update the replication key value for the next batch
                    replication_key_value = new_replication_key_value
