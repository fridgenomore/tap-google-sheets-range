import time

import singer
from singer import metrics, metadata, Transformer, utils
from singer.utils import strptime_to_utc, strftime
from singer.messages import RecordMessage
from tap_google_sheets_range.streams import STREAMS

LOGGER = singer.get_logger()


def write_schema(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    try:
        singer.write_schema(stream_name, schema, stream.key_properties)
        LOGGER.info('Writing schema for: {}'.format(stream_name))
    except OSError as err:
        LOGGER.info('OS Error writing schema for: {}'.format(stream_name))
        raise err


def write_record(stream_name, record, time_extracted, version=None):
    try:
        if version:
            singer.messages.write_message(
                RecordMessage(
                    stream=stream_name,
                    record=record,
                    version=version,
                    time_extracted=time_extracted))
        else:
            singer.messages.write_record(
                stream_name=stream_name,
                record=record,
                time_extracted=time_extracted)
    except OSError as err:
        LOGGER.info('OS Error writing record for: {}'.format(stream_name))
        raise err


def get_bookmark(state, stream, default):
    if (state is None) or ('bookmarks' not in state):
        return default
    return (
        state
        .get('bookmarks', {})
        .get(stream, default)
    )


def write_bookmark(state, stream, value):
    if 'bookmarks' not in state:
        state['bookmarks'] = {}
    state['bookmarks'][stream] = value
    LOGGER.info('Write state for stream: {}, value: {}'.format(stream, value))
    singer.write_state(state)


# Currently syncing sets the stream currently being delivered in the state.
# If the integration is interrupted, this state property is used to identify
#  the starting point to continue from.
# Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
def update_currently_syncing(state, stream_name):
    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


# Transform/validate batch of records w/ schema and sent to target
def process_records(catalog,
                    stream_name,
                    records,
                    time_extracted,
                    version=None):
    stream = catalog.get_stream(stream_name)
    schema = stream.schema.to_dict()
    stream_metadata = metadata.to_map(stream.metadata)
    with metrics.record_counter(stream_name) as counter:
        for record in records:
            # Transform record for Singer.io
            with Transformer() as transformer:
                try:
                    transformed_record = transformer.transform(
                        record,
                        schema,
                        stream_metadata)
                except Exception as err:
                    LOGGER.error('{}'.format(err))
                    raise RuntimeError(err)
                write_record(
                    stream_name=stream_name,
                    record=transformed_record,
                    time_extracted=time_extracted,
                    version=version)
                counter.increment()
        return counter.value


# List selected fields from stream catalog
def get_selected_fields(catalog, stream_name):
    stream = catalog.get_stream(stream_name)
    mdata = metadata.to_map(stream.metadata)
    mdata_list = singer.metadata.to_list(mdata)
    selected_fields = []
    for entry in mdata_list:
        field = None
        try:
            field = entry['breadcrumb'][1]
            if entry.get('metadata', {}).get('selected', False):
                selected_fields.append(field)
        except IndexError:
            pass
    return selected_fields


def sync_stream(stream_name, selected_streams, catalog, state, records, time_extracted=None):
    # Should sheets_loaded be synced?
    if stream_name in selected_streams:
        LOGGER.info('STARTED Syncing {}'.format(stream_name))
        update_currently_syncing(state, stream_name)
        selected_fields = get_selected_fields(catalog, stream_name)
        LOGGER.info('Stream: {}, selected_fields: {}'.format(stream_name, selected_fields))
        write_schema(catalog, stream_name)
        if not time_extracted:
            time_extracted = utils.now()
        record_count = process_records(
            catalog=catalog,
            stream_name=stream_name,
            records=records,
            time_extracted=time_extracted)
        LOGGER.info('FINISHED Syncing {}, Total Records: {}'.format(stream_name, record_count))
        update_currently_syncing(state, None)


def sync(client, config, catalog, state):
    # Get selected_streams from catalog based on last sync state
    # last_stream = if sync was interrupted contains the last syncing stream
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info('last/currently syncing stream: {}'.format(last_stream))
    selected_streams = []
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
    LOGGER.info('selected_streams: {}'.format(selected_streams))

    if not selected_streams:
        return

    # FILE_METADATA
    file_metadata = {}
    stream_name = 'file_metadata'
    stream_object = STREAMS.get(stream_name)(client, config, state)

    file_metadata, time_extracted = stream_object.sync()
    file_mtime = strptime_to_utc(file_metadata[0].get('modifiedTime'))

    # Check if file has changed else break
    start_date = config.start_date
    last_mtime = strptime_to_utc(get_bookmark(state, stream_name, start_date))
    LOGGER.info('File mtime [last_mtime = {}, file_mtime = {}]'.format(last_mtime, file_mtime))
    if file_mtime <= last_mtime:
        LOGGER.info('file_mtime <= last_mtime, FILE IS NOT CHANGED. EXITING.')
        write_bookmark(state, 'file_metadata', strftime(file_mtime))
        return
    # Sync file_metadata if selected
    # file_metadata bookmark is updated at the end of sync
    sync_stream(stream_object.stream_name, selected_streams, catalog, state, file_metadata, time_extracted)

    sheets = config.list_sheets()

    # Loop throw sheets (worksheet tabs) in spreadsheet
    for sheet_name in sheets:
        stream_object = STREAMS.get('sheet')(client, config, state, sheet_name)
        stream_name = stream_object.stream_name

        update_currently_syncing(state, stream_name)
        selected_fields = get_selected_fields(catalog, stream_name)
        LOGGER.info('Stream: [{}], selected_fields: [{}]'.format(stream_name, selected_fields))
        write_schema(catalog, stream_name)

        # Emit a Singer ACTIVATE_VERSION message before initial sync (but not subsequent syncs)
        # everytime after each sheet sync is complete.
        # This forces hard deletes on the data downstream if fewer records are sent.
        # https://github.com/singer-io/singer-python/blob/master/singer/messages.py#L137
        last_integer = int(get_bookmark(state, stream_name, 0))
        activate_version = int(time.time() * 1000)
        activate_version_message = singer.ActivateVersionMessage(
            stream=stream_name,
            version=activate_version)
        if last_integer == 0:
            # initial load, send activate_version before AND after data sync
            singer.write_message(activate_version_message)
            LOGGER.info(
                'INITIAL SYNC, Stream: [{}], Activate Version: [{}]'.format(stream_name, activate_version))

        total_row = 0
        for sheet_records, time_extracted in stream_object.sync():
            # Process records, send batch of records to target
            record_count = process_records(
                catalog=catalog,
                stream_name=stream_object.stream_name,
                records=sheet_records,
                time_extracted=time_extracted,
                version=None)
            LOGGER.info('Sheet: [{}], records processed: [{}]'.format(sheet_name, record_count))
            total_row += record_count

        # End of Stream: Send Activate Version and update State
        singer.write_message(activate_version_message)
        write_bookmark(state, stream_name, activate_version)
        LOGGER.info('COMPLETED Syncing Sheet [{}], Total Rows: [{}]'.format(sheet_name, total_row))
        update_currently_syncing(state, None)

    # Update file_metadata bookmark
    write_bookmark(state, 'file_metadata', strftime(file_mtime))

    return
