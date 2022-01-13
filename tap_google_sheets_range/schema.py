import singer
from singer import metadata
from tap_google_sheets_range.streams import STREAMS

LOGGER = singer.get_logger()


def get_schemas(client, config):
    schemas = {}
    field_metadata = {}
    all_streams = []
    for stream_name, stream_object in STREAMS.items():
        if stream_name == 'file_metadata':
            all_streams.append(stream_object(client, config))
        elif stream_name in ['sheet_data']:
            sheets = config.list_sheets()
            for sheet_name in sheets:
                all_streams.append(stream_object(client, config, None, sheet_name))


    for stream in all_streams:
        stream_name = stream.stream_name
        LOGGER.info("Starting synchronize schema [Stream: {}]".format(stream_name))
        schema = stream.get_schema()

        schemas[stream_name] = schema
        mdata = metadata.new()

        # Documentation:
        # https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#singer-python-helper-functions
        # Reference:
        # https://github.com/singer-io/singer-python/blob/master/singer/metadata.py#L25-L44
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream.key_properties,
            valid_replication_keys=stream.replication_keys,
            replication_method=stream.replication_method
        )
        field_metadata[stream_name] = mdata
        LOGGER.info("Completed synchronize schema [Stream: {}]".format(stream_name))
    return schemas, field_metadata
