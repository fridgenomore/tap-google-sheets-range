import json
import re
from datetime import timedelta

import singer
from singer import utils
from singer.utils import strftime
from collections import OrderedDict
from tap_google_sheets_range.utils import excel_to_dttm_str, col_string_to_num, col_num_to_string, encode_string, get_schema_from_file


LOGGER = singer.get_logger()


# streams: API URL endpoints to be called
# properties:
#   path: API endpoint relative path, when added to the base URL, creates the full path,
#       default = stream_name
#   key_properties: Primary key fields for identifying an endpoint record.
#   replication_method: INCREMENTAL or FULL_TABLE
#   replication_keys: bookmark_field(s), typically a date-time, used for filtering the results
#       and setting the state
#   params: Query, sort, and other endpoint specific parameters; default = {}
#   data_key: JSON element containing the results list for the endpoint;
#       default = root (no data_key)
class Stream:
    def __init__(self, client, config, state=None):
        self.client = client
        self.config = config
        self.state = state

    def get_schema(self):
        raise NotImplementedError

    def sync(self):
        raise NotImplementedError

    def get_data(self, stream_name, path, api, params):
        # Encode stream_name: fixes issue with special characters in sheet name
        stream_name_escaped = re.escape(stream_name)
        data = {}
        time_extracted = utils.now()
        data = self.client.get(
            path=path,
            api=api,
            params=params,
            endpoint=stream_name_escaped)
        return data, time_extracted


# file_metadata: Queries Google Drive API to get file information and see if file has been modified
# Provides audit info about who and when last changed the file.
class FileMetadata(Stream):
    stream_id = "file_metadata"
    stream_name = "file_metadata"
    api = "files"
    path = "files/{spreadsheet_id}"
    key_properties = ["id"]
    replication_method = "INCREMENTAL"
    replication_keys = ["modifiedTime"]
    params = {
        "fields": "id,name,size,createdTime,modifiedTime,version,teamDriveId,driveId,lastModifyingUser"
    }

    def get_schema(self):
        return get_schema_from_file(self.stream_name)

    # Transform file_metadata: remove nodes from lastModifyingUser, format as array
    def transform_data(self, data):
        file_metadata_tf = json.loads(json.dumps(data))
        # Remove extra keys
        if file_metadata_tf.get('lastModifyingUser'):
            file_metadata_tf['lastModifyingUser'].pop('photoLink', None)
            file_metadata_tf['lastModifyingUser'].pop('me', None)
            file_metadata_tf['lastModifyingUser'].pop('permissionId', None)
        file_metadata_arr = [file_metadata_tf]
        return file_metadata_arr

    def sync(self):
        path = self.path.replace('{spreadsheet_id}', self.config.spreadsheet_id)
        data, time_extracted = self.get_data(
            stream_name=FileMetadata.stream_name, path=path, api=FileMetadata.api, params=FileMetadata.params)
        records_tf = self.transform_data(data)
        return records_tf, time_extracted


class Sheet(Stream):
    api = "sheets"
    path = "spreadsheets/{spreadsheet_id}"
    key_properties = ["__sdc_row"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    params = {
        "includeGridData": "true",
        "ranges": "{sheet_title}!{sheet_range}"
    }

    def __init__(self, client, config, state, sheet_title):
        super().__init__(client, config, state)
        self.stream_id = config.get_sheet_target_table(sheet_title)
        self.stream_name = config.get_sheet_target_table(sheet_title)
        self.sheet_title = sheet_title

    def get_schema(self):
        headers = self.config.list_sheet_headers(self.sheet_title)
        sheet_json_schema = {
            'type': 'object',
            'additionalProperties': False,
            'properties': {
                "__sdc_load_time": {
                    "type": ["null", "string"],
                    "format": "date-time"
                },
                '__sdc_spreadsheet_id': {
                    'type': ['null', 'string']
                },
                '__sdc_sheet_id': {
                    'type': ['null', 'integer']
                },
                '__sdc_row': {
                    'type': ['null', 'integer']
                },
                '__sdc_is_hidden': {
                    "type": ["null", "string"],
                    "format": "boolean"
                }
            }
        }

        for header in headers:
            column_name = header.name
            col_properties = {}
            if header.type:
                col_properties['type'] = header.type
            else:
                col_properties['type'] = {'type': ['null', 'string']}
            if header.format:
                col_properties['format'] = header.format

            sheet_json_schema['properties'][column_name] = col_properties

        LOGGER.info(f"Schema: {sheet_json_schema}")

        return sheet_json_schema

    def get_sheet_columns_metadata(self):
        columns = []
        first_column_letter = self.config.get_column(self.sheet_title)
        column_index = col_string_to_num(first_column_letter)
        for header in self.config.list_sheet_headers(self.sheet_title):
            column_letter = col_num_to_string(column_index)
            column = {
                'columnIndex': column_index,
                'columnLetter': column_letter,
                'columnName': header.name,
                'columnType': header.type,
                'columnFormat': header.format
            }
            column_index = column_index + 1
            columns.append(column)

        return sorted(columns, key=lambda i: i['columnIndex'])

    # Transform sheet_data: add spreadsheet_id, sheet_id, and row, convert dates/times
    # Convert from array of values to JSON with column names as keys
    def transform_data(self, from_row, spreadsheet_data, extracted_time):
        sheet_data_tf = []
        columns = self.get_sheet_columns_metadata()

        # parse data from spreadsheet_data
        # https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets
        sheet_metadata = spreadsheet_data.get('sheets')[0]
        sheet_id = sheet_metadata.get('properties', {}).get('sheetId')
        data = next(iter(sheet_metadata.get('data', [])), {})
        sheet_row_data = data.get('rowData', [])
        sheet_row_metadata = data.get('rowMetadata', [])

        row_index = from_row
        for i, row in enumerate(sheet_row_data):
            row = row.get('values', [])
            is_empty = True
            sheet_data_row_tf = {'__sdc_load_time': extracted_time,
                                 '__sdc_spreadsheet_id': self.config.spreadsheet_id,
                                 '__sdc_sheet_id': sheet_id,
                                 '__sdc_row': row_index,
                                 '__sdc_is_hidden': sheet_row_metadata[i].get('hiddenByUser', False)}
            col_num = 1
            for value in row:
                # 'effectiveValue': {'numberValue': 44511.45914351852},
                column_effective_value = value.get('effectiveValue', None)
                column_effective_value_type = None

                # 'effectiveFormat': {'numberFormat': {'type': 'DATE', 'pattern': 'M/d/yyyy'}... }
                column_number_format = value.get('effectiveFormat', {}).get('numberFormat', {})
                column_number_format_type = column_number_format.get('type')

                # Select column metadata based on column index
                column = columns[col_num - 1]
                # Get column metadata
                col_name = column.get('columnName')
                col_letter = column.get('columnLetter')

                # Parse effectiveValue type
                if column_effective_value is None or column_effective_value == '':
                    col_val = None
                    # LOGGER.info('WARNING: NO VALUE IN THE {}ND ROW SHEET:[{}], COL:[{}], CELL:[{}{}]'
                    #             .format(row_index, self.sheet_title, col_name, col_letter, row_index))
                elif column_effective_value in ('errorType', 'formulaType'):
                    # col_val = str(val)
                    raise Exception('DATA TYPE ERROR SHEET:[{}], COL:[{}], CELL:[{}{}], TYPE:[{}]'.format(
                        self.sheet_title, col_name, col_letter, row_index, key))
                else:
                    is_empty = False
                    for key, val in column_effective_value.items():
                        column_effective_value_type = key
                        col_val = val
                        # experimental - hyperlink is set in many keys + possible schema validation error
                        # extract hyperlink and cast to string (effective value may be number and etc)
                        # https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#celldata
                        if self.config.is_link(self.sheet_title, col_name):
                            column_effective_value_type = 'stringValue'
                            col_val = value.get('hyperlink') or val

                    try:
                        # replace a value from the list with None
                        if col_val in self.config.null_values:
                            col_val = None
                        # Convert dates/times from Lotus Notes Serial Numbers
                        # DATE-TIME
                        elif column_number_format_type == 'DATE_TIME':
                            col_val = excel_to_dttm_str(col_val) if isinstance(col_val, (int, float)) else col_val
                        # DATE
                        elif column_number_format_type == 'DATE':
                            col_val = excel_to_dttm_str(col_val)[:10] if isinstance(col_val, (int, float)) else col_val
                        # TIME ONLY (NO DATE)
                        elif column_number_format_type == 'TIME':
                            if isinstance(col_val, (int, float)):
                                total_secs = col_val * 86400  # seconds in day
                                # Create string formatted like HH:MM:SS
                                col_val = str(timedelta(seconds=total_secs))
                        # NUMBER (INTEGER AND FLOAT)
                        elif column_effective_value_type == 'numberValue':
                            if isinstance(col_val, int):
                                col_val = int(col_val)
                            elif isinstance(col_val, float):
                                # Determine float decimal digits
                                decimal_digits = str(col_val)[::-1].find('.')
                                # ROUND to multipleOf: 1e-15 else no rounding
                                col_val = float(round(col_val, 15)) if decimal_digits > 15 else float(col_val)
                        # BOOLEAN
                        elif column_effective_value_type == 'boolValue':
                            if isinstance(col_val, bool):
                                col_val = col_val
                            elif isinstance(col_val, str):
                                if col_val.lower() in ('true', 't', 'yes', 'y'):
                                    col_val = True
                                elif col_val.lower() in ('false', 'f', 'no', 'n'):
                                    col_val = False
                            elif isinstance(col_val, int):
                                col_val = True if col_val in (1, -1) else False
                        # STRING
                        elif column_effective_value_type == 'stringValue':
                            col_val = str(col_val)
                        # OTHER: Convert everything else to a string
                        else:
                            col_val = str(col_val)
                            LOGGER.info(
                                'WARNING: POSSIBLE DATA TYPE ERROR'
                                ' SHEET:[{}], COL:[{}], CELL:[{}{}], VALUE:[{}], INSTANCE:[{}], TYPE:[{}], FORMAT:[{}],'
                                .format(self.sheet_title, col_name, col_letter, row_index,
                                        col_val, type(val), column_effective_value_type, column_number_format_type)
                            )
                    except Exception:
                        col_val = str(col_val)
                        LOGGER.info(
                            'ERROR: DATA TYPE CONVERSION FAILED'
                            ' SHEET:[{}], COL:[{}], CELL:[{}{}], VALUE:[{}], INSTANCE:[{}], TYPE:[{}], FORMAT:[{}],'
                            .format(self.sheet_title, col_name, col_letter, row_index,
                                    col_val, type(val), column_effective_value_type, column_number_format_type)
                        )
                        raise

                sheet_data_row_tf[col_name] = col_val
                col_num = col_num + 1

            if not is_empty:
                sheet_data_tf.append(sheet_data_row_tf)
            row_index = row_index + 1
        return sheet_data_tf

    def get_spreadsheet_data(self, range=None):
        # Encode sheet_title -> http request
        sheet_title_encoded = encode_string(self.sheet_title)
        sheet_range = range

        # default range is the first row
        if sheet_range is None:
            first_col_letter = self.config.get_column(self.sheet_title)
            last_col_letter = self.config.get_last_column(self.sheet_title)
            first_row_index = self.config.get_row(self.sheet_title)
            sheet_range = '{}{}:{}{}'.format(first_col_letter, first_row_index, last_col_letter, first_row_index)

        path = Sheet.path.replace('{spreadsheet_id}', self.config.spreadsheet_id)
        params = '&'.join(['%s=%s' % (key, value) for (key, value) in Sheet.params.items()]).replace(
            '{sheet_title}', sheet_title_encoded).replace('{sheet_range}', sheet_range)

        spreadsheet_data, time_extracted = self.get_data(stream_name=self.stream_name, path=path,
                                                         params=params, api=self.api)
        return spreadsheet_data, time_extracted

    def sync(self):
        first_col_letter = self.config.get_column(self.sheet_title)
        last_col_letter = self.config.get_last_column(self.sheet_title)
        first_row_index = self.config.get_row(self.sheet_title)

        # Send extra request to get sheet id and row count
        sheet_range = '{}{}:{}{}'.format(first_col_letter, first_row_index, last_col_letter, first_row_index)
        spreadsheet_metadata, time_extracted = self.get_spreadsheet_data(sheet_range)
        sheet_metadata = spreadsheet_metadata.get('sheets')[0]
        sheet_max_row = sheet_metadata.get('properties').get('gridProperties', {}).get('rowCount')

        last_row_index = self.config.get_last_row(self.sheet_title) or sheet_max_row
        last_row_index = min(last_row_index, sheet_max_row)

        # Initialize paging
        batch_size = self.config.batch_size
        from_row = first_row_index
        to_row = min(last_row_index, from_row+batch_size)

        # Loop thru batches
        while from_row < last_row_index and to_row <= last_row_index:
            sheet_range = '{}{}:{}{}'.format(first_col_letter, from_row, last_col_letter, to_row)

            spreadsheet_data, time_extracted = self.get_spreadsheet_data(sheet_range)

            # Transform batch of rows to JSON with keys for each column
            sheet_data_tf = self.transform_data(
                from_row=from_row,
                spreadsheet_data=spreadsheet_data,
                extracted_time=strftime(time_extracted)
            )

            # Update paging from/to_row for next batch
            from_row = to_row + 1
            to_row = min(last_row_index, to_row + batch_size)

            yield sheet_data_tf, time_extracted


STREAMS = {
    'file_metadata': FileMetadata,
    'sheet': Sheet
}
