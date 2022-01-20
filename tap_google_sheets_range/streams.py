import json
import re
from datetime import datetime, timedelta

import singer
from singer import utils
from singer.utils import strftime
from collections import OrderedDict
from tap_google_sheets_range.utils import excel_to_dttm_str, col_string_to_num, col_num_to_string, get_schema_from_file, encode_string


LOGGER = singer.get_logger()


# streams: API URL endpoints to be called
# properties:
#   <root node>: Plural stream name for the endpoint
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


# Base class for SheetMetadata and SheetData. Provides info about sheet schema and sheet metadata.
class Sheet(Stream):
    api = "sheets"
    path = "spreadsheets/{spreadsheet_id}"
    params = {
        "includeGridData": "true",
        "ranges": "{sheet_title}!{sheet_range}"
    }
    sheet_metadata_cache = {}

    def __init__(self, client, config, state, sheet_title):
        super().__init__(client, config, state)
        self.sheet_title = sheet_title

    def get_sheet_metadata(self):
        sheet_metadata = self.sheet_metadata_cache.get(self.sheet_title)
        if sheet_metadata is None:
            sheet_title_encoded = encode_string(self.sheet_title)

            first_column_letter = self.config.get_column(self.sheet_title)
            last_column_letter = self.config.get_last_column(self.sheet_title)
            first_row_index = self.config.get_row(self.sheet_title)
            sheet_range = '{}{}:{}{}'.format(first_column_letter, first_row_index,
                                             last_column_letter, first_row_index)

            path = Sheet.path.replace('{spreadsheet_id}', self.config.spreadsheet_id)
            params = '&'.join(['%s=%s' % (key, value) for (key, value) in Sheet.params.items()]) \
                .replace('{sheet_title}', sheet_title_encoded) \
                .replace('{sheet_range}', sheet_range)

            response, time_extracted = self.get_data(
                stream_name=self.sheet_title, path=path, api=self.api, params=params
            )
            sheet_metadata = response.get('sheets')[0]
            LOGGER.info("get_sheet_metadata response = {}".format(response.get('sheets')))
            SheetMetadata.sheet_metadata_cache[self.sheet_title] = sheet_metadata
        return sheet_metadata

    def get_sheet_columns_schema(self):
        sheet_metadata = self.get_sheet_metadata()
        sheet_title = self.sheet_title
        sheet_json_schema = OrderedDict()
        data = next(iter(sheet_metadata.get('data', [])), {})
        row_data = data.get('rowData', [])
        if row_data == []:
            LOGGER.info('SKIPPING sheet: {}. The sheet is empty or no data found in the first row'.format(sheet_title))
            return None, None

        headers = self.config.get_sheet_headers(self.sheet_title)
        first_row = row_data[0].get('values', [])
        # Google api doesn't return metadata for empty cells at the end of the requested range
        # Pad the first row with default value
        for i in range(len(headers) - len(first_row)):
            first_row.append(OrderedDict())

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
                }
            }
        }

        columns = []
        first_row_index = self.config.get_row(self.sheet_title)
        first_column_letter = self.config.get_column(self.sheet_title)
        column_index = col_string_to_num(first_column_letter)
        i = 0
        LOGGER.info("Reading metadata from the first row [SHEET_NAME: {}, ROW: {}, LETTER: {}, COL_INDEX: {}]".format(
            sheet_title, first_row_index, first_column_letter, column_index))

        for header in headers:
            column_letter = col_num_to_string(column_index)
            column_name = header

            first_value = None
            first_value = first_row[i]

            column_effective_value = first_value.get('effectiveValue', {})

            col_val = None
            if column_effective_value == {}:
                column_effective_value_type = 'stringValue'
                LOGGER.info('WARNING: NO VALUE IN THE {}ND ROW [SHEET: {}, COL: {}, CELL: {}{}]'
                            ' -> Setting column datatype to STRING'.
                            format(first_row_index, sheet_title, column_name, column_letter, first_row_index))
            else:
                for key, val in column_effective_value.items():
                    if key in ('numberValue', 'stringValue', 'boolValue'):
                        column_effective_value_type = key
                        col_val = str(val)
                    elif key in ('errorType', 'formulaType'):
                        col_val = str(val)
                        raise Exception('DATA TYPE ERROR [SHEET: {}, COL: {}, CELL: {}{}, TYPE: {}]'.format(
                            sheet_title, column_name, column_letter, first_row_index, key))

            column_number_format = first_row[i].get('effectiveFormat', {}).get('numberFormat', {})
            column_number_format_type = column_number_format.get('type')

            # Determine datatype for sheet_json_schema
            #
            # column_effective_value_type = numberValue, stringValue, boolValue;
            #  INVALID: errorType, formulaType
            #  https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/other#ExtendedValue
            #
            # column_number_format_type = UNSPECIFIED, TEXT, NUMBER, PERCENT, CURRENCY, DATE,
            #  TIME, DATE_TIME, SCIENTIFIC
            #  https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets/cells#NumberFormatType
            #
            column_format = None  # Default
            if column_effective_value == {}:
                col_properties = {'type': ['null', 'string']}
                column_gs_type = 'stringValue'
            elif column_effective_value_type == 'stringValue':
                col_properties = {'type': ['null', 'string']}
                column_gs_type = 'stringValue'
            elif column_effective_value_type == 'boolValue':
                col_properties = {'type': ['null', 'boolean', 'string']}
                column_gs_type = 'boolValue'
            elif column_effective_value_type == 'numberValue':
                if column_number_format_type == 'DATE_TIME':
                    col_properties = {
                        'type': ['null', 'string'],
                        'format': 'date-time'
                    }
                    column_gs_type = 'numberType.DATE_TIME'
                elif column_number_format_type == 'DATE':
                    col_properties = {
                        'type': ['null', 'string'],
                        'format': 'date'
                    }
                    column_gs_type = 'numberType.DATE'
                elif column_number_format_type == 'TIME':
                    col_properties = {
                        'type': ['null', 'string'],
                        'format': 'time'
                    }
                    column_gs_type = 'numberType.TIME'
                elif column_number_format_type == 'TEXT':
                    col_properties = {'type': ['null', 'string']}
                    column_gs_type = 'stringValue'
                elif column_number_format_type == 'NUMBER':
                    col_properties = {'type': ['null', 'number']}
                    column_gs_type = 'numberType'
                else:
                    # col_properties = {'type': ['null', 'string']}
                    col_properties = {
                        'type': ['null', 'number', 'string'],
                        'multipleOf': 1e-15
                    }
                    column_gs_type = 'numberType'
            # Catch-all to deal with other types and set to string
            # column_effective_value_type: formulaValue, errorValue, or other
            else:
                col_properties = {'type': ['null', 'string']}
                column_gs_type = 'unsupportedValue'
                LOGGER.info('WARNING: UNSUPPORTED {}ND ROW VALUE [SHEET: {}, COL: {}, CELL: {}{}, TYPE: {}]'.format(
                    first_row_index, sheet_title, column_name,
                    column_letter, first_row_index, column_effective_value_type))
                LOGGER.info('Converting to string.')

            column = {
                'columnIndex': column_index,
                'columnLetter': column_letter,
                'columnName': column_name,
                'columnType': column_gs_type
            }
            columns.append(column)

            sheet_json_schema['properties'][column_name] = col_properties
            i = i + 1
            column_index = column_index + 1

        LOGGER.info(f"Schema: {sheet_json_schema}")
        LOGGER.info(f"Columns: {columns}")
        return sheet_json_schema, columns


class SheetMetadata(Sheet):
    stream_id = "sheet_metadata"
    stream_name = "sheet_metadata"
    key_properties = ["sheetId"]
    replication_method = "FULL_TABLE"
    replication_keys = []

    def get_schema(self):
        return get_schema_from_file(self.stream_name)

    def transform_data(self, data, columns):
        # Convert to properties to dict
        sheet_metadata = data.get('properties')
        sheet_metadata_tf = json.loads(json.dumps(sheet_metadata))
        sheet_id = sheet_metadata_tf.get('sheetId')
        sheet_url = 'https://docs.google.com/spreadsheets/d/{}/edit#gid={}'.format(
            self.config.spreadsheet_id, sheet_id)
        sheet_metadata_tf['spreadsheetId'] = self.config.spreadsheet_id
        sheet_metadata_tf['sheetUrl'] = sheet_url
        sheet_metadata_tf['columns'] = columns
        return sheet_metadata_tf

    def sync(self):
        sheet_metadata = self.get_sheet_metadata()
        schema, columns = self.get_sheet_columns_schema()
        data = self.transform_data(sheet_metadata, columns)
        return data


class SheetData(Sheet):
    api = "sheets"
    path = "spreadsheets/{spreadsheet_id}/values/{sheet_title}!{range_rows}"
    key_properties = ["__sdc_row"]
    replication_method = "FULL_TABLE"
    replication_keys = []
    params = {
            "dateTimeRenderOption": "SERIAL_NUMBER",
            "valueRenderOption": "UNFORMATTED_VALUE",
            "majorDimension": "ROWS"
    }

    def __init__(self, client, config, state, sheet_title):
        super().__init__(client, config, state, sheet_title)
        self.stream_id = '_'.join([sheet_title, config.spreadsheet_id]).replace('-', '__')
        self.stream_name = '_'.join([sheet_title, config.spreadsheet_id]).replace('-', '__')

    def get_schema(self):
        try:
            sheet_json_schema, columns = self.get_sheet_columns_schema()
        except Exception as err:
            LOGGER.warning('{}'.format(err))
            LOGGER.warning('SKIPPING Malformed sheet: {}'.format(self.sheet_title))
            sheet_json_schema, columns = None, None
            raise err

        return sheet_json_schema

    # Transform sheet_data: add spreadsheet_id, sheet_id, and row, convert dates/times
    # Convert from array of values to JSON with column names as keys
    def transform_data(self, sheet_id, sheet_title, from_row, columns, sheet_data_rows, extracted_time):
        sheet_data_tf = []
        row_num = from_row
        # Create sorted list of columns based on columnIndex
        cols = sorted(columns, key=lambda i: i['columnIndex'])

        # LOGGER.info('sheet_data_rows: {}'.format(sheet_data_rows))
        for row in sheet_data_rows:
            # SKIP empty row
            if row == []:
                LOGGER.info('EMPTY ROW: {}, SKIPPING'.format(row_num))
            else:
                sheet_data_row_tf = {'__sdc_load_time': extracted_time,
                                     '__sdc_spreadsheet_id': self.config.spreadsheet_id,
                                     '__sdc_sheet_id': sheet_id,
                                     '__sdc_row': row_num}
                # Add spreadsheet_id, sheet_id, and row
                col_num = 1
                for value in row:
                    # Select column metadata based on column index
                    col = cols[col_num - 1]
                    # Get column metadata
                    col_name = col.get('columnName')
                    col_type = col.get('columnType')
                    col_letter = col.get('columnLetter')

                    # NULL values
                    if value is None or value == '':
                        col_val = None

                    # Convert dates/times from Lotus Notes Serial Numbers
                    # DATE-TIME
                    elif col_type == 'numberType.DATE_TIME':
                        if isinstance(value, (int, float)):
                            col_val = excel_to_dttm_str(value)
                        else:
                            col_val = str(value)
                            LOGGER.info(
                                'WARNING: POSSIBLE DATA TYPE ERROR; SHEET: {}, COL: {}, CELL: {}{}, TYPE: {}'.format(
                                    sheet_title, col_name, col_letter, row_num, col_type))
                    # DATE
                    elif col_type == 'numberType.DATE':
                        if isinstance(value, (int, float)):
                            col_val = excel_to_dttm_str(value)[:10]
                        else:
                            col_val = str(value)
                            LOGGER.info(
                                'WARNING: POSSIBLE DATA TYPE ERROR; SHEET: {}, COL: {}, CELL: {}{}, TYPE: {}'.format(
                                    sheet_title, col_name, col_letter, row_num, col_type))
                    # TIME ONLY (NO DATE)
                    elif col_type == 'numberType.TIME':
                        if isinstance(value, (int, float)):
                            try:
                                total_secs = value * 86400  # seconds in day
                                # Create string formatted like HH:MM:SS
                                col_val = str(timedelta(seconds=total_secs))
                            except ValueError:
                                col_val = str(value)
                                LOGGER.info(
                                    'WARNING: POSSIBLE DATA TYPE ERROR; SHEET: {}, COL: {}, CELL: {}{}, TYPE: {}'.format(
                                        sheet_title, col_name, col_letter, row_num, col_type))
                        else:
                            col_val = str(value)
                    # NUMBER (INTEGER AND FLOAT)
                    elif col_type == 'numberType':
                        if isinstance(value, int):
                            col_val = int(value)
                        elif isinstance(value, float):
                            # Determine float decimal digits
                            decimal_digits = str(value)[::-1].find('.')
                            if decimal_digits > 15:
                                try:
                                    # ROUND to multipleOf: 1e-15
                                    col_val = float(round(value, 15))
                                except ValueError:
                                    col_val = str(value)
                                    LOGGER.info(
                                        'WARNING: POSSIBLE DATA TYPE ERROR; SHEET: {}, COL: {}, CELL: {}{}, TYPE: {}'.format(
                                            sheet_title, col_name, col_letter, row_num, col_type))
                            else:  # decimal_digits <= 15, no rounding
                                try:
                                    col_val = float(value)
                                except ValueError:
                                    col_val = str(value)
                                    LOGGER.info(
                                        'WARNING: POSSIBLE DATA TYPE ERROR: SHEET: {}, COL: {}, CELL: {}{}, TYPE: {}'.format(
                                            sheet_title, col_name, col_letter, row_num, col_type))
                        else:
                            col_val = str(value)
                            LOGGER.info(
                                'WARNING: POSSIBLE DATA TYPE ERROR: SHEET: {}, COL: {}, CELL: {}{}, TYPE: {}'.format(
                                    sheet_title, col_name, col_letter, row_num, col_type))
                    # STRING
                    elif col_type == 'stringValue':
                        col_val = str(value)
                    # BOOLEAN
                    elif col_type == 'boolValue':
                        if isinstance(value, bool):
                            col_val = value
                        elif isinstance(value, str):
                            if value.lower() in ('true', 't', 'yes', 'y'):
                                col_val = True
                            elif value.lower() in ('false', 'f', 'no', 'n'):
                                col_val = False
                            else:
                                col_val = str(value)
                                LOGGER.info(
                                    'WARNING: POSSIBLE DATA TYPE ERROR; SHEET: {}, COL: {}, CELL: {}{}, TYPE: {}'.format(
                                        sheet_title, col_name, col_letter, row, col_type))
                        elif isinstance(value, int):
                            if value in (1, -1):
                                col_val = True
                            elif value == 0:
                                col_val = False
                            else:
                                col_val = str(value)
                                LOGGER.info(
                                    'WARNING: POSSIBLE DATA TYPE ERROR; SHEET: {}, COL: {}, CELL: {}{}, TYPE: {}'.format(
                                        sheet_title, col_name, col_letter, row, col_type))
                    # OTHER: Convert everything else to a string
                    else:
                        col_val = str(value)
                        LOGGER.info(
                            'WARNING: POSSIBLE DATA TYPE ERROR; SHEET: {}, COL: {}, CELL: {}{}, TYPE: {}'.format(
                                sheet_title, col_name, col_letter, row, col_type))
                    sheet_data_row_tf[col_name] = col_val
                    col_num = col_num + 1
                # APPEND non-empty row
                sheet_data_tf.append(sheet_data_row_tf)
            row_num = row_num + 1
        return sheet_data_tf

    def sync(self):
        sheet_metadata = self.get_sheet_metadata()

        sheet_title = sheet_metadata.get('properties', {}).get('title')
        sheet_id = sheet_metadata.get('properties', {}).get('sheetId')
        sheet_max_row = sheet_metadata.get('properties').get('gridProperties', {}).get('rowCount')

        first_row_index = self.config.get_row(self.sheet_title)
        last_row_index = self.config.get_last_row(self.sheet_title) or sheet_max_row
        last_row_index = min(last_row_index, sheet_max_row)

        first_col_letter = self.config.get_column(self.sheet_title)
        last_col_letter = self.config.get_last_column(self.sheet_title)

        sheet_schema, columns = self.get_sheet_columns_schema()

        # Initialize paging
        batch_size = self.config.batch_size
        from_row = first_row_index
        to_row = min(last_row_index, batch_size)

        # Encode sheet_title -> http request
        sheet_title_encoded = encode_string(self.sheet_title)

        # Loop thru batches
        while from_row < last_row_index and to_row <= last_row_index:
            range_rows = '{}{}:{}{}'.format(first_col_letter, from_row, last_col_letter, to_row)

            path = SheetData.path.replace('{spreadsheet_id}', self.config.spreadsheet_id)\
                .replace('{sheet_title}', sheet_title_encoded)\
                .replace('{range_rows}', range_rows)

            sheet_data, time_extracted = self.get_data(
                stream_name=sheet_title,
                path=path,
                params=SheetData.params,
                api=self.api)
            # Data is returned as a list of arrays, an array of values for each row
            sheet_data_rows = sheet_data.get('values', [])

            # Transform batch of rows to JSON with keys for each column
            sheet_data_tf = self.transform_data(
                sheet_id=sheet_id,
                sheet_title=sheet_title,
                from_row=from_row,
                columns=columns,
                sheet_data_rows=sheet_data_rows,
                extracted_time=strftime(time_extracted)
            )

            # Update paging from/to_row for next batch
            from_row = to_row + 1
            to_row = min(last_row_index, to_row + batch_size)

            yield sheet_data_tf, time_extracted


STREAMS = {
    'file_metadata': FileMetadata,
    'sheet_metadata': SheetMetadata,
    'sheet_data': SheetData
}
