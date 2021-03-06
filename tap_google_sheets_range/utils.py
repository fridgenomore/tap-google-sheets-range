import json
import os
import re
import urllib
from datetime import datetime, timedelta

import pytz
import singer
from singer import strftime


LOGGER = singer.get_logger()


# Convert Excel Date Serial Number (excel_date_sn) to datetime string
# timezone_str: defaults to UTC (which we assume is the timezone for ALL datetimes)
def excel_to_dttm_str(excel_date_sn, timezone_str=None):
    if not timezone_str:
        timezone_str = 'UTC'
    tzn = pytz.timezone(timezone_str)
    sec_per_day = 86400
    excel_epoch = 25569 # 1970-01-01T00:00:00Z, Lotus Notes Serial Number for Epoch Start Date
    # Seems math.floor should be removed, example 2022-01-01 10:00:00 -> 2022-01-01 09:59:59
    # epoch_sec = math.floor((excel_date_sn - excel_epoch) * sec_per_day)
    epoch_sec = round((excel_date_sn - excel_epoch) * sec_per_day)
    epoch_dttm = datetime(1970, 1, 1)
    excel_dttm = epoch_dttm + timedelta(seconds=epoch_sec)
    utc_dttm = tzn.localize(excel_dttm).astimezone(pytz.utc)
    utc_dttm_str = strftime(utc_dttm)
    return utc_dttm_str


# Convert column letter to column index
def col_string_to_num(col: str):
    value = col.upper()
    if len(value) == 1:
        return ord(value)%64
    elif len(value) == 2:
        return 26 + (ord(value[0])%64) * (ord(value[1])%64)
    elif len(value) == 3:
        return 26 + 26 ** 2 + (ord(value[0])%64) * (ord(value[1])%64) * (ord(value[2])%64)
    else:
        raise ValueError(f"Wrong column name [{col}]")


# Convert column index to column letter
def col_num_to_string(num):
    string = ""
    while num > 0:
        num, remainder = divmod(num - 1, 26)
        string = chr(65 + remainder) + string
    return string


def encode_string(value):
    return urllib.parse.quote_plus(value)


def get_schema_from_file(stream_name):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    schema_path = os.path.join(dir_path, 'schemas/{}.json'.format(stream_name))

    with open(schema_path) as file:
        schema = json.load(file)
    return schema


class Config:
    def __init__(self, sa_keyfile, spreadsheet_id, sheets, start_date, user_agent,
                 batch_size=None, request_timeout=None, null_values=None):
        self.sa_keyfile = sa_keyfile
        self.spreadsheet_id = spreadsheet_id
        self.sheets = self.load_sheet_config(sheets)
        self.start_date = start_date
        self.user_agent = user_agent
        self.batch_size = batch_size or 300
        self.request_timeout = request_timeout or 300
        self.null_values = null_values or ['---']

    def load_sheet_config(self, config):
        if not config:
            raise ValueError('Wrong config. Sheets list is empty.')
        sheet_config = {}
        try:
            for key, value in config.items():
                sheet_config[key] = Config.SheetConfig(**value)
        except:
            LOGGER.error('Wrong config. Missing headers/range value.')
            raise
        return sheet_config

    def list_sheets(self):
        return self.sheets.keys()

    def get_sheet_cell_range(self, sheet_title):
        return self.sheets.get(sheet_title).data

    def is_link(self, sheet_title, header_name):
        for header in self.list_sheet_headers(sheet_title):
            if header.name == header_name:
                return header.link
        return False

    def list_sheet_headers(self, sheet_title):
        headers = self.sheets.get(sheet_title).headers
        return headers

    def get_sheet_target_table(self, sheet_title):
        table = self.sheets.get(sheet_title).target_table
        if table is None:
            return '_'.join([sheet_title, self.spreadsheet_id]).replace('-', '__')
            # return sheet_title
        return table

    def get_column(self, sheet):
        range = self.get_sheet_cell_range(sheet).split(':')
        column = re.search('[A-Z]{1,3}', range[0])
        if not column:
            return None
        return column.group()

    def get_row(self, sheet):
        range = self.get_sheet_cell_range(sheet).split(':')
        row = re.search('[0-9]+', range[0])
        if not row:
            return 1
        return int(row.group())

    def get_last_column(self, sheet):
        range = self.get_sheet_cell_range(sheet).split(':')
        column = re.search('[A-Z]{1,3}', range[1])
        if not column:
            return None
        return column.group()

    def get_last_row(self, sheet):
        range = self.get_sheet_cell_range(sheet).split(':')
        row = re.search('[0-9]+', range[1])
        if not row:
            return None
        return int(row.group())

    def check_config(self):
        for sheet in self.list_sheets():
            if not self.list_sheet_headers(sheet):
                raise ValueError('Wrong sheet config: Header list is empty. Sheet: [{}]'.format(sheet))
            if not self.get_sheet_cell_range(sheet):
                raise ValueError('Wrong sheet config: Range is empty. Sheet: [{}]'.format(sheet))
            pattern = '[A-Z]{1,3}[0-9]*:[A-Z]{1,3}[0-9]*'
            if not re.match(pattern, self.get_sheet_cell_range(sheet)):
                raise ValueError('Wrong sheet config: Range doesn\'t match to the pattern.'
                                 ' Sheet: [{}] Range:[{}] Pattern: [{}]'.
                                 format(sheet, self.get_sheet_cell_range(sheet), pattern))
            cols_count = col_string_to_num(self.get_last_column(sheet)) - \
                         col_string_to_num(self.get_column(sheet)) + \
                         1
            headers_count = len(self.list_sheet_headers(sheet))
            if headers_count != cols_count:
                raise ValueError("Wrong sheet config: Columns count doesn't equal to headers count"
                                 ' Sheet:[{}] Range:[{}] Headers:[{}] Columns:[{}]'.
                                 format(sheet, self.get_sheet_cell_range(sheet), headers_count, cols_count))
            headers = [h.name for h in self.list_sheet_headers(sheet)]
            if len(headers) != len(set(headers)):
                raise ValueError("Wrong sheet config: Found duplicate header.")
            if not isinstance(self.null_values, list):
                raise ValueError("Wrong config: null_values is not a list")

    class HeaderConfig:
        def __init__(self, name, type=None, format=None, link=False):
            if not isinstance(name, str):
                raise ValueError("Wrong header config: Header name is not a string. Header:[{}]"
                                 .format(name))
            self.name = name
            self.type = type or ["null", "string"]
            self.format = format
            self.link = link
            self.check_config()

        def check_config(self):
            format_list = ["color", "email", "idn-email", "ipv4", "ipv6", "ip-address", "hostname",
                           "host-name", "idn-hostname", "uri" ,"uri-template", "uri-reference",
                           "iri", "iri-reference", "date-time", "time", "date", "regex",
                           "uuid", "duration", "json-pointer", "relative-json-pointer"]
            if self.format and self.format not in format_list:
                raise ValueError("Wrong header config. Wrong format value, check the list of possible values."
                                 " Header: [{}] Type: [{}], Format: [{}]"
                                 .format(self.name, self.type, self.format))


    class SheetConfig:
        def __init__(self, headers, data, target_table=None):
            self.headers = self.load_header_config(headers)
            self.data = data
            self.target_table = target_table

        def load_header_config(self, config):
            if not config:
                raise ValueError('Missing headers config.')
            header_config = []
            for value in config:
                header_config.append(Config.HeaderConfig(**value))
            return header_config