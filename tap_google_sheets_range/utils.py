import re
import singer

LOGGER = singer.get_logger()


class Config:
    def __init__(self, sa_keyfile, spreadsheet_id, sheets, start_date, user_agent,
                 batch_size=None, request_timeout=None):
        self.sa_keyfile = sa_keyfile
        self.spreadsheet_id = spreadsheet_id
        self.sheets = self.read_sheet_config(sheets)
        self.start_date = start_date
        self.user_agent = user_agent
        self.batch_size = batch_size or 300
        self.request_timeout = request_timeout or 300

    class SheetConfig:
        def __init__(self, headers, data):
            self.headers = headers
            self.data = data

    def read_sheet_config(self, config):
        if not config:
            raise ValueError('Wrong config. Sheets list is empty.')
        sheet_config = {}
        try:
            for key, value in config.items():
                sheet_config[key] = self.SheetConfig(**value)
        except:
            LOGGER.error('Wrong config. Missing headers/range value.')
            raise
        return sheet_config

    def list_sheets(self):
        return self.sheets.keys()

    def get_sheet_cell_range(self, sheet_title):
        return self.sheets.get(sheet_title).data

    def get_sheet_headers(self, sheet_title):
        headers = self.sheets.get(sheet_title).headers
        if not headers:
            return []
        return [h.strip() for h in headers.strip(',').split(',')]

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
            if not self.get_sheet_headers(sheet):
                raise ValueError('Wrong sheet config. Header list is empty. Sheet: [{}]'.format(sheet))
            if not self.get_sheet_cell_range(sheet):
                raise ValueError('Wrong sheet config. Range is empty. Sheet: [{}]'.format(sheet))
            pattern = '[A-Z]{1,3}[0-9]*:[A-Z]{1,3}[0-9]*'
            if not re.match(pattern, self.get_sheet_cell_range(sheet)):
                raise ValueError('Wrong sheet config. Range doesnt match to the pattern.'
                                 ' Sheet: [{}] Range:[{}] Pattern: [{}]'.
                                 format(sheet, self.get_sheet_cell_range(sheet), pattern))