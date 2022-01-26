#!/usr/bin/env python3

import sys
import json
import singer
from singer import Catalog

from tap_google_sheets_range.client import GoogleClient
from tap_google_sheets_range.discover import discover
from tap_google_sheets_range.sync import sync
from tap_google_sheets_range.utils import Config

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'sa_keyfile',
    'spreadsheet_id',
    'sheets',
    'start_date',
    'user_agent'
]


@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    LOGGER.info('Args: {}'.format(REQUIRED_CONFIG_KEYS))
    with GoogleClient(sa_keyfile=args.config.get('sa_keyfile'),
                      user_agent=args.config['user_agent']) as client:
        state = {}
        if args.state:
            state = args.state
        LOGGER.info("Config: {}".format(args.config))
        config = Config(**args.config)
        config.check_config()

        if args.discover:
            LOGGER.info("Starting discovery mode")
            catalog = discover(client, config)
            json.dump(catalog.to_dict(), sys.stdout, indent=2)
            LOGGER.info("Finished discovery mode")
        elif args.catalog:
            sync(client=client,
                 config=config,
                 catalog=args.catalog,
                 state=state)
