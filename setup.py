#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-google-sheets-range',
      version='1.0.0',
      description='Singer.io tap for extracting data from the Google Sheets v4 API',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_google_sheets_range'],
      install_requires=[
          'backoff==1.8.0',
          'requests==2.22.0',
          'singer-python==5.9.0',
          'google-auth'
      ],
      extras_require={
          'dev': [
              'ipdb==0.11',
              'pylint',
              'nose'
          ]
      },
      entry_points='''
          [console_scripts]
          tap-google-sheets-range=tap_google_sheets_range:main
      ''',
      packages=find_packages(),
      package_data={
          'tap_google_sheets_range': [
              'schemas/*.json'
          ]
      })
