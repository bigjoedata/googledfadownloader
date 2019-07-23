#!/usr/bin/python
#Based on https://github.com/googleads/googleads-dfa-reporting-samples/blob/master/python/v3_3/get_report_files.py

import config
from datetime import datetime, timedelta
import httplib2 as lib2
from apiclient import discovery
from urllib.parse import urlencode
from urllib.request import Request , urlopen, HTTPError
from googleapiclient.discovery import build
import google.oauth2.credentials
from google.auth.transport.requests import AuthorizedSession
from googleapiclient.discovery import build as google_build
from googleapiclient import http
import argparse
import io
import os
import sys

profile_id=config.profile_id
report_id=config.report_id
#outputfiledir=config.outputfiledir

access_token = config.access_token
refresh_token = config.refresh_token
client_id = config.client_id
client_secret = config.client_secret

CHUNK_SIZE = 32 * 1024 * 1024 #Sets 32MB file chunk downloads
user_agent = None
token_uri = 'https://accounts.google.com/o/oauth2/token'
token_expiry = datetime.now() - timedelta(days = 1)

credentials = google.oauth2.credentials.Credentials(
    None,
    refresh_token=refresh_token,
    token_uri=token_uri,
    client_id=client_id,
    client_secret=client_secret)

api_name = 'dfareporting'
api_version = 'v3.3'
API_SCOPES = ['https://www.googleapis.com/auth/dfareporting',
              'https://www.googleapis.com/auth/dfatrafficking',
              'https://www.googleapis.com/auth/ddmconversions']

# helpful tip, you can call other Google APIs by changing the api name and api version above
service = google_build(serviceName=api_name, version=api_version, credentials=credentials)

reportslistrequest = service.reports().files().list(profileId=profile_id, reportId=report_id)
report_file = reportslistrequest.execute()

os.chdir('csv') # puts it in the relative dir csv
csvfiles = os.listdir()


for id in report_file['items']:
  if 'REPORT_AVAILABLE' in id['status']:
    file_id=id['id']
    file_name = id['fileName'] or id['id']
    extension = '.csv' if id['format'] == 'CSV' else '.xml'
    simple_file = file_name + '_' + id['dateRange']['startDate'] + '-' + id['dateRange']['endDate'] + extension
    if simple_file not in csvfiles:
      out_file = io.FileIO(file_name + '_' + id['dateRange']['startDate'] + '-' + id['dateRange']['endDate'] + extension, mode='wb')
      reportdl = service.files().get_media(reportId=report_id, fileId=file_id)
      downloader = http.MediaIoBaseDownload(out_file, reportdl, chunksize=CHUNK_SIZE)
      download_finished = False
      while download_finished is False:
        _, download_finished = downloader.next_chunk()
      print('File %s downloaded to %s'
        % (id['id'], os.path.realpath(out_file.name)))
    else:
        pass
