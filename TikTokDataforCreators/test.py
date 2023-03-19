from httplib2 import UnimplementedHmacDigestAuthOptionError
from tiktokapipy.api import TikTokAPI
import pandas as pd
import os
from traitlets import Bool
import datetime
import config
import logging
import download_videos
import airtable
import tempfile
import boto3
# Adjust this to capture more videos
# API works by scrolling down page on TikTok,
# if user has a lot fo videos, scroll time should be longer
SCROLL_TIME = 1
S3 = boto3.resource('s3')
username='tytheproductguy'
date = datetime.datetime.now().strftime('%m-%d-%Y')
with tempfile.TemporaryDirectory(dir='/Users/ericcollins') as tmpdirname:
    filename = f'/{date}'
    file = tmpdirname + filename
    with TikTokAPI(scroll_down_time=SCROLL_TIME,navigation_retries=5, navigation_timeout=0, 
                data_dump_file=file) as api:
        user_object = api.user(username, video_limit=0)