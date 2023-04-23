from tiktokapipy.api import TikTokAPI
import os
import datetime
import logging
import tempfile
import boto3
import json

def get_video_views(url):
    with TikTokAPI(data_dump_file='test') as api:
        video = api.video(url)

if __name__=='__main__':
    url = 'https://www.tiktok.com/@tylerandhistummy/video/7220474750462676270'
    get_video_views(url=url)
    