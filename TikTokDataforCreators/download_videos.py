# download all tiktok video from user
# edit video from entire directory using moviepy
# Created by HengSok


from turtle import down
import requests,os,time
import random 
from random import choice
import datetime
import pandas as pd
import config
import shutil


# Download All Video From Tiktok User Function
def download_video_no_watermark(user: str,
                                video_id: int):
    date = datetime.datetime.now()
    url = "https://tiktok-downloader-download-tiktok-videos-without-watermark.p.rapidapi.com/vid/index"

    querystring = {"url":f'https://www.tiktok.com/@{user}r/video/{str(video_id)}'}

    headers = {
        "X-RapidAPI-Key": "847b077d69mshaeae673f63126b4p1f4fa8jsn47c20efef8f4",
        "X-RapidAPI-Host": "tiktok-downloader-download-tiktok-videos-without-watermark.p.rapidapi.com"
    }

    video_object = requests.request("GET", url, headers=headers, params=querystring).json()
    user_video_harvest_path = config.HarvestPath(user=user,
                                                 date=date).video_path
    if not os.path.exists(user_video_harvest_path):
        os.makedirs(user_video_harvest_path)
        
    download_url = video_object['video'][0]
                       
    video_bytes = requests.get(download_url, stream=True)
    with open(config.HarvestPath(user=user, 
                                 video_id = video_id,
                                 date=date).video_path_file, 'wb') as out_file:
        out_file.write(video_bytes.content)

def zip_videos(user: str):
    date = datetime.datetime.now()
    if not os.path.exists(config.ExtractPath(user=user).video_path):
        os.makedirs(config.ExtractPath(user=user).video_path)
    shutil.make_archive(config.ExtractPath(date=date, user=user).video_path_file, 
                            'zip', 
                            config.HarvestPath(user=user, date=date).video_path)

def run():
    date = datetime.datetime.now()
    extract_file = config.ExtractPath(date=date).data_path_file
    extract_data = pd.read_csv(extract_file)
    
    for user in extract_data['user_unique_id'].unique():
        user_data = extract_data.loc[extract_data['user_unique_id'] == user]
        for video_id in user_data['video_id']:
            download_video_no_watermark(user=user,
                                        video_id=video_id)
    # Extract to zip file
    zip_videos(user=user)
if __name__ == "__main__":
    run()