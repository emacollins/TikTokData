# download all tiktok video from user
# edit video from entire directory using moviepy
# Created by HengSok


from turtle import down
import requests,os,time
import tempfile
import datetime
import pandas as pd
import config
import shutil
import boto3
import zip_videos
import airtable

# Download All Video From Tiktok User Function
def download_video_no_watermark(user: str,
                                video_id: int,
                                tmpdirname: str):
    s3 = boto3.resource('s3')
    date = datetime.datetime.now()
    url = "https://tiktok-downloader-download-tiktok-videos-without-watermark.p.rapidapi.com/vid/index"
    querystring = {"url":f'https://www.tiktok.com/@{user}r/video/{str(video_id)}'}
    headers = {
        "X-RapidAPI-Key": "847b077d69mshaeae673f63126b4p1f4fa8jsn47c20efef8f4",
        "X-RapidAPI-Host": "tiktok-downloader-download-tiktok-videos-without-watermark.p.rapidapi.com"
    }

    video_object = requests.request("GET", url, headers=headers, params=querystring).json()
    
        
    download_url = video_object['video'][0]
                       
    video_bytes = requests.get(download_url, stream=True)
    tmp_video_file = tmpdirname + f'/{video_id}.mp4'
    with open(tmp_video_file, 'wb') as out_file:
        out_file.write(video_bytes.content)
    s3.meta.client.upload_file(tmp_video_file, 
                                Bucket=config.BUCKET, 
                                Key=config.HarvestPath(user=user,
                                                        date=date,
                                                        video_id=video_id).video_path_file_s3_key,
                                ExtraArgs=None, 
                                Callback=None, 
                                Config=None)  

        
def run():  
    # Uses temp directorty to download the files and upload to S3
    with tempfile.TemporaryDirectory(dir=config.LOCAL_PATH_PREFIX) as tmpdirname:
        date = datetime.datetime.now()
        extract_file = config.ExtractPath(date=date).data_path_file
        extract_data = pd.read_csv(extract_file)
        for user in extract_data['user_unique_id'].unique():
            user_data = extract_data.loc[extract_data['user_unique_id'] == user]
            for video_id in user_data['video_id'].unique():
                download_video_no_watermark(user=user,
                                            video_id=video_id,
                                            tmpdirname=tmpdirname)
            zip_videos.run(user=user,
                           date=date)
            
if __name__ == "__main__":
    run()