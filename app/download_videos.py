# download all tiktok video from user
# edit video from entire directory using moviepy
# Created by HengSok


from asyncio import tasks
from concurrent.futures import ThreadPoolExecutor
import asyncio
import requests
import tempfile
import datetime
import pandas as pd
import config
import boto3
import zipfile
import os
import pathlib


# Download All Video From Tiktok User Function
def download_video_no_watermark(user: str,
                                video_id: int,
                                tmpdirname: str,
                                session) -> dict:
    date = datetime.datetime.now()
    url = "https://tiktok-downloader-download-tiktok-videos-without-watermark.p.rapidapi.com/vid/index"
    querystring = {"url":f'https://www.tiktok.com/@{user}r/video/{str(video_id)}'}
    headers = {
        "X-RapidAPI-Key": config.Secret_Key(key_name='X-RAPIDAPI-KEY').value,
        "X-RapidAPI-Host": "tiktok-downloader-download-tiktok-videos-without-watermark.p.rapidapi.com"
    }

    with session.get(url, headers=headers, params=querystring) as video_object:
        try:
            video_object = video_object.json()
            download_url = video_object['video'][0]
                            
            video_bytes = requests.get(download_url, stream=True)
            tmp_video_file = tmpdirname + f'/{video_id}.mp4'
            with open(tmp_video_file, 'wb') as out_file:
                out_file.write(video_bytes.content)
            s3 = boto3.resource('s3')
            s3.meta.client.upload_file(tmp_video_file, 
                                        Bucket=config.BUCKET, 
                                        Key=config.HarvestPath(user=user,
                                                                date=date,
                                                                video_id=video_id).video_path_file_s3_key,
                                        ExtraArgs=None, 
                                        Callback=None, 
                                        Config=None)  
            return video_object
        except:
            return dict()

def zip_videos(user: str,
        date: datetime.datetime,
        tmpdirname: str):
    # The S3 bucket containing the .mp4 files
    bucket = config.BUCKET

    # The S3 folder to which the .zip file will be saved
    zip_file_destination_s3_key = config.ExtractPath(user=user).video_path_s3_key
    s3 = boto3.client('s3')
    date_string = date.strftime('%m-%d-%Y')
    zip_file_name = f'{user}_downloaded_videos_{date_string}.zip'
    
    # Zip the .mp4 files
    with zipfile.ZipFile(zip_file_name, 'w') as zip_file:
        directory=pathlib.Path(tmpdirname)
        for file_path in directory.iterdir():
            zip_file.write(file_path, arcname=file_path.name)

    # Upload the .zip file to S3
    with open(zip_file_name, 'rb') as data:
        s3.upload_fileobj(data, bucket, zip_file_destination_s3_key + '/' + zip_file_name)
    os.remove(zip_file_name)
    

async def start_async_process(video_ids: list,
                              tmpdirname: str,
                              user: str):
    with ThreadPoolExecutor(max_workers=100) as executor:
        with requests.Session() as session:
            loop = asyncio.get_event_loop()
            tasks = [loop.run_in_executor(executor, 
                                          download_video_no_watermark,
                                          *(user, video_id, tmpdirname, session))
                     for video_id in video_ids
                     ]
            for video_object in await asyncio.gather(*tasks):
                pass
        
            
   
def run(user: str,
        date: datetime) -> bool:  
    # Uses temp directorty to download the files and upload to S3
    with tempfile.TemporaryDirectory(dir=config.ROOT_DIRECTORY, prefix=f'{user}_') as tmpdirname:
        date = datetime.datetime.now()
        extract_file = config.ExtractPath(date=date,
                                          user=user).data_path_file
        user_data = pd.read_csv(extract_file)
        video_ids = list(set(user_data['video_id']))
        
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(start_async_process(video_ids,
                                                           tmpdirname,
                                                           user))
        loop.run_until_complete(future)
        
        try:
            zip_videos(user=user,
                date=date,
                tmpdirname=tmpdirname)
            return True
        except Exception as e:
            print(e)
            return False

if __name__ == "__main__":
    run(user='thephotoverse',
        date=datetime.datetime.now())
    