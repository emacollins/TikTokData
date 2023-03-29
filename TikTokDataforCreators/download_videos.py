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
import aws_utils
from vidvault_utils import timeit
import time

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
    try:
        with session.get(url, headers=headers, params=querystring) as video_object:
                video_object = video_object.json()
                download_url = video_object['video'][0]
                                
                video_bytes = requests.get(download_url, stream=True)
                tmp_video_file = tmpdirname + f'/{video_id}.mp4'
                with open(tmp_video_file, 'wb') as out_file:
                    out_file.write(video_bytes.content)
                  
                return video_object
    except:
        return dict()
@timeit(message='ZIP all videos')
def zip_videos(user: str,
        date: datetime.datetime,
        tmpdirname: str):
    # The S3 bucket containing the .mp4 files
    bucket = config.BUCKET

    # The S3 folder to which the .zip file will be saved
    zip_file_destination_s3_key = config.ExtractPath(user=user).video_path_s3_key
    s3 = boto3.client('s3')
    date_string = date.strftime('%m-%d-%Y')
    zip_file_path = f'{user}_downloaded_videos_{date_string}.zip'
    
    # Zip the .mp4 files
    with zipfile.ZipFile(zip_file_path, 'w') as zip_file:
        directory=pathlib.Path(tmpdirname)
        for file_path in directory.iterdir():
            zip_file.write(file_path, arcname=file_path.name)

    return zip_file_path
    
    # Upload the .zip file to S3
    #with open(zip_file_path, 'rb') as data:
        #s3.upload_fileobj(data, bucket, zip_file_destination_s3_key + '/' + zip_file_path)
    
    

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
        
            
@timeit('download_videos() run')   
def run(user: str,
        date: datetime) -> bool:  
    # Uses temp directorty to download the files and upload to S3
    s3 = boto3.client('s3')
    with tempfile.TemporaryDirectory(dir=config.ROOT_DIRECTORY, prefix=f'{user}_') as tmpdirname:
        date = datetime.datetime.now()
        extract_file = config.ExtractPath(date=date,
                                          user=user).data_path_file
        user_data = pd.read_csv(extract_file)
        video_ids = list(set(user_data['video_id']))
        start_time = time.time()
        
        
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(start_async_process(video_ids,
                                                           tmpdirname,
                                                           user))
        loop.run_until_complete(future)
        
        elapsed_time = time.time() - start_time
        total_time = round(elapsed_time, 4)
        print(f'Individual video downloads for {user} | complete in {total_time:.4f} seconds')
        
        try:
            zip_file_path = zip_videos(user=user,
                date=date,
                tmpdirname=tmpdirname)
        except Exception as e:
            print(e)
            print(f'Zipping files for {user} failed!')
            return False
        
        try: 
            s3.upload_file(zip_file_path, config.BUCKET, config.ExtractPath(user=user, date=date).video_path_file_s3_key)
            os.remove(zip_file_path)
            return True
                
        except Exception as e:
            print(e)
            print(f'Uploading zip file to S3 for {user} S3 failed!')
            return False

if __name__ == "__main__":
    run(user='thephotoverse',
        date=datetime.datetime.now())
    