# download all tiktok video from user
# edit video from entire directory using moviepy
# Created by HengSok


from turtle import down
from httplib2 import UnimplementedHmacDigestAuthOptionError
import requests
import tempfile
import datetime
import pandas as pd
import config
import boto3
import zipfile
import os
import swifter



# Download All Video From Tiktok User Function
def download_video_no_watermark(user: str,
                                video_id: int,
                                tmpdirname: str):
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
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file(tmp_video_file, 
                                Bucket=config.BUCKET, 
                                Key=config.HarvestPath(user=user,
                                                        date=date,
                                                        video_id=video_id).video_path_file_s3_key,
                                ExtraArgs=None, 
                                Callback=None, 
                                Config=None)  


def zip_videos(user: str,
        date: datetime.datetime):
    # The S3 bucket containing the .mp4 files
    bucket = config.BUCKET

    # The S3 folder containing the .mp4 files
    source_folder = config.HarvestPath(user=user,
                                       date=date).video_path_s3_key

    # The S3 folder to which the .zip file will be saved
    destination_folder = config.ExtractPath(user=user).video_path_s3_key

    # List all .mp4 files in the source folder
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(Bucket=bucket, Prefix=source_folder)
    mp4_files = [content['Key'] for content in response['Contents'] if content['Key'].endswith('.mp4')]
    
    date_string = date.strftime('%m-%d-%Y')
    zip_file_name = f'downloaded_videos_{date_string}.zip'
    # Zip the .mp4 files
    with zipfile.ZipFile(zip_file_name, 'w') as zip_file:
        for file in mp4_files:
            # Download the file from S3
            s3.download_file(bucket, file, file.split('/')[-1])
            # Add the file to the zip archive
            zip_file.write(file.split('/')[-1])

    # Upload the .zip file to S3
    with open(zip_file_name, 'rb') as data:
        s3.upload_fileobj(data, bucket, destination_folder + '/' + zip_file_name)
        
    # Delete the local .zip and .mp4 files
    os.remove(zip_file_name)
    for file in mp4_files:
        os.remove(file.split('/')[-1])

   
def run(user: str,
        date: datetime) -> bool:  
    # Uses temp directorty to download the files and upload to S3
    with tempfile.TemporaryDirectory(dir=config.LOCAL_PATH_PREFIX) as tmpdirname:
        date = datetime.datetime.now()
        extract_file = config.ExtractPath(date=date,
                                          user=user).data_path_file
        user_data = pd.read_csv(extract_file)
        
        for video_id in user_data['video_id'].unique():
            download_video_no_watermark(user=user,
                                        video_id=video_id,
                                        tmpdirname=tmpdirname)
        
    try:
        zip_videos(user=user,
               date=date)
        return True
    except Exception as e:
        print(e)
        return False

if __name__ == "__main__":
    run(user='tytheproductguy',
        date=datetime.datetime.now())
    