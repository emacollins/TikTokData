# download all tiktok video from user
# edit video from entire directory using moviepy
# Created by HengSok


from turtle import down
import requests,os,time
import random 
from random import choice
import datetime
import pandas as pd



# Download All Video From Tiktok User Function
def download_video_no_watermark(user: str,
                                video_id: int):

    url = "https://tiktok-downloader-download-tiktok-videos-without-watermark.p.rapidapi.com/vid/index"

    querystring = {"url":f'https://www.tiktok.com/@{user}r/video/{str(video_id)}'}

    headers = {
        "X-RapidAPI-Key": "847b077d69mshaeae673f63126b4p1f4fa8jsn47c20efef8f4",
        "X-RapidAPI-Host": "tiktok-downloader-download-tiktok-videos-without-watermark.p.rapidapi.com"
    }

    video_object = requests.request("GET", url, headers=headers, params=querystring).json()
    
    if not os.path.exists(f"/Users/ericcollins/TikTokData/TikTokDataforCreators/videos/{user}"):
        os.makedirs(f"/Users/ericcollins/TikTokData/TikTokDataforCreators/videos/{user}")
        
    download_url = video_object['video'][0]
                       
    video_bytes = requests.get(download_url, stream=True)
    with open(f'/Users/ericcollins/TikTokData/TikTokDataforCreators/videos/{user}/{str(video_id)}.mp4', 'wb') as out_file:
        out_file.write(video_bytes.content)
    
def run():
    date_string = datetime.datetime.now().date().strftime('%m-%d-%Y')
    extract_file = f'/Users/ericcollins/TikTokData/TikTokDataforCreators/extract/extract_{date_string}.csv'
    extract_data = pd.read_csv(extract_file)
    
    for user in extract_data['user_unique_id'].unique():
        user_data = extract_data.loc[extract_data['user_unique_id'] == user]
        for video_id in user_data['video_id']:
            download_video_no_watermark(user=user,
                                        video_id=video_id)
    
if __name__ == "__main__":
    run()