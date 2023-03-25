from tiktokapipy.api import TikTokAPI
import os
import datetime
import config
import logging
import tempfile
import boto3
import json
import extract

# Adjust this to capture more videos
# API works by scrolling down page on TikTok,
# if user has a lot fo videos, scroll time should be longer

def get_scroll_time(user: str):
    filename = f'{user}'
    with TikTokAPI(scroll_down_time=1,navigation_retries=5, navigation_timeout=0, 
                    data_dump_file=filename) as api:
        user_object = api.user(user, video_limit=0)
    filename2 = f'{user}.UserResponse.json'
    with open(filename2, 'r') as file:
        json_data = json.load(file)
    video_count = json_data['UserModule']['stats'][user]['videoCount']
    scroll_time = video_count / 5
    os.remove(filename2)
    return scroll_time
        


def run(user: str, 
        date: datetime.datetime) -> bool:
    """Takes in username and cleans relevant data for that user."""
    scroll_time = get_scroll_time(user=user)
    date_string = date.strftime('%m-%d-%Y')
    with tempfile.TemporaryDirectory(dir=config.LOCAL_PATH_PREFIX) as tmpdirname:
        filename = tmpdirname + f'/{date_string}'
        with TikTokAPI(scroll_down_time=scroll_time,navigation_retries=5, navigation_timeout=0, 
                    data_dump_file=filename) as api:
            try:
                user_object = api.user(user, video_limit=0)
                upload_to_s3(directory=tmpdirname,
                                 user=user,
                                 date=date)
                return True
            except Exception as e:
                print(e)
                print(f'Harvest data error on {user}')
                return False

          
def upload_to_s3(directory: str,
                 user: str,
                 date: datetime.datetime):
    
    s3 = boto3.resource('s3')
    for file in os.listdir(directory):
                    s3.meta.client.upload_file(directory + '/' + file, 
                                        Bucket=config.BUCKET, 
                                        Key=config.HarvestPath(user=user,
                                                               date=date).user_data_path_file_s3_key,
                                        ExtraArgs=None, 
                                        Callback=None, 
                                        Config=None)  
    
if __name__ == '__main__':
    user = 'tylerandhistummy'
    date = datetime.datetime.now()
    run(user='tytheproductguy',
        date=datetime.datetime.now())
    extract.run(user=user,
                date=date)