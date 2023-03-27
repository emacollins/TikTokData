from tiktokapipy.api import TikTokAPI
import os
import datetime
import config
import logging
import tempfile
import boto3
import json
import utils
import harvest
import extract
import download_videos
import pipeline

logger = logging.getLogger('run_log.' + __name__)

HARVEST_TEST_USER = 'figapp'

def get_scroll_time(user: str):
    filename = f'{user}'
    with TikTokAPI(scroll_down_time=1,navigation_retries=5, navigation_timeout=0, 
                    data_dump_file=filename) as api:
        try:
            user_object = api.user(user, video_limit=0)
        except Exception as e:
            logger.info(f'Harvest scrape error for {user}: {str(e)} - {utils.get_log_timestamp()}')
            assert 1 == 0, f'Harvest scrape error for {user}: {str(e)}'
        
    filename2 = f'{user}.UserResponse.json'
    with open(filename2, 'r') as file:
        json_data = json.load(file)
    video_count = json_data['UserModule']['stats'][user]['videoCount']
    os.remove(filename2)
    
    # Limit scroll time to within these ranges
    
    if video_count > config.HARVEST_SCROLL_TIME['MAX']:
        return config.HARVEST_SCROLL_TIME['MAX']
    
    elif video_count < config.HARVEST_SCROLL_TIME['MIN']:
        return config.HARVEST_SCROLL_TIME['MIN']
    else:
        return video_count     


def run(user: str, 
        date: datetime.datetime):
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
                
            except Exception as e:
                logger.info(f'Harvest scrape error for {user}: {str(e)} - {utils.get_log_timestamp()}')
                assert 1 == 0, f'Harvest scrape error on {user}: {str(e)}'

          
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
    date = datetime.datetime.now()
    run(user=HARVEST_TEST_USER,
        date=datetime.datetime.now())