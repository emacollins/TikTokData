from httplib2 import UnimplementedHmacDigestAuthOptionError
from tiktokapipy.api import TikTokAPI
import pandas as pd
import os
from traitlets import Bool
import datetime
import config
import logging
import download_videos

def get_user_video_data(username: str,
                        directory: config.HarvestPath) -> pd.DataFrame:
    """Takes in username and cleans relevant data for that user."""
    if not os.path.exists(directory.user_data_path):
        os.makedirs(directory.user_data_path)
    with TikTokAPI(scroll_down_time=10,navigation_retries=5, navigation_timeout=0, 
                   data_dump_file=directory.user_data_path_file) as api:
        try:
            user = api.user(username, video_limit=0)
        except:
            print(f'Harvest data error on {username}')
    clean_file_names(username=username,
                     directory=directory)

def clean_file_names(username: str,
                     directory: config.HarvestPath):
    for file in os.listdir(directory.video_path):
        os.rename(directory.user_data_path_file_raw_api, directory.user_data_path_file)
           
        
def run(harvest_videos: bool):
    user_sign_up_directory = config.UserSignUpPath().cached_user_table
    users = pd.read_csv(user_sign_up_directory)['user']
    for user in users:
        harvest_directory = config.HarvestPath(user, 
                                       date=datetime.datetime.now(),
                                       datetime_format='%m-%d-%Y')
        get_user_video_data(username=user,
                            directory=harvest_directory)
        
        
if __name__ == '__main__':
    run()
    