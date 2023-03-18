from httplib2 import UnimplementedHmacDigestAuthOptionError
from tiktokapipy.api import TikTokAPI
import pandas as pd
import os
from traitlets import Bool
import datetime
import config
import logging
import download_videos
import airtable
import tempfile

# Adjust this to capture more videos
# API works by scrolling down page on TikTok,
# if user has a lot fo videos, scroll time should be longer
SCROLL_TIME = 10


def get_user_video_data(username: str,
                        directory: config.HarvestPath) -> bool:
    """Takes in username and cleans relevant data for that user."""
    if not os.path.exists(directory.user_data_path):
        os.makedirs(directory.user_data_path)
    with TikTokAPI(scroll_down_time=SCROLL_TIME,navigation_retries=5, navigation_timeout=0, 
                   data_dump_file=directory.user_data_path_file) as api:
        try:
            user = api.user(username, video_limit=0)
            clean_file_names(directory=directory)
            if not config.USE_LOCAL_PATHS:
                
            return True
        except:
            print(f'Harvest data error on {username}')
            clean_file_names(directory=directory)
            return False
            
    

def clean_file_names(directory: config.HarvestPath):
    for file in os.listdir(directory.user_data_path):
        try:
            os.rename(directory.user_data_path + '/' + file, directory.user_data_path_file)
        except:
            pass
 
def get_airtable_data():
    table = airtable.get_table_data()
    df = airtable.convert_to_dataframe(airtable_table=table)
    df.to_csv(config.UserSignUpPath().cached_user_table, index=False)
    
def run():
    get_airtable_data()  # Gets info from our airtable table
    user_sign_up_directory = config.UserSignUpPath().cached_user_table
    users = pd.read_csv(user_sign_up_directory)['user']
    bad_users = {'username': []}  #Used to collect the users that errored on data pull
    for user in users:
        harvest_directory = config.HarvestPath(user, 
                                       date=datetime.datetime.now(),
                                       datetime_format='%m-%d-%Y')
        saved_user_data = get_user_video_data(username=user,
                            directory=harvest_directory)
        if not saved_user_data:
            bad_users['username'].append(user)
            bad_users_history = pd.read_csv(config.UserSignUpPath().bad_users)
            bad_users_current_harvest = pd.DataFrame(data=bad_users)
            bad_users = pd.concat([bad_users_history, bad_users_current_harvest])
            bad_users.to_csv(config.UserSignUpPath().bad_users, index=False)
            
        
if __name__ == '__main__':
    run()
    