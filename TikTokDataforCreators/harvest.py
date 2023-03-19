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
import boto3
# Adjust this to capture more videos
# API works by scrolling down page on TikTok,
# if user has a lot fo videos, scroll time should be longer
SCROLL_TIME = 1
S3 = boto3.resource('s3')

def get_user_video_data(username: str) -> bool:
    """Takes in username and cleans relevant data for that user."""
    #if not os.path.exists(directory.user_data_path):
        #os.makedirs(directory.user_data_path)
        
    date = datetime.datetime.now()
    date_string = date.strftime('%m-%d-%Y')
    with tempfile.TemporaryDirectory(dir=config.LOCAL_PATH_PREFIX) as tmpdirname:
        filename = tmpdirname + f'/{date_string}'
        with TikTokAPI(scroll_down_time=SCROLL_TIME,navigation_retries=5, navigation_timeout=0, 
                    data_dump_file=filename) as api:
            try:
                user_object = api.user(username, video_limit=0)
                if not config.USE_LOCAL_PATHS:
                    upload_to_s3(directory=tmpdirname,
                                 user=username,
                                 date=date)
                return True
            except Exception as e:
                print(e)
                print(f'Harvest data error on {username}')
                return False

          
def upload_to_s3(directory: str,
                 user: str,
                 date: datetime.datetime):
    for file in os.listdir(directory):
                    S3.meta.client.upload_file(directory + '/' + file, 
                                        Bucket=config.BUCKET, 
                                        Key=config.HarvestPath(user=user,
                                                               date=date).user_data_path_file_s3_key,
                                        ExtraArgs=None, 
                                        Callback=None, 
                                        Config=None)  
    

def clean_file_names(directory: str,
                     user: str):
    date = datetime.datetime.now().strftime('%-%d-%Y')
    for file in os.listdir(directory):
        try:
            os.rename(directory + '/' + file, f'{user}/{date}.json')
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
        saved_user_data = get_user_video_data(username=user)
        if not saved_user_data:
            bad_users['username'].append(user)
            bad_users_history = pd.read_csv(config.UserSignUpPath().bad_users)
            bad_users_current_harvest = pd.DataFrame(data=bad_users)
            bad_users = pd.concat([bad_users_history, bad_users_current_harvest])
            bad_users.to_csv(config.UserSignUpPath().bad_users, index=False)
            
        
if __name__ == '__main__':
    run()
    