import datetime
import pandas as pd
import boto3
# TODO: Create a temp directory class


#LOCAL PATH TESTING
"""
LOCAL_PATH_PREFIX = '/Users/ericcollins/'
ROOT_DIRECTORY = '/Users/ericcollins/TikTokData/app'
    
"""

LOCAL_PATH_PREFIX = '/app/'
ROOT_DIRECTORY = '/app'

BUCKET = 'vidvault-app'
S3_URI_PATH_PREFIX = 's3://vidvault-app/'
VIDEOS_SCRAPED_THRESHOLD_DECREASE = 0.01
MINIMUM_VIDOES_SCRAPED_ACCURACY_THRESHOLD = 0.90
HARVEST_SCROLL_TIME = {'MAX': 250,
                       'MIN': 5}

# Path directories
class HarvestPath:
    def __init__(self,
                 user: str,
                 date: datetime.datetime = None,
                 datetime_format: str = '%m-%d-%Y',
                 video_id: str = None) -> None:
        self.bucket = BUCKET
        self.date = date
        self.datetime_format = datetime_format
        self.user = user
        self.video_id = video_id
        if self.date:
            self.user_data_path_file = S3_URI_PATH_PREFIX + f'TikTokData/TikTokDataforCreators/harvest/data/{self.user}/{self.date.strftime(self.datetime_format)}.json'
            self.user_data_path_file_raw_api = S3_URI_PATH_PREFIX + f'TikTokData/TikTokDataforCreators/harvest/data/{self.user}/{self.date.strftime(self.datetime_format)}.json.UserResponse.json'
            self.video_path = S3_URI_PATH_PREFIX + f'TikTokData/TikTokDataforCreators/harvest/videos/{self.user}/date/{self.date.strftime(self.datetime_format)}'
            
            self.user_data_path_file_s3_key = f'TikTokData/TikTokDataforCreators/harvest/data/{self.user}/{self.date.strftime(self.datetime_format)}.json'
            self.video_path_s3_key = f'TikTokData/TikTokDataforCreators/harvest/videos/{self.user}/date/{self.date.strftime(self.datetime_format)}'

        if self.video_id:
            self.video_path_file = S3_URI_PATH_PREFIX + f'TikTokData/TikTokDataforCreators/harvest/videos/{self.user}/date/{self.date.strftime(self.datetime_format)}/{self.video_id}.mp4'
            
            self.video_path_file_s3_key = f'TikTokData/TikTokDataforCreators/harvest/videos/{self.user}/date/{self.date.strftime(self.datetime_format)}/{self.video_id}.mp4'

        
        self.user_data_path = S3_URI_PATH_PREFIX + f'TikTokData/TikTokDataforCreators/harvest/data/{self.user}'
        self.user_data_path_s3_key = f'TikTokData/TikTokDataforCreators/harvest/data/{self.user}'

class ExtractPath:
    def __init__(self,
                 date: datetime.datetime = None,
                 user: str = None,
                 datetime_format: str = '%m-%d-%Y') -> None:
        self.bucket = BUCKET
        self.user = user
        self.date = date
        self.datetime_format = datetime_format
        self.data_path = S3_URI_PATH_PREFIX + f'TikTokData/TikTokDataforCreators/extract/data/{self.user}'
        self.data_path_s3_key = f'TikTokData/TikTokDataforCreators/extract/data/{self.user}'
        if self.user:
            self.video_path = S3_URI_PATH_PREFIX + f'TikTokData/TikTokDataforCreators/extract/videos/{self.user}'
            self.video_path_s3_key = f'TikTokData/TikTokDataforCreators/extract/videos/{self.user}'
        if (self.user != None) & (self.date != None):
            self.video_path_file = S3_URI_PATH_PREFIX + f'TikTokData/TikTokDataforCreators/extract/videos/{self.user}/{self.date.strftime(self.datetime_format)}'
            self.video_path_file_s3_key = f'TikTokData/TikTokDataforCreators/extract/videos/{self.user}/{self.user}_downloaded_videos_{self.date.strftime(self.datetime_format)}.zip'
            
            self.data_path_file = S3_URI_PATH_PREFIX + f'TikTokData/TikTokDataforCreators/extract/data/{self.user}/{self.date.strftime(self.datetime_format)}.csv'
            self.data_path_file_s3_key = f'TikTokData/TikTokDataforCreators/extract/data/{self.user}/{self.date.strftime(self.datetime_format)}.zip'

                
class LoadPath:
    def __init__(self,
                 user: str):
        self.user = user
        self.bucket = BUCKET
        self.data_path_file = S3_URI_PATH_PREFIX + f'TikTokData/TikTokDataforCreators/load/data/{self.user}/data.csv'    
    
            
        
        
        
class UserSignUpPath:
    
    def __init__(self) -> None:
        self.bucket = BUCKET
        self.cached_user_table = S3_URI_PATH_PREFIX + 'TikTokData/TikTokDataforCreators/user_sign_up/tiktok_accounts_to_track.csv'
        self.bad_users = S3_URI_PATH_PREFIX + 'TikTokData/TikTokDataforCreators/user_sign_up/bad_users.csv'  #  Usernames that caused errors on the harvest

class Secret_Key:
    def __init__(self, key_name: str) -> None:
        self.ssm = boto3.client('ssm')
        self.key_name = key_name
        self.value = self.ssm.get_parameter(Name=self.key_name, WithDecryption=True)['Parameter']['Value']
