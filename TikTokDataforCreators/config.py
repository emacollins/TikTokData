import datetime
import pandas as pd


USE_LOCAL_PATHS = False
LOCAL_PATH_PREFIX = '/Users/ericcollins/TikTokData/'
S3_URI_PATH_PREFIX = 's3://vidvault-app/TikTokData/'

# Path directories
class HarvestPath:
    def __init__(self,
                 user: str,
                 date: datetime.datetime = None,
                 datetime_format: str = '%m-%d-%Y',
                 video_id: str = None) -> None:
        
        self.date = date
        self.datetime_format = datetime_format
        self.user = user
        self.video_id = video_id
        if USE_LOCAL_PATHS:
            # For local testing
            self.get_harvest_local_paths()
        else:
            self.get_harvest_s3_paths()

    def get_harvest_local_paths(self):
            if self.date:
                self.user_data_path_file = LOCAL_PATH_PREFIX + f'TikTokDataforCreators/harvest/data/{self.user}/{self.date.strftime(self.datetime_format)}.json'
                self.user_data_path_file_raw_api = LOCAL_PATH_PREFIX + f'TikTokDataforCreators/harvest/data/{self.user}/{self.date.strftime(self.datetime_format)}.json.UserResponse.json'
                self.video_path = LOCAL_PATH_PREFIX + f'TikTokDataforCreators/harvest/videos/{self.user}/date/{self.date.strftime(self.datetime_format)}'

            if self.video_id:
                self.video_path_file = LOCAL_PATH_PREFIX + f'TikTokDataforCreators/harvest/videos/{self.user}/date/{self.date.strftime(self.datetime_format)}/{self.video_id}.mp4'
            
            self.user_data_path = LOCAL_PATH_PREFIX + f'TikTokDataforCreators/harvest/data/{self.user}'
    
    def get_harvest_s3_paths(self):
        if self.date:
            self.user_data_path_file = S3_URI_PATH_PREFIX + f'TikTokDataforCreators/harvest/data/{self.user}/{self.date.strftime(self.datetime_format)}.json'
            self.user_data_path_file_raw_api = S3_URI_PATH_PREFIX + f'TikTokDataforCreators/harvest/data/{self.user}/{self.date.strftime(self.datetime_format)}.json.UserResponse.json'
            self.video_path = S3_URI_PATH_PREFIX + f'TikTokDataforCreators/harvest/videos/{self.user}/date/{self.date.strftime(self.datetime_format)}'

        if self.video_id:
            self.video_path_file = S3_URI_PATH_PREFIX + f'TikTokDataforCreators/harvest/videos/{self.user}/date/{self.date.strftime(self.datetime_format)}/{self.video_id}.mp4'
        
        self.user_data_path = S3_URI_PATH_PREFIX + f'TikTokDataforCreators/harvest/data/{self.user}'

class ExtractPath:
    def __init__(self,
                 date: datetime.datetime = None,
                 user: str = None,
                 datetime_format: str = '%m-%d-%Y') -> None:
        
        self.user = user
        self.date = date
        self.datetime_format = datetime_format
        if USE_LOCAL_PATHS:
            self.data_path = LOCAL_PATH_PREFIX + f'TikTokDataforCreators/extract/data'
            if self.date:
                self.data_path_file = LOCAL_PATH_PREFIX + f'TikTokDataforCreators/extract/data/extract_{self.date.strftime(self.datetime_format)}.csv'
            if self.user:
                self.video_path = LOCAL_PATH_PREFIX + f'TikTokDataforCreators/extract/videos/{self.user}'
            if (self.user != None) & (self.date != None):
                self.video_path_file = LOCAL_PATH_PREFIX + f'TikTokDataforCreators/extract/videos/{self.user}/downloaded_videos_{self.user}-{self.date.strftime(self.datetime_format)}'
        else:
            self.data_path = S3_URI_PATH_PREFIX + f'TikTokDataforCreators/extract/data'
            if self.date:
                self.data_path_file = S3_URI_PATH_PREFIX + f'TikTokDataforCreators/extract/data/extract_{self.date.strftime(self.datetime_format)}.csv'
            if self.user:
                self.video_path = S3_URI_PATH_PREFIX + f'TikTokDataforCreators/extract/videos/{self.user}'
            if (self.user != None) & (self.date != None):
                self.video_path_file = S3_URI_PATH_PREFIX + f'TikTokDataforCreators/extract/videos/{self.user}/downloaded_videos_{self.user}-{self.date.strftime(self.datetime_format)}'
                
class LoadPath:
    def __init__(self):
        if USE_LOCAL_PATHS:
            self.data_path_file = LOCAL_PATH_PREFIX + f'TikTokDataforCreators/load/data/tiktokdata.csv'
            self.data_path = LOCAL_PATH_PREFIX + f'TikTokDataforCreators/load/data'
        else:
            self.data_path_file = S3_URI_PATH_PREFIX + f'TikTokDataforCreators/load/data/tiktokdata.csv'
            self.data_path = S3_URI_PATH_PREFIX + f'TikTokDataforCreators/load/data'
    
    
            
        
        
        
class UserSignUpPath:
    def __init__(self) -> None:
        if USE_LOCAL_PATHS:
            self.cached_user_table = LOCAL_PATH_PREFIX + 'TikTokDataforCreators/user_sign_up/tiktok_accounts_to_track.csv'
            self.bad_users = LOCAL_PATH_PREFIX + 'TikTokDataforCreators/user_sign_up/bad_users.csv'  #  Usernames that caused errors on the harvest
        else:
            self.cached_user_table = S3_URI_PATH_PREFIX + 'TikTokDataforCreators/user_sign_up/tiktok_accounts_to_track.csv'
            self.bad_users = S3_URI_PATH_PREFIX + 'TikTokDataforCreators/user_sign_up/bad_users.csv'  #  Usernames that caused errors on the harvest