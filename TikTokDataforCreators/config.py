import datetime


USE_LOCAL_PATHS = True


# Path directories
class HarvestPath:
    def __init__(self,
                 user: str,
                 date: datetime.datetime = None ,
                 local_paths: bool = USE_LOCAL_PATHS, 
                 datetime_format: str = '%m-%d-%Y',
                 video_id: str = None) -> None:
        
        
        self.local_paths = local_paths
        self.date = date
        self.datetime_format = datetime_format
        self.user = user
        self.video_id = video_id
        if self.local_paths:
            self._get_local_paths()
        
        
    def _get_local_paths(self):
        if self.date:
            self.user_data_path_file = f'/Users/ericcollins/TikTokData/TikTokDataforCreators/harvest/data/{self.user}/{self.date.strftime(self.datetime_format)}.json'
            self.user_data_path_file_raw_api = f'/Users/ericcollins/TikTokData/TikTokDataforCreators/harvest/data/{self.user}/{self.date.strftime(self.datetime_format)}.json.UserResponse.json'
            self.video_path = f'/Users/ericcollins/TikTokData/TikTokDataforCreators/harvest/videos/{self.user}/date/{self.date.strftime(self.datetime_format)}'

        if self.video_id:
            self.video_path_file = f'/Users/ericcollins/TikTokData/TikTokDataforCreators/harvest/videos/{self.user}/date/{self.date.strftime(self.datetime_format)}/{self.video_id}.mp4'
        
        self.user_data_path = f'/Users/ericcollins/TikTokData/TikTokDataforCreators/harvest/data/{self.user}'



class ExtractPath:
    def __init__(self,
                 date: datetime.datetime = None,
                 user: str = None,
                 datetime_format: str = '%m-%d-%Y',
                 local_paths: bool = USE_LOCAL_PATHS) -> None:
        self.local_paths = local_paths
        self.user = user
        self.date = date
        self.datetime_format = datetime_format
        if self.local_paths:
            self.data_path = f'/Users/ericcollins/TikTokData/TikTokDataforCreators/extract/data'
            if self.date:
                self.data_path_file = f'/Users/ericcollins/TikTokData/TikTokDataforCreators/extract/data/extract_{self.date.strftime(self.datetime_format)}.csv'
            if self.user:
                self.video_path = f'/Users/ericcollins/TikTokData/TikTokDataforCreators/extract/videos/{self.user}'
            if (self.user != None) & (self.date != None):
                self.video_path_file = f'/Users/ericcollins/TikTokData/TikTokDataforCreators/extract/videos/{self.user}/downloaded_videos_{self.user}-{self.date.strftime(self.datetime_format)}'

class LoadPath:
    def __init__(self,
                 local_paths: bool):
        self.local_paths = local_paths
        if self.local_paths:
            self.data_path_file = f'Users/ericcollins/TikTokData/TikTokDataforCreators/load/data/tiktokdata.csv'
            self.data_path = f'Users/ericcollins/TikTokData/TikTokDataforCreators/load/data'
        
    
    
            
        
        
        
class UserSignUpPath:
    def __init__(self) -> None:
        self.airtable_raw_data = ''
        self.cached_user_table = '/Users/ericcollins/TikTokData/TikTokDataforCreators/user_sign_up/tiktok_accounts_to_track.csv'
