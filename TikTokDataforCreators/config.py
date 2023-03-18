import datetime


USE_LOCAL_PATHS = True


# Path directories
class HarvestPath:
    def __init__(self,
                 user: str,
                 date: datetime.datetime,
                 local_paths: bool = USE_LOCAL_PATHS, 
                 datetime_format: str = '%d-%m-%Y',
                 video_id: str = None) -> None:
        
        
        self.local_paths = local_paths
        self.date = date
        self.datetime_format = datetime_format
        self.user = user
        self.video_id = video_id
        if local_paths:
            self._get_local_paths()
        
        
    def _get_local_paths(self):
        self.user_data_path_file = f'/Users/ericcollins/TikTokData/TikTokDataforCreators/harvest/data/{self.user}/{self.date.strftime(self.datetime_format)}.json'
        self.user_data_path = f'/Users/ericcollins/TikTokData/TikTokDataforCreators/harvest/data/{self.user}'
        
        if self.video_id:
            self.video_path_file = f'/Users/ericcollins/TikTokData/TikTokDataforCreators/harvest/videos/{self.user}/{self.date.strftime(self.datetime_format)}/{self.video_id}.mp4'
        self.video_path = f'/Users/ericcollins/TikTokData/TikTokDataforCreators/harvest/videos/{self.user}'
        


class ExtractPath:
    def __init__(self,
                 user: str, 
                 date: datetime.datetime, 
                 datetime_format: str = '%d-%m-%Y') -> None:
        self.user = user
        self.data_path = f'/Users/ericcollins/TikTokData/TikTokDataforCreators/extract/data/{date.strftime(datetime_format)}'
        self.video_path = f'/Users/ericcollins/TikTokData/TikTokDataforCreators/harvest/videos/{self.user}'
        
        
class UserSignUpPath:
    def __init__(self) -> None:
        self.airtable_raw_data = ''
        self.cached_user_table = '/Users/ericcollins/TikTokData/TikTokDataforCreators/user_sign_up'
