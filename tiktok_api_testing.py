from asyncio import constants
from tiktokapipy.api import TikTokAPI
import pandas as pd
import constants

def sample_return_data():
    with TikTokAPI() as api:
        user = api.user('figapp')
        for video in user.videos.sorted_by(key=lambda vid: vid.create_time, reverse=False):
            for tags in video.tags:
                print(user)
                print('$$$$$$$$')
                print(video.stats)
                print('-----')
                print(tags)
                print('**********')
                

def data_store_dict() -> dict:
    data_store = 


    

def get_user_videos(username: str) -> pd.DataFrame:
    """Takes in username and cleans relevant data for that user."""
    data_store_dict = dict.fromkeys(constants.all_fields.needed)
    with TikTokAPI() as api:
        user = api.user('figapp')
        
sample_return_data()