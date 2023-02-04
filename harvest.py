from tiktokapipy.api import TikTokAPI
import pandas as pd
from traitlets import Bool
import time



def get_user_video_data(username: str) -> pd.DataFrame:
    """Takes in username and cleans relevant data for that user."""
    filepath = f'data_files/{username}'
    with TikTokAPI(navigation_retries=2, navigation_timeout=10, 
                   data_dump_file=filepath) as api:
        user = api.user(username)
        print(f'{username} API Called')

def run():
    users = pd.read_csv('tiktok_accounts_to_track.csv')['user']
    for user in users:
        get_user_video_data(username=user, influencer=True)

if __name__ == '__main__':
    users = pd.read_csv('tiktok_accounts_to_track.csv')['user']
    for user in users:
        get_user_video_data(username=user, influencer=True)
        