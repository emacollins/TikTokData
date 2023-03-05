from tiktokapipy.api import TikTokAPI
import pandas as pd
from traitlets import Bool
import datetime



def get_user_video_data(username: str) -> pd.DataFrame:
    """Takes in username and cleans relevant data for that user."""
    date = datetime.datetime.now().strftime('%m-%d-%Y')
    filepath = f'harvest/{date}_{username}'
    with TikTokAPI(navigation_retries=2, navigation_timeout=10, 
                   data_dump_file=filepath) as api:
        user = api.user(username)

def run():
    users = pd.read_csv('/Users/ericcollins/TikTokData/TikTokDataforCreators/tiktok_accounts_to_track.csv')['user']
    for user in users:
        get_user_video_data(username=user)

if __name__ == '__main__':
    users = pd.read_csv('/Users/ericcollins/TikTokData/TikTokDataforCreators/tiktok_accounts_to_track.csv')['user']
    for user in users:
        get_user_video_data(username=user, influencer=True)
        