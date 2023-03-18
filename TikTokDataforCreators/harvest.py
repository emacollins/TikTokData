from httplib2 import UnimplementedHmacDigestAuthOptionError
from tiktokapipy.api import TikTokAPI
import pandas as pd
import os
from traitlets import Bool
import datetime
import config
import logging


def get_user_video_data(username: str) -> pd.DataFrame:
    """Takes in username and cleans relevant data for that user."""
    date = datetime.datetime.now().strftime('%m-%d-%Y')
    directory = config.HarvestPath(username, date=date)
    if not os.path.exists(directory):
        os.makedirs(directory)
    with TikTokAPI(scroll_down_time=10,navigation_retries=5, navigation_timeout=0, 
                   data_dump_file=directory) as api:
        try:
            user = api.user(username, video_limit=0)
        except:
            print(f'Harvest data error on {username}')


def run():
    directory = config.UserSignUpPath().cached_user_table
    users = pd.read_csv(directory)['user']
    for user in users:
        get_user_video_data(username=user)

if __name__ == '__main__':
    run()
    