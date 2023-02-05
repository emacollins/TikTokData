import json
import pandas as pd
import datetime
import os

ACCOUNT_LIST = pd.read_csv('tiktok_accounts_to_track.csv', index_col='user')

def data_store_dict_return() -> dict:
    
    data_store_dict = {'video_id': [],
                   'user_unique_id': [],
                   'video_create_time': [],
                   'video_digg_count': [],
                   'video_share_count': [],
                   'video_comment_count': [],
                   'video_play_count': [],
                   'hashtags': [],
                   'used_proper_hastags': []}
    return data_store_dict

def check_tags(video_tags: list, username: str) -> bool:
    """Checks if the creator used at least one of their expected hashtags on the video"""
    is_influencer = bool(ACCOUNT_LIST.loc[username, 'influencer'])
    if not is_influencer:
        return True
    expected_tags = str(ACCOUNT_LIST.loc[username, 'hashtags']).split(';')
    check = any(x in video_tags for x in expected_tags)
    return check

def extract(data: dict, user: str) -> pd.DataFrame:
    data_store_dict = data_store_dict_return()
    for video_id, video_values in data['ItemModule'].items():
        data_store_dict['video_id'].append(video_id)
        data_store_dict['user_unique_id'].append(user)
        data_store_dict['video_create_time'].append(video_values['createTime'])
        data_store_dict['video_digg_count'].append(video_values['stats']['diggCount'])
        data_store_dict['video_share_count'].append(video_values['stats']['shareCount'])
        data_store_dict['video_comment_count'].append(video_values['stats']['commentCount'])
        data_store_dict['video_play_count'].append(video_values['stats']['playCount'])
        challenges = []
        if len(video_values['challenges']) != 0:
            for challenge in video_values['challenges']:
                challenges.append(challenge['title'])
            tags_string = ';'.join(map(str, challenges))
            data_store_dict['hashtags'].append(tags_string)
        else:
            data_store_dict['hashtags'].append('')
        
        if check_tags(video_tags=challenges, username=user):
            data_store_dict['used_proper_hastags'].append(True)
        else:
            data_store_dict['used_proper_hastags'].append(False)
    
    df = pd.DataFrame(data=data_store_dict)
    return df


def run():
    users = list(ACCOUNT_LIST.index)
    user_data = []
    for user in users:
        filename = f'harvest/{user}.UserResponse.json'
        with open(filename, 'r') as f:
            data = json.load(f)
        df_user = extract(data=data, user=user)
        user_data.append(df_user)
    df_final = pd.concat(user_data)
    df_final['data_date'] = datetime.datetime.now().date()
    df_final.set_index('video_id', inplace=True)
    date = datetime.datetime.now().strftime('%m-%d-%Y')
    extract_file_name = f'extract_{date}.csv'
    if extract_file_name in os.listdir('/Users/ericcollins/TikTokData/extract/'):
        df_final.to_csv(f'extract_{date}.csv')
        print('Extract Complete!')
    else:
        print('Data already extracted today!')
        return False
        

if __name__ == '__main__':
    run()