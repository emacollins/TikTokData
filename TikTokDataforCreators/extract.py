import json
import pandas as pd
import datetime
import os
import config
import boto3

# Drops users that errored in the harvest step
ACCOUNT_LIST = pd.read_csv(config.UserSignUpPath().cached_user_table, index_col='user')
BAD_USERS = pd.read_csv(config.UserSignUpPath().bad_users)
ACCOUNT_LIST = ACCOUNT_LIST.drop(BAD_USERS['username'])

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

def get_user_video_count(data: dict, user: str):
    """Gets the user's video count for quality check
    
    """
    video_count = data['UserModule']['stats'][user]['videoCount']
    return video_count

def check_extras(data: dict):
    """Checks if there are extra data (more than 30 videos)"""
    extras = data['extras']
    
    
def extract_extras(data: dict, user: str):
    """Extracts out the extra field in the harvested data

    Args:
        data (dict): Harvested Data File
        user (str): user in question
    """
    
    extras = data['extras']
    data_store_dict = data_store_dict_return()
    all_videos = []
    for contents in extras:
        videos = contents['itemList']
        for video_info in videos:
            data_store_dict['video_id'].append(video_info.get('id', ''))
            data_store_dict['user_unique_id'].append(user)
            data_store_dict['video_create_time'].append(video_info.get('createTime', ''))
            data_store_dict['video_digg_count'].append(video_info['stats']['diggCount'])
            data_store_dict['video_share_count'].append(video_info['stats']['shareCount'])
            data_store_dict['video_comment_count'].append(video_info['stats']['commentCount'])
            data_store_dict['video_play_count'].append(video_info['stats']['playCount'])

            challenges = video_info.get('challenges', False)
            if challenges:
                video_challenge_tags = []
                for challenge_info in challenges:
                     video_challenge_tags.append(challenge_info['title'])
                tags_string = ';'.join(map(str, video_challenge_tags))
                data_store_dict['hashtags'].append(tags_string)
            else:
                data_store_dict['hashtags'].append('')
                
            if check_tags(video_tags=challenges, username=user):
                data_store_dict['used_proper_hastags'].append(True)
            else:
                data_store_dict['used_proper_hastags'].append(False)
        df_page = pd.DataFrame(data=data_store_dict)
        all_videos.append(df_page)
    
    df = pd.concat(all_videos)
    return df


def check_video_count(df: pd.DataFrame, 
                      data: dict,
                      user: str) -> bool:
    """checks if all video data was collected for user

    Args:
        df (pd.DataFrame): DataFrame of single users data
        data (dict): raw harvested data
    Returns:
        bool: True if all videos scraped, false if missing
    """
    videos_scraped = len(df['video_id'].unique())
    videos_expected = get_user_video_count(data=data, user=user)
    
    if videos_scraped == videos_expected:
        return True
    else:
        print(f'{user} did not get all videos. {videos_scraped} / {videos_expected} scraped')
     
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
    
    df_recent_videos = pd.DataFrame(data=data_store_dict)
    if data['extras']:
        df_extra_videos = extract_extras(data=data, user=user)
    else:
        df_extra_videos = pd.DataFrame()
    df = pd.concat([df_recent_videos, df_extra_videos])
    check_video_count(df=df, data=data, user=user)
    return df


def run():
    users = list(ACCOUNT_LIST.index)
    user_data = []
    date = datetime.datetime.now()
    for user in users:
        s3 = boto3.client('s3')
        s3_object = s3.get_object(Bucket=config.BUCKET, Key=config.HarvestPath(user=user,
                                                                               date=date).user_data_path_file_s3_key)
        data = json.loads(s3_object['Body'].read())
        df_user = extract(data=data, user=user)
        user_data.append(df_user)
    df_final = pd.concat(user_data)
    df_final['data_date'] = datetime.datetime.now().date()
    df_final['video_create_time'] = pd.to_datetime(df_final['video_create_time'],unit='s')
    df_final = df_final.drop_duplicates(subset=['video_id'])
    df_final.set_index('video_id', inplace=True)
    df_final.to_csv(config.ExtractPath(date=date).data_path_file)
    return True

if __name__ == '__main__':
    run()
    