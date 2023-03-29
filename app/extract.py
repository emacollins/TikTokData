import json
import pandas as pd
import datetime
import config
import boto3
import airtable_utils
from vidvault_utils import timeit

def get_user_threshold(airtable_row_id: str):
    """Implement dynamic thershold. All users default to 0.99
    to start. Extract will check how many videos we scraped from harvest and 
    compare to the threshold. videos_scraped / videos_scraped_threshold. If this ratio is less
    than the threshold, we will descrease threshold and rerun"""
    table = airtable_utils.get_table_data()
    df = airtable_utils.convert_to_dataframe(airtable_table=table)
    df = df.set_index('airtable_row_id')
    threshold = df.loc[airtable_row_id, 'videos_scraped_threshold']
    return threshold
    
def data_store_dict_return() -> dict:
    
    data_store_dict = {'video_id': [],
                   'user_unique_id': [],
                   'video_create_time': [],
                   'video_digg_count': [],
                   'video_share_count': [],
                   'video_comment_count': [],
                   'video_play_count': [],
                   'hashtags': []}
    return data_store_dict

def get_user_video_count(data: dict, user: str):
    """Gets the user's video count for quality check
    
    """
    video_count = data['UserModule']['stats'][user]['videoCount']
    return video_count

    
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
        try:
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
                    
            df_page = pd.DataFrame(data=data_store_dict)
            all_videos.append(df_page)
        except Exception as e:
            print(f'Extras page of harvest data skipped for user {user}')
            pass
    df = pd.concat(all_videos)
    return df


def check_video_count(df: pd.DataFrame, 
                      data: dict,
                      user: str,
                      airtable_row_id: str) -> bool:
    """checks if all video data was collected for user

    Args:
        df (pd.DataFrame): DataFrame of single users data
        data (dict): raw harvested data
    Returns:
        bool: True if all videos scraped, false if missing
    """
    videos_scraped = len(df['video_id'].unique())
    videos_expected = get_user_video_count(data=data, user=user)
    user_threshold = get_user_threshold(airtable_row_id=airtable_row_id)
    
    airtable_utils.update_database_cell(row_id=airtable_row_id,
                                        field='videos_scraped',
                                        value=videos_scraped)
    airtable_utils.update_database_cell(row_id=airtable_row_id,
                                        field='total_videos',
                                        value=videos_expected)
    
    if videos_scraped / videos_expected < user_threshold: # Scraper doesnt always work, set threshold to 95% or it will fail and rerun
        new_user_threshold = user_threshold - config.VIDEOS_SCRAPED_THRESHOLD_DECREASE
        airtable_utils.update_database_cell(row_id=airtable_row_id,
                                            field='videos_scraped_threshold',
                                            value=new_user_threshold)
        print(f'{user} did not meet the videos scraped threshold, retrying!')
        assert 1 == 0
    
    
    if videos_scraped == videos_expected:
        return True

    else:
        print(f'{user} did not get all videos. {videos_scraped} / {videos_expected} scraped, but threshold met.')
        return False
     
def extract(data: dict, user: str, airtable_row_id: str) -> pd.DataFrame:
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
    
    df_recent_videos = pd.DataFrame(data=data_store_dict)
    if data['extras']:
        df_extra_videos = extract_extras(data=data, user=user)
    else:
        df_extra_videos = pd.DataFrame()
    df = pd.concat([df_recent_videos, df_extra_videos])
    check_video_count(df=df, data=data, user=user, airtable_row_id=airtable_row_id)
    return df

@timeit(message='Extract')
def run(user: str,
        date: datetime.datetime,
        airtable_row_id: str):

    s3 = boto3.client('s3')
    s3_object = s3.get_object(Bucket=config.BUCKET, Key=config.HarvestPath(user=user,
                                                                            date=date).user_data_path_file_s3_key)
    data = json.loads(s3_object['Body'].read())
    df_final = extract(data=data, user=user,airtable_row_id=airtable_row_id)
    df_final['data_date'] = datetime.datetime.now().date()
    df_final['video_create_time'] = pd.to_datetime(df_final['video_create_time'],unit='s')
    df_final = df_final.drop_duplicates(subset=['video_id'])
    df_final.set_index('video_id', inplace=True)
    df_final.to_csv(config.ExtractPath(user=user, 
                                       date=date).data_path_file)
    return True

if __name__ == '__main__':
    run(user='hi.surya',
        date=datetime.datetime.now(),
        airtable_row_id='recLyU8bBKQl2tAJD')
    