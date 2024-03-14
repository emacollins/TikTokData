import datetime
import pandas as pd
import requests
import json
import airtable_utils
import config
import vidvault_utils
import time

ERROR_COUNT = 0

def flatten_list(l):
    return [item for sublist in l for item in sublist]

def check_for_api_error(data: dict) -> bool:
    
    global ERROR_COUNT
    
    if ERROR_COUNT > 10:
        assert 1 == 0, 'Error count of 10 reached on harvest'
    
    """Returns True if an error is found in the data"""
    if 'error' in data:
        if data['error'] == 'API error, please contact us.':
            print('Scrape API error, retrying!')
            time.sleep(1)
            ERROR_COUNT =+ 1
            return True
    if 'statusCode' in data:
        if data['statusCode'] == '10101':
            print('10101 Status code on scrape, retrying')
            time.sleep(1)
            ERROR_COUNT =+ 1
            return True
    if 'messages' in data:
        if data['messages'] == 'The API is unreachable, please contact the API provider':
            print('API is unreachable message on scrape, retrying')
            time.sleep(1)
            ERROR_COUNT =+ 1
            return True
    if 'status_code' in data:
        if data['status_code'] != 200:
            status_code = data['status_code']
    return False
    

def get_user_uid(user: str):

    url = "https://scraptik.p.rapidapi.com/get-user"

    querystring = {"username": user}

    headers = {
        "X-RapidAPI-Key": config.Secret_Key(key_name='SCRAPTIK_RAPIDAPI').value,
        "X-RapidAPI-Host": "scraptik.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)

    data = json.loads(response.text)
    return data['user']['uid']

def get_user_videos(user_id: str):
    hasMore = True
    cursor = "0"
    master_itemList = []
    while hasMore:
        

        url = "https://scraptik.p.rapidapi.com/user-posts"

        querystring = {"user_id": user_id, "count":"30", "max_cursor": str(cursor)}

        headers = {
            "X-RapidAPI-Key": config.Secret_Key(key_name='SCRAPTIK_RAPIDAPI').value,
            "X-RapidAPI-Host": "scraptik.p.rapidapi.com"
        }

        response = requests.request("GET", url, headers=headers, params=querystring)
    
        data = json.loads(response.text)
        
        if check_for_api_error(data=data):
            continue
        
        try:
            hasMore = data['has_more']
            if hasMore == 0:
                hasMore = False
        except:
            hasMore = False
        try:
            cursor = data['max_cursor']
        except:
            pass
        try:
            master_itemList.append(data['aweme_list'])
            time.sleep(.5)
        except:
            pass
        
    itemList = flatten_list(master_itemList)
    return itemList

def get_user_threshold(airtable_row_id: str):

    """Implement dynamic thershold. All users default to 0.99
    to start. Extract will check how many videos we scraped from harvest and 
    compare to the threshold. videos_scraped / videos_scraped_threshold. If this ratio is less
    than the threshold, we will descrease threshold and rerun"""
    df = airtable_utils.get_table_data()
    df = df.set_index('airtable_row_id')
    threshold = df.loc[airtable_row_id, 'videos_scraped_threshold']
    return threshold

def check_video_count(df: pd.DataFrame,
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
    videos_expected = df.iloc[0, 1]
    user_threshold = get_user_threshold(airtable_row_id=airtable_row_id)
    
    airtable_utils.update_database_cell(row_id=airtable_row_id,
                                        field='videos_scraped',
                                        value=int(videos_scraped))
    airtable_utils.update_database_cell(row_id=airtable_row_id,
                                        field='total_videos',
                                        value=int(videos_expected))
    
    if videos_scraped / videos_expected < user_threshold: # Scraper doesnt always work, set threshold to 95% or it will fail and rerun
        new_user_threshold = user_threshold - config.VIDEOS_SCRAPED_THRESHOLD_DECREASE
        airtable_utils.update_database_cell(row_id=airtable_row_id,
                                            field='videos_scraped_threshold',
                                            value=new_user_threshold)
        print(f'{user} did not meet the videos scraped threshold, retrying!: {videos_scraped} / {videos_expected}')
        assert 1 == 0
    
    
    if videos_scraped >= videos_expected:
        print(f'{videos_scraped} / {videos_expected} videos scraped')
        return True
     

@vidvault_utils.timeit(message='Harvest')
def run(user: str,
        date: datetime.datetime,
        airtable_row_id: str):
    user_id = get_user_uid(user)
    print(f'User ID: {user_id}')
    user_videos = get_user_videos(user_id=user_id)
    print(f'User Videos Scraped: {len(user_videos)}')
    video_stats = {"video_id": [], "user_video_count": []}
    
    for video in user_videos:
        video_stats['video_id'].append(video['aweme_id'])
        video_stats['user_video_count'].append(video['author']['aweme_count'])
    
    df = pd.DataFrame(data=video_stats)
    check_video_count(df,user,airtable_row_id)
    print("Video count checked")
    df.set_index('video_id', inplace=True)
    df.to_csv(config.ExtractPath(user=user, date=date).data_path_file)
    
    
if __name__ == '__main__':
    run(user='witchyasamother',
        date=datetime.datetime.now(),
        airtable_row_id='reclh4aYdI8Z18HNc')
    
    
    
        