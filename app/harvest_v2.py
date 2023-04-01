import datetime
import pandas as pd
import requests
import json
import airtable_utils
import config
import vidvault_utils

def flatten_list(l):
    return [item for sublist in l for item in sublist]

def get_user_sec_uid(user: str):

    url = "https://scraptik.p.rapidapi.com/web/get-user"

    querystring = {"username":user}

    headers = {
        "X-RapidAPI-Key": config.Secret_Key(key_name='SCRAPTIK_RAPIDAPI').value,
        "X-RapidAPI-Host": "scraptik.p.rapidapi.com"
    }

    response = requests.request("GET", url, headers=headers, params=querystring)

    data = json.loads(response.text)
    return data['userInfo']['user']['secUid']

def get_user_videos(user_secUid: str):
    hasMore = True
    cursor = "0"
    master_itemList = []
    while hasMore:
    
    
        secUid = user_secUid
        

        url = "https://scraptik.p.rapidapi.com/web/user-posts"

        querystring = {"secUid": secUid,"count":"30","cursor":cursor}

        headers = {
            "X-RapidAPI-Key": "2eeb3e0118msh858f7a2fd2695cbp1a0abdjsn212f30c29918",
            "X-RapidAPI-Host": "scraptik.p.rapidapi.com"
        }

        response = requests.request("GET", url, headers=headers, params=querystring)
    
        data = json.loads(response.text)
        try:
            hasMore = data['hasMore']
        except:
            hasMore = False
        try:
            cursor = data['cursor']
        except:
            pass
        try:
            master_itemList.append(data['itemList'])
        except:
            pass
        
    itemList = flatten_list(master_itemList)
    return itemList

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
    
    
    if videos_scraped == videos_expected:
        return True

    else:
        print(f'{user} did not get all videos. {videos_scraped} / {videos_expected} scraped, but threshold met.')
        return True
     

@vidvault_utils.timeit(message='Harvest')
def run(user: str,
        date: datetime.datetime,
        airtable_row_id: str):
    user_secUid = get_user_sec_uid(user)
    user_videos = get_user_videos(user_secUid=user_secUid)
    video_stats = {"video_id": [], "user_video_count": []}
    
    for video in user_videos:
        video_stats['video_id'].append(video['id'])
        video_stats['user_video_count'].append(video['authorStats']['videoCount'])
    
    df = pd.DataFrame(data=video_stats)
    check_video_count(df,user,airtable_row_id)
    df.set_index('video_id', inplace=True)
    df.to_csv(config.ExtractPath(user=user, date=date).data_path_file)
    
    
if __name__ == '__main__':
    run(user='kara_in_florida',
        date=datetime.datetime.now(),
        airtable_row_id='recx3A6RM6knuCFU0')
    
    
    
        