import datetime
import config
from turtle import down
import harvest
import extract
import download_videos
import load
import time
import aws_utils
import airtable_utils
from functools import wraps
import time
import pandas as pd
import numpy as np
import math
import vidvault_utils

TEST_USER = 'https://www.tiktok.com/@figapp?l'
TEST_AIRTABLE_ROW = 'reclM3jB9bWcBQG2y'

def check_if_this_is_pipeline_test(airtable_row_id: str):
    table = airtable_utils.get_table_data()
    df = airtable_utils.convert_to_dataframe(airtable_table=table)
    df = df.set_index('airtable_row_id')
    is_test = df.loc[airtable_row_id, 'test_run']
    
    if is_test == 'True':
        return True
    else: return False

def check_if_threshold_set(airtable_row_id: str):
    table = airtable_utils.get_table_data()
    df = airtable_utils.convert_to_dataframe(airtable_table=table)
    df = df.set_index('airtable_row_id')
    threshold = df.loc[airtable_row_id, 'videos_scraped_threshold']
    try:
        threshold = float(threshold)
    except:
        return False
    
    if math.isnan(threshold):
        return False
    else: return True


def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f'Pipeline took {total_time:.4f} seconds')
        return result
    return timeit_wrapper

# TODO: have an input arg of user and then apply 

def main(user_data: dict):

    user_raw = user_data['user']
    user = vidvault_utils.clean_user(user=user_raw)
    airtable_row_id = user_data['airtable_row_id']
    date=datetime.datetime.now()
    start_time = time.time()
    print(f'Pipeline for {user} started!')
    
    # Airtable wont auto fill threshold, so need to do manually and default to 0.99
    threshold_check = check_if_threshold_set(airtable_row_id)
    if threshold_check:
        pass
    else:
        airtable_utils.update_database_cell(row_id=airtable_row_id,
                                                field='videos_scraped_threshold',
                                                value=0.99)
    
    harvest.run(user=user, 
                date=date)
    
    extract.run(user=user, 
                date=date,
                airtable_row_id=airtable_row_id)
    try:
        load.run(user=user, 
                 date=date)
        
    except Exception as e:
        print(e)
        print(f'Load of analytics data failed on {user}, proceeding to video download')
    
    airtable_utils.update_database_cell(row_id=airtable_row_id,
                                                field='scrape_completed',
                                                value="True")
    
def run(user_data: dict):
    try:
        user = user_data['user']
        airtable_row_id = user_data['airtable_row_id']
        main(user_data=user_data)
        
    except Exception as e:
        print(e)
        print(f'Scrape Pipeline failed on user {user}')
        pass
        airtable_utils.update_database_cell(row_id=airtable_row_id,
                                            field='upload_failed',
                                            value="True")

if __name__ == '__main__':
    user_data = {'user': TEST_USER, 'airtable_row_id': TEST_AIRTABLE_ROW}
    run(user_data)
    