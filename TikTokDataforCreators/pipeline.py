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

TEST_USER = 'tylerandhistummy'
TEST_AIRTABLE_ROW = 'recFxfnl5fAbxcfVf'

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

    user = user_data['user'].lower()
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
    
    
    save_video = download_videos.run(user=user, 
                                     date=date)
    print(f'Videos saved for {user} complete')
    
    if check_if_this_is_pipeline_test(airtable_row_id):
        airtable_utils.update_database_cell(row_id=airtable_row_id,
                                            field='test_run',
                                            value="False")
        assert 1 == 0, f"Test run for {user} complete!"
        
    if save_video:
        try:
            url = aws_utils.create_presigned_url(bucket_name=config.BUCKET,
                                        object_name=config.ExtractPath(user=user,
                                                                        date=date).video_path_file_s3_key)
            airtable_utils.update_database_cell(row_id=airtable_row_id,
                                                field='download_link',
                                                value=url)

            airtable_utils.update_database_cell(row_id=airtable_row_id,
                                                field='videos_uploaded',
                                                value="True")
        except Exception as e:
            
            airtable_utils.update_database_cell(row_id=airtable_row_id,
                                                field='upload_failed',
                                                value="True")
            print(e)
            print(f'Could not get url and update airtable for {user}')
    elapsed_time = time.time() - start_time
    total_time = round(elapsed_time, 4)
    print(f'Pipeline for {user} | complete in {total_time:.4f} seconds')
def run(user_data: dict):
    try:
        user = user_data['user']
        user = user.lower()
        airtable_row_id = user_data['airtable_row_id']
        main(user_data=user_data)
        
    except Exception as e:
        print(e)
        print(f'Pipeline failed on user {user}')
        pass
        airtable_utils.update_database_cell(row_id=airtable_row_id,
                                            field='upload_failed',
                                            value="True")


if __name__ == '__main__':
    user_data = {'user': TEST_USER, 'airtable_row_id': TEST_AIRTABLE_ROW}
    run(user_data)
    