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

TEST_USER = 'thephotoverse'
TEST_AIRTABLE_ROW = 'recb4iqk60sDmE4cu'

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
@timeit
def main(user_data: dict):
    
    user = user_data['user']
    airtable_row_id = user_data['airtable_row_id']
    date=datetime.datetime.now()
    
    harvest.run(user=user, 
                date=date)
    print(f'Harvest for {user} complete')
    
    extract.run(user=user, 
                date=date,
                airtable_row_id=airtable_row_id)
    print(f'Extract for {user} complete')
    try:
        load.run(user=user, 
                 date=date)
        print(f'Load for {user} complete')
    except Exception as e:
        print(e)
        print(f'Load of analytics data failed on {user}, proceeding to video download')
    
    
    save_video = download_videos.run(user=user, 
                                     date=date)
    print(f'Videos saved for {user} complete')
    
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

def run(user_data: dict):
    try:
        user = user_data['user']
        airtable_row_id = user_data['airtable_row_id']
        main(user_data=user_data)
        
    except Exception as e:
        print(e)
        print(f'Pipeline failed on user {user}')
        bad_users_history = pd.read_csv(config.UserSignUpPath().bad_users)
        bad_users_current_harvest = pd.DataFrame(data=[user_data])
        bad_users_final = pd.concat([bad_users_history, bad_users_current_harvest])
        bad_users_final.to_csv(config.UserSignUpPath().bad_users, index=False)
        #airtable_utils.mark_video_upload_failed(row_id=airtable_row_id)  # Old update command
        airtable_utils.update_database_cell(row_id=airtable_row_id,
                                            field='upload_failed',
                                            value="True")


if __name__ == '__main__':
    user_data = {'user': TEST_USER, 'airtable_row_id': TEST_AIRTABLE_ROW}
    run(user_data)
    
