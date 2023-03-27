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
import logging
import utils
import logging

logger = logging.getLogger('run_log.' + __name__)


TEST_USER = 'thephotoverse'
TEST_AIRTABLE_ROW = 'recb4iqk60sDmE4cu'

def check_if_this_is_pipeline_test(airtable_row_id: str):
    table = airtable_utils.get_table_data()
    df = airtable_utils.convert_to_dataframe(airtable_table=table)
    df = df.set_index('airtable_row_id')
    is_test = df.loc[airtable_row_id, 'test_run']
    
    if is_test:
        return True
    else: return False

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
    logger.info(f'Harvest for {user} succesful - {utils.get_log_timestamp()}')
    print(f'Harvest for {user} complete')
    
    extract.run(user=user, 
                date=date,
                airtable_row_id=airtable_row_id)
    logger.info(f'Extract for {user} succesful - {utils.get_log_timestamp()}')
    print(f'Extract for {user} complete')
    
    try:
        load.run(user=user, 
                 date=date)
        print(f'Load for {user} complete')
        logger.info(f'HLoad for {user} succesful - {utils.get_log_timestamp()}')
    except Exception as e:
        logger.info(f'Load of analytics data failed on {user}, proceeding to video download {str(e)}- {utils.get_log_timestamp()}')
        print(f'Load of analytics data failed on {user}, proceeding to video download')
    
    
    save_video = download_videos.run(user=user, 
                                     date=date)
    logger.info(f'Save Video for {user} succesful - {utils.get_log_timestamp()}')
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
            logger.info(f'URL Link Update for {user} succesful - {utils.get_log_timestamp()}')
        except Exception as e:
            
            airtable_utils.update_database_cell(row_id=airtable_row_id,
                                                field='upload_failed',
                                                value="True")
            logger.info(f'Could not get url and update airtable for {user} {str(e)}- {utils.get_log_timestamp()}')
            print(f'Could not get url and update airtable for {user}')
    
    

def run(user_data: dict):
    try:
        user = user_data['user']
        airtable_row_id = user_data['airtable_row_id']
        main(user_data=user_data)
        logger.info(f'Pipeline for {user} succesful - {utils.get_log_timestamp()}')
    except Exception as e:
        logger.info(f'Pipeline for {user} shutdown - {utils.get_log_timestamp()}')
        airtable_utils.update_database_cell(row_id=airtable_row_id,
                                            field='upload_failed',
                                            value="True")


if __name__ == '__main__':
    user_data = {'user': TEST_USER, 'airtable_row_id': TEST_AIRTABLE_ROW}
    run(user_data)
    
