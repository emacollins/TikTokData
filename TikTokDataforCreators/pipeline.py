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

TEST_USER = 'thephotoverse'
TEST_AIRTABLE_ROW = 'recb4iqk60sDmE4cu'

def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f'Function {func.__name__} took {total_time:.4f} seconds')
        return result
    return timeit_wrapper

# TODO: have an input arg of user and then apply 
@timeit
def run(user: str,
        airtable_row_id: str,
        date: datetime.datetime):
    
    harvest.run(user=user, 
                date=date)
    print(f'Harvest for {user} complete')
    
    extract.run(user=user, 
                date=date)
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
            airtable_utils.mark_user_videos_uploaded(row_id=airtable_row_id)
            airtable_utils.update_download_link(row_id=airtable_row_id, 
                                                download_link=url)
        except Exception as e:
            airtable_utils.mark_video_upload_failed(row_id=airtable_row_id)
            print(e)
            print(f'Could not get url and update airtable for {user}')

    

if __name__ == '__main__':
    run(user=TEST_USER,
        airtable_row_id=TEST_AIRTABLE_ROW,
        date=datetime.datetime.now())
