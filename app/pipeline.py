import math
import time
import datetime
import config
import download_videos
import aws_utils
import airtable_utils
import vidvault_utils
import harvest_v2

TEST_USER = 'tytheproductguy'
TEST_AIRTABLE_ROW = 'recKGvmEQKlfQ8600'

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

@vidvault_utils.timeit('Pipeline')
def main(user_data: dict):

    user_raw = user_data['user']
    user = vidvault_utils.clean_user(user=user_raw)
    airtable_row_id = user_data['airtable_row_id']
    date=datetime.datetime.now()
    start_time = time.time()
    print(f'Pipeline for {user} started!')
    
    harvest_v2.run(user=user, 
                date=date,
                airtable_row_id=airtable_row_id)
    
    
    airtable_utils.update_database_cell(row_id=airtable_row_id,
                                                field='scrape_completed',
                                                value="True")


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
        airtable_row_id = user_data['airtable_row_id']
        
        # Turn on in_progress field in database
        airtable_utils.update_database_cell(row_id=airtable_row_id,
                                                field='in_progress',
                                                value="True")
        main(user_data=user_data) 
        
        airtable_utils.update_database_cell(row_id=airtable_row_id,
                                                field='in_progress',
                                                value="False")
        
    except Exception as e:
        print(e)
        print(f'Pipeline failed on user {user}')
        pass
        airtable_utils.update_database_cell(row_id=airtable_row_id,
                                            field='upload_failed',
                                            value="True")
        airtable_utils.update_database_cell(row_id=airtable_row_id,
                                                field='in_progress',
                                                value="False")


if __name__ == '__main__':
    user_data = {'user': TEST_USER, 'airtable_row_id': TEST_AIRTABLE_ROW}
    run(user_data)
    