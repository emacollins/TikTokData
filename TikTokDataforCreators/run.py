import datetime
import airtable_utils
import aws_utils
import config
import pandas as pd
import pipeline
import multiprocessing
from functools import wraps
import time

TEST = True

def get_airtable_data():
    table = airtable_utils.get_table_data()
    df = airtable_utils.convert_to_dataframe(airtable_table=table)
    df.to_csv(config.UserSignUpPath().cached_user_table, index=False)

def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(f'Run complete in {total_time:.4f} seconds')
        return result
    return timeit_wrapper    

@timeit
def run():
    """This runs the entire process. It first checks which users have 
    not yet been sent their videos (from Airtable database), and then 
    groups those users into a multiprocessing pool so each pipeline is 
    run concurrently
    """
    # Get customer database and clean
    get_airtable_data()
    user_sign_up_directory = config.UserSignUpPath().cached_user_table
    bad_users_history = pd.read_csv(config.UserSignUpPath().bad_users)
    users = pd.read_csv(user_sign_up_directory)[['airtable_row_id', 'user', 'videos_uploaded', 'test_run']]
    test_users = users.loc[(~users['user'].isin(bad_users_history['user'])) & (users['test_run'] == True)]
    new_users = users.loc[(~users['user'].isin(bad_users_history['user'])) & (users['videos_uploaded'] == False)]
    users_to_download = pd.concat([test_users, new_users])
    users_to_download = users_to_download[['airtable_row_id', 'user']]
    users_to_download = users_to_download[['airtable_row_id', 'user']]
    users_to_download = users_to_download.sample(1)
    
    user_data = users_to_download.to_dict('records')
    try:
        with multiprocessing.Pool(processes=min([len(users_to_download), 8])) as pool:
            pool.map(pipeline.run, user_data)
    except Exception as e:
        print(e)
        pass            

if __name__ == '__main__':
    
    while True:
        run()
        time.sleep(10)
    
    