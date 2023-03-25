import airtable_utils
import config
import pandas as pd
import pipeline
from multiprocessing import get_context
from functools import wraps
import time



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
        print(f'Get all users took {total_time:.4f} seconds')
        return result
    return timeit_wrapper    

@timeit
def run():
    get_airtable_data()
    user_sign_up_directory = config.UserSignUpPath().cached_user_table
    bad_users_history = pd.read_csv(config.UserSignUpPath().bad_users)
    users = pd.read_csv(user_sign_up_directory)[['airtable_row_id', 'user', 'videos_uploaded']]
    users_to_download = users.loc[(~users['user'].isin(bad_users_history['user'])) & (users['videos_uploaded'] == False)]
    users_to_download = users_to_download[['airtable_row_id', 'user']]
    user_data = users_to_download.to_dict('records')
    try:
        with get_context("spawn").Pool(processes=min([len(users_to_download), 8])) as pool:
            pool.map(pipeline.run, user_data)
        print('Run complete')
    except Exception as e:
        print(e)
        pass            

if __name__ == '__main__':
    run()