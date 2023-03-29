import datetime
import airtable_utils
import aws_utils
import config
import pandas as pd
import pipeline
import multiprocessing
import time
from vidvault_utils import timeit

TEST = True

def get_airtable_data():
    table = airtable_utils.get_table_data()
    df = airtable_utils.convert_to_dataframe(airtable_table=table)
    df.to_csv(config.UserSignUpPath().cached_user_table, index=False)   

@timeit(message='******Entire Run')
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
    users = pd.read_csv(user_sign_up_directory)[['airtable_row_id', 'user', 'videos_uploaded', 'test_run', 'scrape_completed']]
    test_users = users.loc[(~users['user'].isin(bad_users_history['user'])) & (users['test_run'] == True)]
    new_users = users.loc[(~users['user'].isin(bad_users_history['user'])) & (users['videos_uploaded'] == False) & (users['scrape_completed'] == False)]
    users_to_download = pd.concat([test_users, new_users])
    users_to_download = users_to_download[['airtable_row_id', 'user']]
    users_to_download = users_to_download[['airtable_row_id', 'user']]
    try:
        users_to_download = users_to_download.sample(1)
        
    except Exception as e:
	    users_to_download = pd.DataFrame()
    
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
    
    