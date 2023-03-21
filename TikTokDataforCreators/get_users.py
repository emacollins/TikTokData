import airtable_utils
import aws_utils
import config
import pandas as pd

# From Harvest
def get_airtable_data():
    table = airtable_utils.get_table_data()
    df = airtable_utils.convert_to_dataframe(airtable_table=table)
    df.to_csv(config.UserSignUpPath().cached_user_table, index=False)
    

def run():
    get_airtable_data()
    bad_users = {'username': [],
                 'airtable_row_id': []}  #Used to collect the users that errored on data pull
    user_sign_up_directory = config.UserSignUpPath().cached_user_table
    users = pd.read_csv(user_sign_up_directory)[['airtable_row_id', 'user', 'videos_uploaded']]
    for row in users.itertuples():
        try:
            if not row.videos_uploaded:
                print(row.user)
            else:
                pass
        except Exception as e:
            bad_users['username'].append(row.user)
            bad_users['airtable_row_id'].append(row.airtable_row_id)
            bad_users_history = pd.read_csv(config.UserSignUpPath().bad_users)
            bad_users_current_harvest = pd.DataFrame(data=bad_users)
            bad_users = pd.concat([bad_users_history, bad_users_current_harvest])
            bad_users.to_csv(config.UserSignUpPath().bad_users, index=False)
        
        
        

if __name__ == '__main__':
    run()