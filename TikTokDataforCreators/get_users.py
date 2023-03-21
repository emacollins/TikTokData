import datetime
import airtable_utils
import aws_utils
import config
import pandas as pd
import pipeline

# From Harvest
def get_airtable_data():
    table = airtable_utils.get_table_data()
    df = airtable_utils.convert_to_dataframe(airtable_table=table)
    df.to_csv(config.UserSignUpPath().cached_user_table, index=False)
    

def run():
    date=datetime.datetime.now()
    get_airtable_data()
    bad_users = {'user': [],
                 'airtable_row_id': []}  #Used to collect the users that errored on data pull
    user_sign_up_directory = config.UserSignUpPath().cached_user_table
    bad_users_history = pd.read_csv(config.UserSignUpPath().bad_users)
    users = pd.read_csv(user_sign_up_directory)[['airtable_row_id', 'user', 'videos_uploaded']]
    for row in users.itertuples():
        if row.user in set(bad_users_history['user']):
            continue
        try:
            if not row.videos_uploaded:
                pipeline.run(user=row.user,
                             airtable_row_id=row.airtable_row_id,
                             date=date)
            else:
                continue
        except Exception as e:
            print(e)
            print(f'Pipeline failed on user {row.user}')
            bad_users['user'].append(row.user)
            bad_users['airtable_row_id'].append(row.airtable_row_id)
            bad_users_current_harvest = pd.DataFrame(data=bad_users)
            bad_users_final = pd.concat([bad_users_history, bad_users_current_harvest])
            bad_users_final.to_csv(config.UserSignUpPath().bad_users, index=False)
            airtable_utils.mark_video_upload_failed(row_id=row.airtable_row_id)
        
    print('Run complete')
        

if __name__ == '__main__':
    run()