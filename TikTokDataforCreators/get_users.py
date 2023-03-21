
# From Harvest
def get_airtable_data():
    table = airtable.get_table_data()
    df = airtable.convert_to_dataframe(airtable_table=table)
    df.to_csv(config.UserSignUpPath().cached_user_table, index=False)
    
if not saved_user_data:
            bad_users['username'].append(user)
            bad_users_history = pd.read_csv(config.UserSignUpPath().bad_users)
            bad_users_current_harvest = pd.DataFrame(data=bad_users)
            bad_users = pd.concat([bad_users_history, bad_users_current_harvest])
            bad_users.to_csv(config.UserSignUpPath().bad_users, index=False)

user_sign_up_directory = config.UserSignUpPath().cached_user_table
users = pd.read_csv(user_sign_up_directory)['user']
bad_users = {'username': []}  #Used to collect the users that errored on data pull

# From extract

# Drops users that errored in the harvest step
ACCOUNT_LIST = pd.read_csv(config.UserSignUpPath().cached_user_table, index_col='user')
BAD_USERS = pd.read_csv(config.UserSignUpPath().bad_users)
ACCOUNT_LIST = ACCOUNT_LIST.drop(BAD_USERS['username'])