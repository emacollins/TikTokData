import os
import glob
import pandas as pd
import gspread
import config
import boto3
from io import BytesIO

LOAD_PATH = config.LoadPath().data_path_file

def add_is_influencer_column(combined_csv: pd.DataFrame) -> pd.DataFrame:
    """adds is influencer column to final dataframe"""
    account_tracker = pd.read_csv(config.UserSignUpPath().cached_user_table)[['user', 'influencer']]
    df = combined_csv.merge(account_tracker, how='left', left_on='user_unique_id', right_on='user').drop(columns='user')
    df['day_over_day_change'] = df['video_play_count'].diff()
    return df
    

def run():
    
    # Initialize S3 client
    s3 = boto3.client('s3')

    # Bucket and key prefix of the CSV files you want to load
    bucket_name = config.BUCKET
    key_prefix = config.ExtractPath().data_path_s3_key

    # List all objects in the specified bucket and with the specified key prefix
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=key_prefix)

    # Create an empty list to hold the contents of all CSV files
    dfs = []
    # Loop through each object and load its contents into a pandas dataframe
    for obj in objects['Contents']:
        key = obj['Key']
        if key.endswith('.csv'):  # Only load CSV files
            s3_object = s3.get_object(Bucket=bucket_name, Key=key)
            df = pd.read_csv(BytesIO(s3_object['Body'].read()), header=0)
            dfs.append(df)

    # Concatenate all dataframes into a single dataframe
    combined_csv = pd.concat(dfs, axis=0, ignore_index=True)
    
    
    combined_csv = add_is_influencer_column(combined_csv=combined_csv)
    # export to csv
    combined_csv.to_csv(LOAD_PATH, index=False, encoding='utf-8-sig')
    
if __name__ == '__main__':
    run()