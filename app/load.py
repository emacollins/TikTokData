import datetime
import pandas as pd
import config
import boto3
from io import BytesIO
    

def run(user:str, 
        date: datetime.datetime):
    # Initialize S3 client
    s3 = boto3.client('s3')

    # Bucket and key prefix of the CSV files you want to load
    bucket_name = config.BUCKET
    key_prefix = config.ExtractPath(user=user,
                                    date=date).data_path_s3_key

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
    
    # export to csv
    combined_csv.to_csv(config.LoadPath(user=user).data_path_file, 
                        index=False, 
                        encoding='utf-8-sig')
    
if __name__ == '__main__':
    run(user='tytheproductguy',
        date=datetime.datetime.now())