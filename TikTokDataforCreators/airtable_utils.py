import os
from re import U
from pyairtable import Table
import pandas as pd
from numpy import nan

"""
Example from Docs

table.all()
[ {"id": "rec5eR7IzKSAOBHCz", "fields": { ... }}]

table.create({"Foo": "Bar"})
{"id": "recwAcQdqwe21as", "fields": { "Foo": "Bar" }}]

table.update("recwAcQdqwe21as", {"Foo": "Foo"})
{"id": "recwAcQdqwe21as", "fields": { "Foo": "Foo" }}]

table.delete("recwAcQdqwe21as")

"""

AIRTABLE_API_KEY = 'keyDdCRa1BsB4FG4A'
BASE_ID = 'appTc74HsWp5MCjaQ'
TABLE_NAME = 'Paying Customer Info'

def get_table_data():
    
    table = Table(AIRTABLE_API_KEY, BASE_ID, TABLE_NAME)
    #df = pd.DataFrame(data=table.all()['fields']).to_csv('TEST-airtable.csv',index=False)
    return table.all()

def get_new_structure():
    
    new_structure = {'airtable_row_id': [],
                     'airtable_row_create_time': [],
                     'user': [],
                     'videos_uploaded': [],
                     'customer_email': [],
                     'hashtags': [],
                     'influencer': [],
                     'last_data_pull_date': [],
                     'last_video_pull_date': [],
                     'download_link': []}
    return new_structure

def cleaned_dictionary(row, structure):
    new_structure = structure.copy()
    new_structure['airtable_row_id'].append(row.get('id', nan))
    new_structure['airtable_row_create_time'].append(row.get('createdTime', nan))
    new_structure['user'].append(row.get('fields', {}).get('tiktok_username', nan))
    new_structure['videos_uploaded'].append(row.get('fields', {}).get('videos_uploaded', nan))
    new_structure['customer_email'].append(row.get('fields', {}).get('customer_email', nan))
    new_structure['hashtags'].append(nan)
    new_structure['influencer'].append(nan)
    new_structure['last_data_pull_date'].append(nan)
    new_structure['last_video_pull_date'].append(nan)
    new_structure['download_link'].append(row.get('fields', {}).get('download_link', nan))

    
    return new_structure

def convert_to_dataframe(airtable_table: list) -> pd.DataFrame:
    """Takes the raw output of the airtable query and makes tabular

    Args:
        airtable_table (list): This is the output of get_table()

    Returns:
        pd.DataFrame: Tabular dataset
    """
    new_structure = get_new_structure()
    for row in airtable_table:
        new_structure = cleaned_dictionary(row, new_structure)
    df = pd.DataFrame(data=new_structure)
    return df    

def update_download_link(row_id: str,
                         download_link: str):
    
    table = Table(AIRTABLE_API_KEY, BASE_ID, TABLE_NAME)
    try:
        table.update(row_id, {'download_link': download_link})
        return True
    except Exception as e:
        print(e)
        return False

def mark_user_videos_uploaded(row_id: str):
    table = Table(AIRTABLE_API_KEY, BASE_ID, TABLE_NAME)
    try:
        table.update(row_id, {"videos_uploaded": "True"})
        return True
    except Exception as e:
        print(e)
        return False
def mark_video_upload_failed(row_id: str):
    table = Table(AIRTABLE_API_KEY, BASE_ID, TABLE_NAME)
    try:
        table.update(row_id, {"upload_failed": "True"})
        return True
    except Exception as e:
        print(e)
        return False

def update_video_counts(row_id: str, videos_scraped: int, total_videos: int):
    table = Table(AIRTABLE_API_KEY, BASE_ID, TABLE_NAME)
    try:
        table.update(row_id, {"total_videos": total_videos})
        table.update(row_id, {"videos_scraped": videos_scraped})
        return True
    except Exception as e:
        print(e)
        return False


if __name__ == '__main__':
    table = get_table_data()
    df = convert_to_dataframe(airtable_table=table)
    df.to_csv('TEST_AIRTABLE_HARVEST.csv', index=False)
    #update_download_link('recKGvmEQKlfQ8600', 'https://vidvault-app.s3.amazonaws.com/customer-videos/tytheproductguy-2023-03-18.zip?AWSAccessKeyId=AKIAZDABZ7ECYCMNF46H&Signature=fBj7zH2cGDUa1fzswM2%2BHLEchzk%3D&Expires=1679734490')
    #mark_user_videos_uploaded('recKGvmEQKlfQ8600')