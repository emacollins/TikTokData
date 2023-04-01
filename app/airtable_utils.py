from re import U
from pyairtable import Table
import pandas as pd
from numpy import nan
import config

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


AIRTABLE_API_KEY = config.Secret_Key(key_name='AIRTABLE_API_KEY').value
BASE_ID = config.Secret_Key(key_name='BASE_ID').value
TABLE_NAME = 'Paying Customer Info'

def get_table_data() -> pd.DataFrame:
    
    table = Table(AIRTABLE_API_KEY, BASE_ID, TABLE_NAME)
    df = convert_to_dataframe(airtable_table=table.all())
    return df

def get_new_structure():
    
    new_structure = {'airtable_row_id': [],
                     'airtable_row_create_time': [],
                     'user': [],
                     'videos_uploaded': [],
                     'customer_email': [],
                     'videos_scraped_threshold': [],
                     'download_link': [],
                     'test_run': [],
                     'scrape_completed': [],
                     'in_progress': []}
    return new_structure

def cleaned_dictionary(row, structure):
    new_structure = structure.copy()
    new_structure['airtable_row_id'].append(row.get('id', nan))
    new_structure['airtable_row_create_time'].append(row.get('createdTime', nan))
    new_structure['user'].append(row.get('fields', {}).get('tiktok_username', nan))
    new_structure['videos_uploaded'].append(row.get('fields', {}).get('videos_uploaded', nan))
    new_structure['customer_email'].append(row.get('fields', {}).get('customer_email', nan))
    new_structure['videos_scraped_threshold'].append(row.get('fields', {}).get('videos_scraped_threshold', nan))
    new_structure['download_link'].append(row.get('fields', {}).get('download_link', nan))
    new_structure['test_run'].append(row.get('fields', {}).get('test_run', nan))
    new_structure['scrape_completed'].append(row.get('fields', {}).get('scrape_completed', nan))
    new_structure['in_progress'].append(row.get('fields', {}).get('in_progress', nan))
    return new_structure

def convert_to_dataframe(airtable_table: list) -> pd.DataFrame:
    """Takes the raw output of the airtable query and makes tabular

    *** TREAT ALL FIELDS
    Args:
        airtable_table (list): This is the output of get_table()

    Returns:
        pd.DataFrame: Tabular dataset,  *** TREAT ALL FIELDS AS STRINGS
    """
    new_structure = get_new_structure()
    for row in airtable_table:
        new_structure = cleaned_dictionary(row, new_structure)
    df = pd.DataFrame(data=new_structure)
    return df    


def update_database_cell(row_id: str,
                          field: str,
                          value: str):
    """Updates any cell in our base in airtable with a value"""
    table = Table(AIRTABLE_API_KEY, BASE_ID, TABLE_NAME)
    try:
        table.update(row_id, {field: value})
        return True
    except Exception as e:
        print(e)
        return False



if __name__ == '__main__':
    df = get_table_data()
    df.to_csv('TEST_AIRTABLE_HARVEST.csv', index=False)
    