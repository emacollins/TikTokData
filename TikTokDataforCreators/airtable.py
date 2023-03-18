import os
from pyairtable import Table
import pandas as pd

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

def get_table():
    api_key = 'keyDdCRa1BsB4FG4A'
    base_id = 'appTc74HsWp5MCjaQ'
    table_name = 'Paying Customer Info'
    table = Table(api_key, base_id, table_name)
    df = pd.DataFrame(data=table.all()['fields']).to_csv('TEST-airtable.csv',index=False)
    print(table.all())
    

if __name__ == '__main__':
    get_table()