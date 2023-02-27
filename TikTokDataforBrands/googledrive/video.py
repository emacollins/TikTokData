import os
from googleapiclient.http import MediaFileUpload
from Google import Create_Service

CLIENT_SECRET_FILE = '/Users/ericcollins/TikTokData/googledrive/credentials.json'
API_NAME = 'drive'
API_VERSION = 'v3'
SCOPES = ['https://www.googleapis.com/auth/drive']

service = Create_Service(CLIENT_SECRET_FILE, API_NAME, API_VERSION, SCOPES)

def export_csv_file(file_path: str, parents: list=None):
    if not os.path.exists(file_path):
        print(f'{file_path} not found')
        return
    try:
        file_metadata = {
            'name': os.path.basename(file_path), 
            'mimeType': 'applications/vnd.google.apps.spreadsheet',
            'parent': parents
        }
        
        media = MediaFileUpload(filename=file_path, mimetype=None)
        
        response = service.files().create(
            media_body=media,
            body=file_metadata
        ).execute()
        
        print(response)
        return response
    
    except Exception as e:
        print(e)
        return

csv_files = os.listdir('/Users/ericcollins/TikTokData/load')

for csv_file in csv_files:
    #export_csv_file(os.path.join('/Users/ericcollins/TikTokData/load', csv_file))
    export_csv_file(os.path.join('/Users/ericcollins/TikTokData/load', csv_file), parents=['1-GnUdyjg9-WbGEq-pDu0Af4VMVguhq4I'])