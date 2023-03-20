import boto3
import zipfile
import datetime
import os

import config

s3 = boto3.client('s3')

def run(user: str,
        date: datetime.datetime):
    # The S3 bucket containing the .mp4 files
    bucket = config.BUCKET

    # The S3 folder containing the .mp4 files
    source_folder = config.HarvestPath(user=user,
                                       date=date).video_path_s3_key

    # The S3 folder to which the .zip file will be saved
    destination_folder = config.ExtractPath(user=user).video_path_s3_key

    # List all .mp4 files in the source folder
    response = s3.list_objects_v2(Bucket=bucket, Prefix=source_folder)
    mp4_files = [content['Key'] for content in response['Contents'] if content['Key'].endswith('.mp4')]
    
    date_string = date.strftime('%m-%d-%Y')
    zip_file_name = f'downloaded_videos_{date_string}.zip'
    # Zip the .mp4 files
    with zipfile.ZipFile(zip_file_name, 'w') as zip_file:
        for file in mp4_files:
            # Download the file from S3
            s3.download_file(bucket, file, file.split('/')[-1])
            # Add the file to the zip archive
            zip_file.write(file.split('/')[-1])

    # Upload the .zip file to S3
    with open(zip_file_name, 'rb') as data:
        s3.upload_fileobj(data, bucket, destination_folder + '/' + zip_file_name)
        
    # Delete the local .zip and .mp4 files
    os.remove(zip_file_name)
    for file in mp4_files:
        os.remove(file.split('/')[-1])
    
if __name__ == '__main__':
    run(user='tytheproductguy',
        date=datetime.datetime.now())