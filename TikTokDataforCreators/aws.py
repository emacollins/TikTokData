import datetime
import boto3
import shutil
import config
import logging
from botocore.exceptions import ClientError


# Let's use Amazon S3
s3 = boto3.resource('s3')

def upload_data_file():
    Bucket = 'vidvault-app'
    Key = f'analytics/tiktokdata.csv'
    s3.meta.client.upload_file(config.LoadPath().data_path_file, Bucket=Bucket, Key=Key, ExtraArgs=None, Callback=None, Config=None)  

def upload_zip_files(user: str,
                     date: datetime.datetime):
    """Takes a user's mp4 files, zips, and uploads to S3

    Args:
        user (str): _description_
    """
    datetime_format = '%Y-%m-%d'
    zip_file = config.ExtractPath(user=user,
                              date=date,
                              datetime_format=datetime_format).video_path_file + '.zip'  # Need to add extension here manually
    # Upload a file to an S3 object
    Bucket = 'vidvault-app'
    Key = f'customer-videos/{user}-{date.strftime(datetime_format)}.zip'
    s3.meta.client.upload_file(zip_file, Bucket=Bucket, Key=Key, ExtraArgs=None, Callback=None, Config=None)    
    presigned_url = create_presigned_url(bucket_name=Bucket,
                         object_name=Key)
    print(presigned_url)
    return presigned_url

def create_presigned_url(bucket_name, object_name, expiration=3600*24*7):
    """Generate a presigned URL to share an S3 object

    :param bucket_name: string
    :param object_name: string
    :param expiration: Time in seconds for the presigned URL to remain valid
    :return: Presigned URL as string. If error, returns None.
    """

    # Generate a presigned URL for the S3 object
    s3_client = boto3.client('s3')
    try:
        response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name,
                                                            'Key': object_name},
                                                    ExpiresIn=expiration)
    except ClientError as e:
        logging.error(e)
        return None

    # The response contains the presigned URL
    return response
if __name__ == '__main__':
    #upload_zip_files(user='tytheproductguy',
                    # date=datetime.datetime.now())
    upload_data_file()