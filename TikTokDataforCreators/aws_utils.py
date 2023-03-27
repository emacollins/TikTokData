import boto3
import logging
from botocore.exceptions import ClientError


def create_presigned_url(bucket_name, object_name, expiration=3600*24*30):
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

def upload_to_s3(file_name, bucket, object_name=None):
    
    s3_client = boto3.client('s3')
    if object_name is None:
        object_name = file_name
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
        print(f'{file_name} uploaded to S3 bucket {bucket}.')
    except ClientError as e:
        logging.info(f'Error uploading {file_name} to S3 bucket {bucket}: {str(e)}')
        print(f'Error uploading {file_name} to S3 bucket {bucket}: {str(e)}')


if __name__ == '__main__':
    pass