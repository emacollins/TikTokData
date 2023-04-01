import config
import boto3
import logging
from botocore.exceptions import ClientError
import datetime


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
    pass

import boto3
import os
import zipfile
from vidvault_utils import timeit

@timeit(message='Upload .zip to AWS')
def upload_zip_extract_file_to_s3(zip_file_path: str, user: str, date: datetime.datetime):
    # create an S3 client
    s3_client = boto3.client('s3')

    s3_bucket = config.BUCKET
    s3_key_name = config.ExtractPath(user=user, date=date).video_path_file_s3_key
    # create a new multipart upload
    response = s3_client.create_multipart_upload(Bucket=s3_bucket, Key=s3_key_name)

    # get the upload ID from the response
    upload_id = response['UploadId']

    # set the part size (in bytes)
    part_size = 5 * 1024 * 1024

    # open the local file in binary mode
    with open(zip_file_path, 'rb') as f:
        # initialize the part number and upload parts list
        part_number = 1
        upload_parts = []

        # create a zipfile object from the local file
        zip_file = zipfile.ZipFile(f)

        # get a list of all files in the zip file
        zip_file_list = zip_file.namelist()

        # upload each file in the zip file in chunks
        for file_name in zip_file_list:
            # open the file in the zip file in binary mode
            zip_file_data = zip_file.open(file_name, 'r')

            # read the file in chunks and upload each part
            while True:
                # read the next part
                data = zip_file_data.read(part_size)

                # break if we've reached the end of the file
                if not data:
                    break

                # upload the part
                response = s3_client.upload_part(
                    Bucket=s3_bucket,
                    Key=s3_key_name,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=data
                )

                # add the response to the upload parts list
                upload_parts.append({'PartNumber': part_number, 'ETag': response['ETag']})

                # increment the part number
                part_number += 1

    # complete the multipart upload
    s3_client.complete_multipart_upload(
        Bucket=s3_bucket,
        Key=s3_key_name,
        UploadId=upload_id,
        MultipartUpload={'Parts': upload_parts}
    )


