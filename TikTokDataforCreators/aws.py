import boto3
import shutil


# Let's use Amazon S3
s3 = boto3.resource('s3')


def upload_zip_files(user: str):
    """Takes a user's mp4 files, zips, and uploads to S3

    Args:
        user (str): _description_
    """
    zip_file_folder = '/Users/ericcollins/TikTokData/TikTokDataforCreators/videos/final_user_zip'
    harvest_videos = '/Users/ericcollins/TikTokData/TikTokDataforCreators/harvest/videos{user}'
    
    shutil.make_archive('/Users/ericcollins/TikTokData/TikTokDataforCreators/videos/final_user_zip', 'zip', )