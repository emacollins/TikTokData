from tiktokapipy.api import TikTokAPI
import pandas as pd
import constants

def sample_return_data():
    with TikTokAPI() as api:
        user = api.user('figapp')
        for video in user.videos.sorted_by(key=lambda vid: vid.create_time, reverse=False):
            for tags in video.tags:
                print(user)
                print('$$$$$$$$')
                print(video.stats)
                print('-----')
                print(tags)
                print('**********')


def get_user_videos(username: str) -> pd.DataFrame:
    """Takes in username and cleans relevant data for that user."""
    data_store_dict = {new_list: [] for new_list in constants.all_fields_needed.keys()}
    with TikTokAPI() as api:
        user = api.user(username)


def get_tags(tags_object) -> str:
    tags = []
    for tag in tags_object:
        tags.append(tag.title)
    return ';'.join(map(str, tags))

            
def get_user_video_data(username: str) -> pd.DataFrame:
    """Takes in username and cleans relevant data for that user."""
    data_store_dict = {new_list: [] for new_list in constants.all_fields_needed.keys()}
    with TikTokAPI() as api:
        user = api.user(username)
        for video in user.videos.sorted_by(key=lambda vid: vid.create_time, reverse=False):
            for field in constants.all_fields_needed.keys():
                if field in constants.user_fields.keys():
                    data_store_dict[field].append(getattr(user, constants.user_fields[field]))
                elif field in constants.video_fields.keys():
                    data_store_dict[field].append(getattr(video, constants.video_fields[field]))
                elif field in constants.video_stats_fields.keys():
                    data_store_dict[field].append(getattr(video.stats, constants.video_stats_fields[field]))
                elif field == 'hashtag':
                    data_store_dict[field].append(get_tags(video.tags))
    df = pd.DataFrame(data=data_store_dict)
    df = df.set_index('video_id')
    return df
    
        
if __name__ == '__main__':
    df = get_user_video_data('figapp')
    df.to_csv('test_video_data_figapp.csv', index=False)