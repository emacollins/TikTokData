import itertools

# The keys are the column names in the output file,
# the values are the attribute name in the API return object
user_fields = {'user_unique_id': 'unique_id'}
video_fields = {'video_id': 'id', 
                'video_create_time': 'create_time'}
video_stats_fields = {'video_digg_count': 'digg_count', 
                      'video_share_count': 'share_count', 
                      'video_comment_count': 'comment_count', 
                      'video_play_count': 'play_count',}
tags_fields = {'hashtag': 'title'}
all_fields_needed = {**user_fields, **video_fields, **video_stats_fields, **tags_fields}
