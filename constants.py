import itertools

user_fields_needed = ['unique_id']
video_fields_needed = ['id', 'stats.digg_count', 
                       'stats.share_count', 
                       'stats.comment_count', 
                       'stats.play_count'
                       'create_time']
tags_fields_needed = ['title']
all_fields_needed = itertools.chain(user_fields_needed, 
                             video_fields_needed, 
                             tags_fields_needed)