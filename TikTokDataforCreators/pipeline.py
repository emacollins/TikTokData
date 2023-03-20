import datetime
from turtle import down
import harvest
import extract
import download_videos
import load
import time

# TODO: have an input arg of user and then apply 
def run():
    
    harvest_start_time = time.time()
    harvest_task = harvest.run()
    harvest_end_time = time.time()
    harvest_elapsed_time = harvest_end_time - harvest_start_time
    print(f'Harvest time: {harvest_elapsed_time} s')

    extract_start_time = time.time()
    extract_task = extract.run()
    extract_end_time = time.time()
    extract_elapsed_time = extract_end_time - extract_start_time
    print(f'Extract time: {extract_elapsed_time} s')

    if extract_task:
        # Load into final file
        load_start_time = time.time()
        load_task = load.run()
        load_end_time = time.time()
        load_elapsed_time = load_end_time - load_start_time
        print(f'Load time: {load_elapsed_time} s')
        
    # Save videos
    save_video_start_time = time.time()
    save_video = download_videos.run()
    save_video_end_time = time.time()
    save_video_elapsed_time = save_video_end_time - save_video_start_time
    print(f'Saving videos took {save_video_elapsed_time} s')
    
if __name__ == '__main__':
    run()