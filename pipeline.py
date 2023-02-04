import harvest
import extract
import load
import time


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
        load_start_time = time.time()
        load_task = load.run()
        load_end_time = time.time()
        load_elapsed_time = load_end_time - load_start_time
        print(f'Load time: {load_elapsed_time} s')
if __name__ == '__main__':
    run()