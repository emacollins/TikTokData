import harvest
import extract
import time


def run():
    
    harvest_start_time = time.time()
    harvest.run()
    harvest_end_time = time.time()
    harvest_elapsed_time = harvest_end_time - harvest_start_time
    print(harvest_elapsed_time)

    extract_start_time = time.time()
    extract.run()
    extract_end_time = time.time()
    extract_elapsed_time = extract_end_time - extract_start_time
    print(extract_elapsed_time)

if __name__ == '__main__':
    run()