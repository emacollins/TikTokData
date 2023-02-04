import os
import glob
import pandas as pd

def run():
    extension = 'csv'
    all_filenames = [i for i in glob.glob('/Users/ericcollins/TikTokData/extract/*.{}'.format(extension))]
    #combine all files in the list
    combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames])
        #export to csv
    combined_csv.to_csv( "/Users/ericcollins/TikTokData/load/tiktokdata.csv", index=False, encoding='utf-8-sig')
    
if __name__ == '__main__':
    run()