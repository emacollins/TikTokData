import os
import glob
import pandas as pd
import gspread

LOAD_PATH = "/Users/ericcollins/TikTokData/load/tiktokdata.csv"

def run():
    extension = 'csv'
    all_filenames = [i for i in glob.glob('/Users/ericcollins/TikTokData/extract/*.{}'.format(extension))]
    #combine all files in the list
    combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames])
        #export to csv
    combined_csv.to_csv(LOAD_PATH, index=False, encoding='utf-8-sig')
    
    #Upload to google sheet
    gc = gspread.oauth(credentials_filename='/Users/ericcollins/TikTokData/googledrive/credentials.json')
    content = open(LOAD_PATH, 'r').read().encode('utf-8') 
    gc.import_csv('15eA_QYvRQbeCehSouCKEqgYHA8Q1ItSr4TmU1gArZ0g', content)
    
if __name__ == '__main__':
    run()