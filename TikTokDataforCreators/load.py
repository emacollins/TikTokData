import os
import glob
import pandas as pd
import gspread

LOAD_PATH = "/Users/ericcollins/TikTokData/load/tiktokdata.csv"

def add_is_influencer_column(combined_csv: pd.DataFrame) -> pd.DataFrame:
    """adds is influencer column to final dataframe"""
    account_tracker = pd.read_csv('TikTokDataforCreators/tiktok_accounts_to_track.csv')[['user', 'influencer']]
    df = combined_csv.merge(account_tracker, how='left', left_on='user_unique_id', right_on='user').drop(columns='user')
    df['day_over_day_change'] = df['video_play_count'].diff()
    return df
    

def run():
    extension = 'csv'
    all_filenames = [i for i in glob.glob('/Users/ericcollins/TikTokData/TikTokDataforCreators/extract/*.{}'.format(extension))]
    # combine all files in the list
    combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames])
    combined_csv = add_is_influencer_column(combined_csv=combined_csv)
    # export to csv
    combined_csv.to_csv(LOAD_PATH, index=False, encoding='utf-8-sig')
    
    #Upload to google sheet
    gc = gspread.oauth(credentials_filename='/Users/ericcollins/TikTokData/TikTokDataforCreators/googledrive/credentials.json')
    content = open(LOAD_PATH, 'r').read().encode('utf-8') 
    gc.import_csv('15eA_QYvRQbeCehSouCKEqgYHA8Q1ItSr4TmU1gArZ0g', content)
    
if __name__ == '__main__':
    run()