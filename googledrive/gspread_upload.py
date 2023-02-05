import gspread
import os
gc = gspread.oauth(credentials_filename='/Users/ericcollins/TikTokData/googledrive/credentials.json')
content = open('/Users/ericcollins/TikTokData/load/tiktokdata.csv', 'r').read().encode('utf-8') 
gc.import_csv('15eA_QYvRQbeCehSouCKEqgYHA8Q1ItSr4TmU1gArZ0g', content)