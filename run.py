import pandas as pd
import video_data_pull as vdp


def get_accounts(filename) -> list:
    df = pd.read_csv(filename)
    accounts = list(df['user'])
    return accounts

    

def run():
    accounts = get_accounts("tiktok_accounts_to_track.csv")
    