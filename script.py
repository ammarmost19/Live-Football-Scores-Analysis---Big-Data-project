matches_path = "Source data/matches.csv"
teams_path = "Source data/teams.csv"
events_path = "Source data/events.csv"

import pandas as pd
import time

def send_teams(teams_path,sorted_df,i):
    df2 = pd.read_csv(teams_path)
    match_id_val = sorted_df.iloc[i]["match_id"]
    target_df2 = df2[df2["match_id"] == match_id_val]
    j = 0
    while j < len(target_df2):
        print(target_df2.iloc[j])
        j += 1
    print("################# game started!! wait 5 seconds #################")
    time.sleep(5)


def send_event(events_path,sorted_df, i):
    df3 = pd.read_csv(events_path)
    match_id_val = sorted_df.iloc[i]["match_id"]
    target_df3 = df3[df3["match_id"] == match_id_val]
    j = 0
    while j < len(target_df3):
        print(target_df3.iloc[j])
        j += 1
        print("################# wait 3 seconds #################")
        time.sleep(3)


def send_info(matches_path):
    df1 = pd.read_csv(matches_path)
    sorted_df = df1.sort_values(by = "match_id", ascending=True)
    i = 0
    while i < len(sorted_df):
        print(sorted_df.iloc[i])
        send_teams(teams_path,sorted_df,i)
        send_event(events_path,sorted_df, i)
        i += 1
        print("################# match ended, wait 10 seconds #################")
        time.sleep(10)


send_info(matches_path)
