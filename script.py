matches_path = "Source data/matches.csv"
teams_path = "Source data/teams.csv"
events_path = "Source data/events.csv"

#---- part2 Run code at regular intervals
# import schedule # pip install schedule
# import time

# def my_task():
#     print("Running scheduled job...")
# schedule.every(5).seconds.do(my_task)
# while True:
#     schedule.run_pending()
#     time.sleep(1)
def send_teams(teams_path,sorted_df,i):
    df2 = pd.read_csv(teams_path)
    match_id_val = sorted_df.iloc[i]["match_id"]
    target_df2 = df2[df2["match_id"] == match_id_val]
    j = 0
    while j < len(target_df2):
        print(target_df2.iloc[j])
        j += 1


import pandas as pd
def send_info(matches_path, teams_path):
    df1 = pd.read_csv(matches_path)
    sorted_df = df1.sort_values(by = "match_id", ascending=True)
    i = 0
    while i <= 0: #when you start real impleamet don't forget to remove the "="
        print(sorted_df.iloc[i])
        send_teams(teams_path,sorted_df,i)
        i += 1



send_info(matches_path,teams_path)
