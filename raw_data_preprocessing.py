import warnings
warnings.filterwarnings('ignore')
import pandas as pd
pd.set_option('display.max_columns', None)

years = range(2016, 2024)
base_filepath = "raw_data"
df_cols = ["playerId", "season", "name", "position", "situation", "games_played", "icetime", "shifts",  #desired columns from the raw data to filter in import_data func (function)
           "I_F_points", "I_F_goals", "onIce_fenwickPercentage", "I_F_xOnGoal", "I_F_xGoals", 
           "I_F_xRebounds", "I_F_xPlayContinuedInZone","I_F_shotsOnGoal", "I_F_takeaways", 
           "I_F_giveaways", "I_F_lowDangerShots", "I_F_mediumDangerShots", "I_F_highDangerShots", 
           "I_F_lowDangerxGoals", "I_F_mediumDangerxGoals", "I_F_highDangerxGoals", "I_F_reboundxGoals", 
           "I_F_xGoals_with_earned_rebounds", "I_F_oZoneShiftStarts", "OnIce_F_xOnGoal", "OnIce_F_xGoals", 
           "OnIce_F_shotsOnGoal", "OnIce_F_lowDangerShots", "OnIce_F_mediumDangerShots", 
           "OnIce_F_highDangerShots", "OnIce_F_lowDangerxGoals", "OnIce_F_mediumDangerxGoals", 
           "OnIce_F_highDangerxGoals"]
keep_cols_lst = ["playerId", "I_F_shotsOnGoal", "icetime", "onIce_fenwickPercentage",   #columns to keep in 5on5 and 5on4 dfs (dataframes) in preprocess_data func
                 "OnIce_F_shotsOnGoal"]
cols_5n5_dict = {"playerId" : "playerId", "onIce_fenwickPercentage" : "5n5_fenwick%",   #columns to rename for 5on5 dfs in preprocess_data func
                 "OnIce_F_shotsOnGoal" : "5n5_line_sog"}
cols_5n4_dict = {"playerId" : "playerId", "I_F_shotsOnGoal" : "5n4_sog", "icetime" : "5n4_icetime", #columns to rename for 5on4 dfs in preprocess_data func
                 "onIce_fenwickPercentage" : "5n4_fenwick%", "OnIce_F_shotsOnGoal" : "5n4_line_sog"}
to_drop = ['situation', 'season', 'I_F_points'] #columns to drop in final_preprocessing func
to_rename = {"I_F_xOnGoal": "xOnGoal", "I_F_xGoals": "xGoals", "I_F_xRebounds": "xRebounds",    #dict of renamed columns for final_preprocessing func
             "I_F_xPlayContinuedInZone": "xPlayCont", "I_F_shotsOnGoal": "sog", "I_F_goals": "goals",
             "I_F_takeaways": "takeaways", "I_F_giveaways": "giveaways", "I_F_lowDangerShots": "lowDangShots",
             "I_F_mediumDangerShots": "medDangShots", "I_F_highDangerShots": "highDangShots",
             "I_F_lowDangerxGoals": "lowDang_xGoals", "I_F_mediumDangerxGoals": "medDang_xGoals", 
             "I_F_highDangerxGoals": "highDang_xGoals", "I_F_reboundxGoals": "rebound_xGoals",
             "I_F_xGoals_with_earned_rebounds": "xGoalsWithEarnedRebounds", "I_F_oZoneShiftStarts": "ozs%",
             "OnIce_F_xOnGoal": "line_xOnGoal", "OnIce_F_xGoals": "line_xGoals", "OnIce_F_shotsOnGoal": "line_sog",
             "OnIce_F_lowDangerShots": "line_lowDangShots", "OnIce_F_mediumDangerShots": "line_medDangShots",
             "OnIce_F_highDangerShots": "line_highDangShots", "OnIce_F_lowDangerxGoals": "line_lowDang_xGoals", 
             "OnIce_F_mediumDangerxGoals": "line_medDang_xGoals", "OnIce_F_highDangerxGoals": "line_highDang_xGoals",
             "onIce_fenwickPercentage": "fenwick%", "position": "isForward"}
cols_reindexed = ['playerId', 'name', 'isForward', 'games_played', 'atoi', 'goals', 'assists', 'sog',   #list of columns in desired order for final_preprocessing func
                  's%', 'takeaways', 'giveaways', 'shifts', 'ozs%', 'fenwick%', 'xOnGoal', 'xGoals', 
                  'xRebounds', 'xPlayCont', 'lowDangShots', 'medDangShots', 'highDangShots', 
                  'lowDang_xGoals', 'medDang_xGoals', 'highDang_xGoals', 'rebound_xGoals', 
                  'xGoalsWithEarnedRebounds', 'line_xOnGoal', 'line_xGoals', 'line_sog', 
                  'line_lowDangShots','line_medDangShots', 'line_highDangShots','line_lowDang_xGoals',
                  'line_medDang_xGoals', 'line_highDang_xGoals', '5n5_fenwick%','5n5_line_sog', 
                  '5n4_sog', '5n4_atoi', '5n4_fenwick%', '5n4_line_sog']

#function to import multiple files of player data from 2015-2023 nhl seasons and return a dict of the pandas dfs
def import_data(years, base_filepath, df_cols):
    dataframes = {}
    for year in years:
        filepath = f"{base_filepath}\\skaters{year}.csv"
        dataframes[year] = pd.read_csv(filepath_or_buffer=filepath, usecols=df_cols)
    return dataframes

#function to preprocess all dfs and return a single, preprocessed df
def preprocess_data(dataframes, keep_cols_lst, cols_5n5_dict, cols_5n4_dict):
    dataframes_all = {} #dict for dfs with situation labeled as "all"
    dataframes_5n5 = {} #dict for dfs with situation labeled as "5on5"
    dataframes_5n4 = {} #dict for dfs with situation labeled as "5on4"
    merged_dataframes = {}  #dict for storing merged dfs and tu use for concatenation in last step

    #looping through each df stored in dataframes dict that is returned by import_data function
    for year, df in dataframes.items():
        #preprocess season and playerId columns
        df['season'] = year  #set 'season' to equal year at end of season
        df['playerId'] = df['playerId'].astype(str)
        df['season'] = df['season'].astype(str)
        df['playerId'] = df['playerId'] + df['season']  #concatenate 'playerId' and 'season'
        
        #preprocess for all situation
        df_all = df[df['situation'] == 'all']   #filtering df to just situations with string 'all'
        dataframes_all[f"df_{year}_all"] = df_all   #setting new df object to equal df_all
        
        #preprocess for 5on5 situation
        df_5n5 = df[df['situation'] == '5on5'][keep_cols_lst]   #filtering df to just 5on5 situations and with only columns in the list 'keep_cols_lst'
        cols_to_drop_5n5 = set(keep_cols_lst) - set(cols_5n5_dict.keys())   #creating a new set that contains all elements in 'keep_cols_lst' that are not in 'cols_5n5_dict.keys()'... set of the columns to drop
        df_5n5 = df_5n5.drop(cols_to_drop_5n5, axis=1).rename(columns=cols_5n5_dict)    #dropping undesired columns and renaming kept columns
        dataframes_5n5[f"df_{year}_5on5"] = df_5n5

        #preprocess for 5on4 situation
        df_5n4 = df[df['situation'] == '5on4'][keep_cols_lst].rename(columns=cols_5n4_dict)
        dataframes_5n4[f"df_{year}_5on4"] = df_5n4

        #merge dataframes
        merged_df = pd.merge(df_all, df_5n5, on='playerId', how='left') #merging df_all and df_5n5 on 'PlayerId' column
        merged_df = pd.merge(merged_df, df_5n4, on='playerId', how='left')  #merging merged_df and df_5n4 on 'playerId' column
        merged_dataframes[f"df_{year}"] = merged_df
        
        #concatenate the merged dataframes stored in the merged_dataframes dict
        df = pd.concat(merged_dataframes.values())
        
    return df

#conducting feature engineering, filtering, hot encoding, final column dropping, renaming, and reindexing. returns df
def final_preprocessing(df):
    df = df[df['games_played']>19]  #filtering out players with less than 20 games played
    df['atoi'] = round((df['icetime'] / 60) / df['games_played'], 2)    #calculating average time on ice (in minutes). computing conversion from total seconds in a season to average minutes per game
    df['5n4_atoi'] = round((df['5n4_icetime'] / 60) / df['games_played'], 2)    #5on4 atoi (in minutes)
    df['assists'] = df['I_F_points'] - df['I_F_goals']  #calculating assists (points-goals)
    df['position'] = df['position'].apply(lambda x: 1 if x in ['L', 'R', 'C'] else 0)   #if player position is a forward (a position found in the list) then player is assigned a value of 1, if defender then 0
    df['I_F_oZoneShiftStarts'] = round(df['I_F_oZoneShiftStarts'] / df['shifts'], 2)    #calculating percentage of shifts a player starts in offensive zone (offensive starts/total shifts)
    df['s%'] = round(df['I_F_goals'] / df['I_F_shotsOnGoal'], 2)    #calculating player shooting percentage (goals/shots on goal)
    df = df.drop(to_drop, axis=1)   #dropping columns in to_drop list
    df = df.rename(columns=to_rename)   #renaming columns in to_rename dict
    df = df.reindex(columns=cols_reindexed) #reindexing columns in cols_reindexed list
    
    return df

#using above functions
dataframes = import_data(years, base_filepath, df_cols) #reading csv files and saving them to dictionary called dataframes
df = preprocess_data(dataframes, keep_cols_lst, cols_5n5_dict, cols_5n4_dict)   #preprocessing data from dfs stored in dataframes dict and returning a single df, 'df'
df = final_preprocessing(df)    #conducting final preprocessing for the df

df.to_csv('player_data.csv')    #export to new csv file
