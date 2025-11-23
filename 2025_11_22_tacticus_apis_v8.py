#!/usr/bin/env python
# coding: utf-8

# In[63]:


import re
import requests
import pandas as pd
import numpy as np
import datetime as dt 
from time import gmtime, strftime
import warnings
import dropbox
import openpyxl
from io import BytesIO
import os
#allow all columns to be visible
pd.set_option('display.max_columns', None)
#supress scientific notation
pd.set_option('display.float_format', lambda x: '%.9f' % x)

# In[66]:


#environmental variables for secrets use

ACCESS_TOKEN = os.environ["ACCESS_TOKEN"]
api_bi = os.environ["api_bi"]
api_us = os.environ["api_us"]
api_vn = os.environ["api_vn"]
api_ky = os.environ["api_ky"]
dropbox_path = os.environ["dropbox_path"]


# In[67]:


"""
#previously generated mapping of all the guild members

global_member_list = pd.concat([
    bi_members[['userId','user_nicknames']],
    us_members[['userId','user_nicknames']],
    vn_members[['userId','user_nicknames']],
    ky_members[['userId','user_nicknames']]
    ], 
    axis=0, ignore_index=True)

with pd.ExcelWriter('global_member_list' + '.xlsx') as writer:
    global_member_list.to_excel(writer, sheet_name='global_member_list', index=False)

#persisted, to be then read from dropbox app folder
"""

dbx = dropbox.Dropbox(oauth2_access_token=ACCESS_TOKEN)
metadata, res = dbx.files_download(dropbox_path)
file_content = res.content
global_member_list = pd.read_excel(BytesIO(file_content), engine='openpyxl')


# In[68]:


url_guild = 'https://api.tacticusgame.com/api/v1/guild'
url_raid_generic = 'https://api.tacticusgame.com/api/v1/guildRaid'


# In[69]:


#get raid season number programmatically and make raid link season-specific

headers = {"accept": "application/json", "X-API-KEY": api_us}
r_raid = requests.get(url_raid_generic, headers = headers)
data_raid_generic = r_raid.json()

raid_season = data_raid_generic['season']

url_raid = 'https://api.tacticusgame.com/api/v1/guildRaid/' + str(raid_season)


# In[70]:


#define_main_input_function
def get_guild_data(guild_api, global_member_list):
    #pull guild data
    headers = {"accept": "application/json", "X-API-KEY": guild_api}
    r_guild = requests.get(url_guild, headers = headers)
    r_raid = requests.get(url_raid, headers = headers)

    data_guild = r_guild.json()
    data_raid = r_raid.json()
    
    guild_name = data_guild['guild']['name']
    guild_tag = data_guild['guild']['guildTag']
    guild_level = data_guild['guild']['level']
    
    #assemble data frame with members
    members = pd.DataFrame(data_guild['guild']['members'], columns=['userId', 'role', 'level', 'lastActivityOn'])
    df_members = pd.DataFrame(data_guild['guild']['members'], columns=['userId', 'role', 'level', 'lastActivityOn'])
    df_members = df_members.merge(global_member_list, on='userId', how='left')
    
    #assemble guild raid logs
    df_raid_log = pd.DataFrame(data_raid['entries'], columns=[
      'userId',
      'tier',
      'set',
      'encounterIndex',
      'remainingHp',
      'maxHp',
      'encounterType',
      'unitId',
      'type',
      'rarity',
      'damageDealt',
      'damageType',
      'startedOn',
      'completedOn',
      'heroDetails',
      'machineOfWarDetails',
      'globalConfigHash'
    ])
    
    #join data from guild table
    df_raid_log = df_raid_log.merge(df_members, on='userId', how='left')

    #add unique index number for each attack
    df_raid_log['attack_index'] = range(1, len(df_raid_log) + 1)

    #create dummy variables for units and MOWs
    df_raid_log['MOW_name'] = None
    df_raid_log['MOW_power'] = None

    df_raid_log['Unit_1_name'] = None
    df_raid_log['Unit_1_power'] = None

    df_raid_log['Unit_2_name'] = None
    df_raid_log['Unit_2_power'] = None

    df_raid_log['Unit_3_name'] = None
    df_raid_log['Unit_3_power'] = None

    df_raid_log['Unit_4_name'] = None
    df_raid_log['Unit_4_power'] = None

    df_raid_log['Unit_5_name'] = None
    df_raid_log['Unit_5_power'] = None
    
    #loop though units and MOWs fields and split them into separate columns
    for i in range(0,len(df_raid_log)-1):
        if df_raid_log.loc[i, 'damageType'] == "Bomb":
            continue
        elif df_raid_log.loc[i, 'damageType'] == "Battle":
            try:
                df_raid_log.loc[i, 'MOW_name'] = df_raid_log.loc[i, 'machineOfWarDetails']['unitId']
            except:
                df_raid_log.loc[i, 'MOW_name'] = np.nan  
            try:
                df_raid_log.loc[i, 'MOW_power'] = df_raid_log.loc[i, 'machineOfWarDetails']['power']
            except:
                df_raid_log.loc[i, 'MOW_power'] = np.nan
            try:
                df_raid_log.loc[i, 'Unit_1_name'] = df_raid_log.loc[i, 'heroDetails'][0]['unitId']
            except:
                df_raid_log.loc[i, 'Unit_1_name'] = np.nan
            try:
                df_raid_log.loc[i, 'Unit_1_power'] = df_raid_log.loc[i, 'heroDetails'][0]['power']
            except:
                df_raid_log.loc[i, 'Unit_1_power'] = np.nan
            try:
                df_raid_log.loc[i, 'Unit_2_name'] = df_raid_log.loc[i, 'heroDetails'][1]['unitId']
            except:
                df_raid_log.loc[i, 'Unit_2_name'] = np.nan
            try:
                df_raid_log.loc[i, 'Unit_2_power'] = df_raid_log.loc[i, 'heroDetails'][1]['power']
            except:
                df_raid_log.loc[i, 'Unit_2_power'] = np.nan
            try:
                df_raid_log.loc[i, 'Unit_3_name'] = df_raid_log.loc[i, 'heroDetails'][2]['unitId']
            except:
                df_raid_log.loc[i, 'Unit_3_name'] = np.nan
            try:
                df_raid_log.loc[i, 'Unit_3_power'] = df_raid_log.loc[i, 'heroDetails'][2]['power']
            except:
                df_raid_log.loc[i, 'Unit_3_power'] = np.nan
            try:
                df_raid_log.loc[i, 'Unit_4_name'] = df_raid_log.loc[i, 'heroDetails'][3]['unitId']
            except:
                df_raid_log.loc[i, 'Unit_4_name'] = np.nan
            try:
                df_raid_log.loc[i, 'Unit_4_power'] = df_raid_log.loc[i, 'heroDetails'][3]['power']
            except:
                df_raid_log.loc[i, 'Unit_4_power'] = np.nan
            try:
                df_raid_log.loc[i, 'Unit_5_name'] = df_raid_log.loc[i, 'heroDetails'][4]['unitId']
            except:
                df_raid_log.loc[i, 'Unit_5_name'] = np.nan
            try:
                df_raid_log.loc[i, 'Unit_5_power'] = df_raid_log.loc[i, 'heroDetails'][4]['power']
            except:
                df_raid_log.loc[i, 'Unit_5_power'] = np.nan
        else:
            continue
            
    #add flagging for meta teams
    df_raid_log['meta_mech_flag'] = np.where(df_raid_log['Unit_1_name'] == "admecRuststalker", 1,
                                    np.where(df_raid_log['Unit_2_name'] == "admecRuststalker", 1,
                                    np.where(df_raid_log['Unit_3_name'] == "admecRuststalker", 1,
                                    np.where(df_raid_log['Unit_4_name'] == "admecRuststalker", 1,
                                    np.where(df_raid_log['Unit_5_name'] == "admecRuststalker", 1,0)))))

    df_raid_log['meta_multi_flag'] = np.where(df_raid_log['Unit_1_name'] == "tauAunShi", 1,
                                     np.where(df_raid_log['Unit_2_name'] == "tauAunShi", 1,
                                     np.where(df_raid_log['Unit_3_name'] == "tauAunShi", 1,
                                     np.where(df_raid_log['Unit_4_name'] == "tauAunShi", 1,
                                     np.where(df_raid_log['Unit_5_name'] == "tauAunShi", 1,0)))))

    df_raid_log['meta_neuro_flag'] = np.where(df_raid_log['Unit_1_name'] == "tyranNeurothrope", 1,
                                     np.where(df_raid_log['Unit_2_name'] == "tyranNeurothrope", 1,
                                     np.where(df_raid_log['Unit_3_name'] == "tyranNeurothrope", 1,
                                     np.where(df_raid_log['Unit_4_name'] == "tyranNeurothrope", 1,
                                     np.where(df_raid_log['Unit_5_name'] == "tyranNeurothrope", 1,0)))))

    df_raid_log['meta_custodes_flag'] = np.where(df_raid_log['Unit_1_name'] == "custoBladeChampion", 1,
                                        np.where(df_raid_log['Unit_2_name'] == "custoBladeChampion", 1,
                                        np.where(df_raid_log['Unit_3_name'] == "custoBladeChampion", 1,
                                        np.where(df_raid_log['Unit_4_name'] == "custoBladeChampion", 1,
                                        np.where(df_raid_log['Unit_5_name'] == "custoBladeChampion", 1,0)))))

    #remove cases where damage is 0 from raid logs
    df_raid_log = df_raid_log.loc[df_raid_log['damageDealt'] > 0]

    #add guild name
    df_raid_log['guild'] = guild_name
            
    #aggregated raid data on player level
    aggregated_raid_data = (
        df_raid_log
        .groupby(['user_nicknames'])
        .apply(lambda g: pd.Series({
            #num attacks
            'num_attacks': g['attack_index'].count(),
            'num_bombs': (g['damageType'] == 'Bomb').sum(),
            'num_battles': (g['damageType'] == 'Battle').sum(),

            'num_attacks_bosses': g.loc[g['encounterType'] == 'Boss', 'attack_index'].count(),
            'num_bombs_bosses': g.loc[(g['damageType'] == 'Bomb') & (g['encounterType'] == 'Boss'),'attack_index'].count(),
            'num_battles_bosses': g.loc[(g['damageType'] == 'Battle') & (g['encounterType'] == 'Boss'),'attack_index'].count(),

            'num_attacks_side_bosses': g.loc[g['encounterType'] == 'SideBoss', 'attack_index'].count(),
            'num_bombs_side_bosses': g.loc[(g['damageType'] == 'Bomb') & (g['encounterType'] == 'SideBoss'),'attack_index'].count(),
            'num_battles_side_bosses': g.loc[(g['damageType'] == 'Battle') & (g['encounterType'] == 'SideBoss'),'attack_index'].count(),

            #damage
            'damage_attacks': g['damageDealt'].sum(),
            'damage_bombs': g.loc[g['damageType'] == 'Bomb', 'damageDealt'].sum(),
            'damage_battles': g.loc[g['damageType'] == 'Battle', 'damageDealt'].sum(),

            'damage_attacks_bosses': g.loc[g['encounterType'] == 'Boss', 'damageDealt'].sum(),
            'damage_bombs_bosses': g.loc[(g['damageType'] == 'Bomb') & (g['encounterType'] == 'Boss'),'damageDealt'].sum(),
            'damage_battles_bosses': g.loc[(g['damageType'] == 'Battle') & (g['encounterType'] == 'Boss'),'damageDealt'].sum(),

            'damage_attacks_side_bosses': g.loc[g['encounterType'] == 'SideBoss', 'damageDealt'].sum(),
            'damage_bombs_side_bosses': g.loc[(g['damageType'] == 'Bomb') & (g['encounterType'] == 'SideBoss'),'damageDealt'].sum(),
            'damage_battles_side_bosses': g.loc[(g['damageType'] == 'Battle') & (g['encounterType'] == 'SideBoss'),'damageDealt'].sum(),

            #legendaries num attacks - tier 4 is first circle of legendaries
            'num_attacks_legendary': g.loc[g['tier'] >= 4, 'attack_index'].count(),
            'num_bombs_legendary': g.loc[(g['damageType'] == 'Bomb') & (g['tier'] >= 4),'attack_index'].count(),
            'num_battles_legendary': g.loc[(g['damageType'] == 'Battle') & (g['tier'] >= 4),'attack_index'].count(),

            'num_attacks_bosses_legendary': g.loc[(g['encounterType'] == 'Boss') & (g['tier'] >= 4), 'attack_index'].count(),
            'num_bombs_bosses_legendary': g.loc[(g['damageType'] == 'Bomb') & (g['encounterType'] == 'Boss') & (g['tier'] >= 4),'attack_index'].count(),
            'num_battles_bosses_legendary': g.loc[(g['damageType'] == 'Battle') & (g['encounterType'] == 'Boss') & (g['tier'] >= 4),'attack_index'].count(),

            'num_attacks_side_bosses_legendary': g.loc[(g['encounterType'] == 'SideBoss') & (g['tier'] >= 4), 'attack_index'].count(),
            'num_bombs_side_bosses_legendary': g.loc[(g['damageType'] == 'Bomb') & (g['encounterType'] == 'SideBoss') & (g['tier'] >= 4),'attack_index'].count(),
            'num_battles_side_bosses_legendary': g.loc[(g['damageType'] == 'Battle') & (g['encounterType'] == 'SideBoss') & (g['tier'] >= 4),'attack_index'].count(),

            #legendaries damage
            'damage_attacks_legendary': g.loc[g['tier'] >= 4, 'damageDealt'].sum(),
            'damage_bombs_legendary': g.loc[(g['damageType'] == 'Bomb') & (g['tier'] >= 4),'damageDealt'].sum(),
            'damage_battles_legendary': g.loc[(g['damageType'] == 'Battle') & (g['tier'] >= 4),'damageDealt'].sum(),

            'damage_attacks_bosses_legendary': g.loc[(g['encounterType'] == 'Boss') & (g['tier'] >= 4), 'damageDealt'].sum(),
            'damage_bombs_bosses_legendary': g.loc[(g['damageType'] == 'Bomb') & (g['encounterType'] == 'Boss') & (g['tier'] >= 4),'damageDealt'].sum(),
            'damage_battles_bosses_legendary': g.loc[(g['damageType'] == 'Battle') & (g['encounterType'] == 'Boss') & (g['tier'] >= 4),'damageDealt'].sum(),

            'damage_attacks_side_bosses_legendary': g.loc[(g['encounterType'] == 'SideBoss') & (g['tier'] >= 4), 'damageDealt'].sum(),
            'damage_bombs_side_bosses_legendary': g.loc[(g['damageType'] == 'Bomb') & (g['encounterType'] == 'SideBoss') & (g['tier'] >= 4),'damageDealt'].sum(),
            'damage_battles_side_bosses_legendary': g.loc[(g['damageType'] == 'Battle') & (g['encounterType'] == 'SideBoss') & (g['tier'] >= 4),'damageDealt'].sum(),

            #meta teams flagging
            'num_battles_meta_mech': g.loc[(g['damageType'] == 'Battle') & (g['meta_mech_flag'] == 1),'attack_index'].count(),
            'num_battles_meta_multi': g.loc[(g['damageType'] == 'Battle') & (g['meta_multi_flag'] == 1),'attack_index'].count(),
            'num_battles_meta_neuro': g.loc[(g['damageType'] == 'Battle') & (g['meta_neuro_flag'] == 1),'attack_index'].count(),
            'num_battles_meta_custodes': g.loc[(g['damageType'] == 'Battle') & (g['meta_custodes_flag'] == 1),'attack_index'].count(),
            
            'damage_meta_mech': g.loc[(g['damageType'] == 'Battle') & (g['meta_mech_flag'] == 1),'damageDealt'].sum(),
            'damage_meta_multi': g.loc[(g['damageType'] == 'Battle') & (g['meta_multi_flag'] == 1),'damageDealt'].sum(),
            'damage_meta_neuro': g.loc[(g['damageType'] == 'Battle') & (g['meta_neuro_flag'] == 1),'damageDealt'].sum(),
            'damage_meta_custodes': g.loc[(g['damageType'] == 'Battle') & (g['meta_custodes_flag'] == 1),'damageDealt'].sum(),
            
            #meta teams with legendaries
            'num_battles_meta_mech_legendary': g.loc[(g['damageType'] == 'Battle') & (g['meta_mech_flag'] == 1) & (g['tier'] >= 4),'attack_index'].count(),
            'num_battles_meta_multi_legendary': g.loc[(g['damageType'] == 'Battle') & (g['meta_multi_flag'] == 1) & (g['tier'] >= 4),'attack_index'].count(),
            'num_battles_meta_neuro_legendary': g.loc[(g['damageType'] == 'Battle') & (g['meta_neuro_flag'] == 1) & (g['tier'] >= 4),'attack_index'].count(),
            'num_battles_meta_custodes_legendary': g.loc[(g['damageType'] == 'Battle') & (g['meta_custodes_flag'] == 1) & (g['tier'] >= 4),'attack_index'].count(),
            
            'damage_meta_mech_legendary': g.loc[(g['damageType'] == 'Battle') & (g['meta_mech_flag'] == 1) & (g['tier'] >= 4),'damageDealt'].sum(),
            'damage_meta_multi_legendary': g.loc[(g['damageType'] == 'Battle') & (g['meta_multi_flag'] == 1) & (g['tier'] >= 4),'damageDealt'].sum(),
            'damage_meta_neuro_legendary': g.loc[(g['damageType'] == 'Battle') & (g['meta_neuro_flag'] == 1) & (g['tier'] >= 4),'damageDealt'].sum(),
            'damage_meta_custodes_legendary': g.loc[(g['damageType'] == 'Battle') & (g['meta_custodes_flag'] == 1) & (g['tier'] >= 4),'damageDealt'].sum(),
            
            #averages
            'avg_damage_attacks': g['damageDealt'].mean(),
            'avg_damage_bombs': g.loc[g['damageType'] == 'Bomb', 'damageDealt'].mean(),
            'avg_damage_battles': g.loc[g['damageType'] == 'Battle', 'damageDealt'].mean(),

            'avg_damage_attacks_bosses': g.loc[g['encounterType'] == 'Boss', 'damageDealt'].mean(),
            'avg_damage_bombs_bosses': g.loc[(g['damageType'] == 'Bomb') & (g['encounterType'] == 'Boss'),'damageDealt'].mean(),
            'avg_damage_battles_bosses': g.loc[(g['damageType'] == 'Battle') & (g['encounterType'] == 'Boss'),'damageDealt'].mean(),

            'avg_damage_attacks_side_bosses': g.loc[g['encounterType'] == 'SideBoss', 'damageDealt'].mean(),
            'avg_damage_bombs_side_bosses': g.loc[(g['damageType'] == 'Bomb') & (g['encounterType'] == 'SideBoss'),'damageDealt'].mean(),
            'avg_damage_battles_side_bosses': g.loc[(g['damageType'] == 'Battle') & (g['encounterType'] == 'SideBoss'),'damageDealt'].mean(),

            'avg_damage_attacks_legendary': g.loc[g['tier'] >= 4, 'damageDealt'].mean(),
            'avg_damage_bombs_legendary': g.loc[(g['damageType'] == 'Bomb') & (g['tier'] >= 4),'damageDealt'].mean(),
            'avg_damage_battles_legendary': g.loc[(g['damageType'] == 'Battle') & (g['tier'] >= 4),'damageDealt'].mean(),

            'avg_damage_attacks_bosses_legendary': g.loc[(g['encounterType'] == 'Boss') & (g['tier'] >= 4), 'damageDealt'].mean(),
            'avg_damage_bombs_bosses_legendary': g.loc[(g['damageType'] == 'Bomb') & (g['encounterType'] == 'Boss') & (g['tier'] >= 4),'damageDealt'].mean(),
            'avg_damage_battles_bosses_legendary': g.loc[(g['damageType'] == 'Battle') & (g['encounterType'] == 'Boss') & (g['tier'] >= 4),'damageDealt'].mean(),

            'avg_damage_attacks_side_bosses_legendary': g.loc[(g['encounterType'] == 'SideBoss') & (g['tier'] >= 4), 'damageDealt'].mean(),
            'avg_damage_bombs_side_bosses_legendary': g.loc[(g['damageType'] == 'Bomb') & (g['encounterType'] == 'SideBoss') & (g['tier'] >= 4),'damageDealt'].mean(),
            'avg_damage_battles_side_bosses_legendary': g.loc[(g['damageType'] == 'Battle') & (g['encounterType'] == 'SideBoss') & (g['tier'] >= 4),'damageDealt'].mean(),
            
            'avg_damage_meta_mech': g.loc[(g['damageType'] == 'Battle') & (g['meta_mech_flag'] == 1),'damageDealt'].mean(),
            'avg_damage_meta_multi': g.loc[(g['damageType'] == 'Battle') & (g['meta_multi_flag'] == 1),'damageDealt'].mean(),
            'avg_damage_meta_neuro': g.loc[(g['damageType'] == 'Battle') & (g['meta_neuro_flag'] == 1),'damageDealt'].mean(),
            'avg_damage_meta_custodes': g.loc[(g['damageType'] == 'Battle') & (g['meta_custodes_flag'] == 1),'damageDealt'].mean(),
                        
            'avg_damage_meta_mech_legendary': g.loc[(g['damageType'] == 'Battle') & (g['meta_mech_flag'] == 1) & (g['tier'] >= 4),'damageDealt'].mean(),
            'avg_damage_meta_multi_legendary': g.loc[(g['damageType'] == 'Battle') & (g['meta_multi_flag'] == 1) & (g['tier'] >= 4),'damageDealt'].mean(),
            'avg_damage_meta_neuro_legendary': g.loc[(g['damageType'] == 'Battle') & (g['meta_neuro_flag'] == 1) & (g['tier'] >= 4),'damageDealt'].mean(),
            'avg_damage_meta_custodes_legendary': g.loc[(g['damageType'] == 'Battle') & (g['meta_custodes_flag'] == 1) & (g['tier'] >= 4),'damageDealt'].mean()
        }))
        .reset_index()
    )
    
    #remove decimals before calculating efficiency and points
    aggregated_raid_data = aggregated_raid_data.round()
    
    #replace NaNs with zeroes
    aggregated_raid_data = aggregated_raid_data.fillna(0)
    
    #add guild name as first column
    aggregated_raid_data.insert(0, 'guild', guild_name)  

    ################################################calculate toplines per bosses and side bosses
    #catch formatted version of side bosses and bosses names
    pattern = r'(?:.*?\d+){2}(.*)$'
    df_raid_log['unit_name'] = df_raid_log['unitId'].str.extract(pattern)
    df_raid_log['unit_name'] = df_raid_log['rarity'] + df_raid_log['encounterType'] + "_" + df_raid_log['unit_name']

    boss_df = (
        df_raid_log[
            (df_raid_log['damageType'] == 'Battle') &
            (df_raid_log['tier'] >= 4)
        ]
        .groupby(['user_nicknames', 'unit_name'])['damageDealt']
        .agg(
            num_battles='count',
            total_damage='sum',
            avg_damage='mean'
        )
        .reset_index()
    )

    #add guild name to be used further on
    boss_df['guild'] = guild_name
    boss_df = boss_df[['guild','user_nicknames','unit_name','num_battles','total_damage','avg_damage']]

    #pivot and format toplines per bosses
    pivot_boss_df = boss_df.pivot(index='user_nicknames', columns='unit_name', values=['num_battles', 'total_damage', 'avg_damage'])
    pivot_boss_df.columns = ['_'.join(str(s).strip() for s in col if s) for col in pivot_boss_df.columns]
    pivot_boss_df.reset_index(inplace=True)
    pivot_boss_df = pivot_boss_df.fillna(0)
    pivot_boss_df = pivot_boss_df.round()

    #merge with main aggr data
    aggregated_raid_data = aggregated_raid_data.merge(pivot_boss_df, on='user_nicknames', how='left')

    #calculate number of additional circles per guild, add it to aggr frame
    add_circles = (max(df_raid_log['tier']) - 5)/2
    aggregated_raid_data['add_circles'] = add_circles

    return df_members, df_raid_log, aggregated_raid_data, boss_df


# In[71]:


#set dummy dfs to be further populated
bi_members = pd.DataFrame()
us_members = pd.DataFrame()
vn_members = pd.DataFrame()
ky_members = pd.DataFrame()

bi_source_raid_log = pd.DataFrame()
us_source_raid_log = pd.DataFrame()
vn_source_raid_log = pd.DataFrame()
ky_source_raid_log = pd.DataFrame()

bi_boss_df = pd.DataFrame()
us_boss_df = pd.DataFrame()
vn_boss_df = pd.DataFrame()
ky_boss_df = pd.DataFrame()

us_aggr_raid_log = pd.DataFrame() 
bi_aggr_raid_log = pd.DataFrame()
vn_aggr_raid_log = pd.DataFrame()
ky_aggr_raid_log = pd.DataFrame()

#run the function to pull and format data
try:
    bi_members, bi_source_raid_log, bi_aggr_raid_log, bi_boss_df = get_guild_data(api_bi, global_member_list)
    print("bi_done")
except Exception:
    print("bi_error")
    
try:
    us_members, us_source_raid_log, us_aggr_raid_log, us_boss_df = get_guild_data(api_us, global_member_list)
    print("us_done")
except Exception:
    print("us_error")

try:
    vn_members, vn_source_raid_log, vn_aggr_raid_log, vn_boss_df = get_guild_data(api_vn, global_member_list)
    print("vn_done")
except Exception:
    print("vn_error")
    
"""
try:
    ky_members, ky_source_raid_log, ky_aggr_raid_log, ky_boss_df = get_guild_data(api_ky, global_member_list)
    print("ky_done")
except Exception:
    print("ky_error")
"""


# In[72]:


#create an empty dfs to be populated with further pulls
benchmark_total_boss_df = pd.DataFrame()
global_boss_df = pd.DataFrame()
global_aggr_raid_log = pd.DataFrame()

processed_logs = [
    bi_source_raid_log, 
    us_source_raid_log,
    vn_source_raid_log,
    ky_source_raid_log
]

processed_boss_logs = [
    bi_boss_df,
    us_boss_df,
    vn_boss_df,
    ky_boss_df
]

processed_aggr_logs = [
    us_aggr_raid_log, 
    bi_aggr_raid_log, 
    vn_aggr_raid_log, 
    ky_aggr_raid_log
]

#merge dfs in case they were properly generated, or skip them in case of error
for df_order in range(0,len(processed_logs)):
    benchmark_total_boss_df = pd.concat([benchmark_total_boss_df, processed_logs[df_order]], axis=0, ignore_index=True)
    global_boss_df = pd.concat([global_boss_df, processed_boss_logs[df_order]], axis=0, ignore_index=True)
    global_aggr_raid_log = pd.concat([global_aggr_raid_log, processed_aggr_logs[df_order]], axis=0, ignore_index=True)


# In[73]:


#calculate benchmarks for boss damage
benchmark_total_boss_df = benchmark_total_boss_df.loc[
    (benchmark_total_boss_df['damageType'] == 'Battle') &
    (benchmark_total_boss_df['tier'] >= 4)
].groupby(['guild', 'unit_name']).apply(lambda g: pd.Series({
    'benchmark_num_battles': g['damageDealt'].count(),
    'benchmark_total_damage': g['damageDealt'].sum(),
    'benchmark_avg_damage': g['damageDealt'].mean()
})).reset_index()

benchmark_total_boss_df = benchmark_total_boss_df.groupby(['unit_name']).apply(lambda g: pd.Series({
    'benchmark_max_avg_damage': g['benchmark_avg_damage'].max()
})).reset_index()


# In[74]:


#calculate efficiency for individual bosses for everybody
global_boss_df = global_boss_df.merge(benchmark_total_boss_df[['unit_name','benchmark_max_avg_damage']], on='unit_name', how='left')

global_boss_df['global_efficiency'] = global_boss_df['avg_damage'] / global_boss_df['benchmark_max_avg_damage']
global_boss_df['global_points'] = global_boss_df['global_efficiency'] * global_boss_df['num_battles']

global_boss_df['global_efficiency'] = global_boss_df['global_efficiency'].round(3)
global_boss_df['global_points'] = global_boss_df['global_points'].round(3)

global_boss_df['guild_and_name'] = global_boss_df['guild'] + global_boss_df['user_nicknames']


# In[75]:


#calculate total global efficiency, aggregated across all bosses on a player level
aggr_global_boss_df = global_boss_df.groupby(['guild','user_nicknames']).apply(lambda g: pd.Series({
    'total_points': g['global_points'].sum()
})).reset_index()

aggr_global_boss_df = aggr_global_boss_df.sort_values(by='total_points',ascending=False)

aggr_global_boss_df['guild_and_name'] = aggr_global_boss_df['guild'] + aggr_global_boss_df['user_nicknames']


# In[76]:


#calculate total points per boss, merge with aggr_global_boss_df
pivot_global_boss_df = global_boss_df.pivot(index='guild_and_name', columns='unit_name', values=['global_points'])
pivot_global_boss_df.columns = ['_'.join(str(s).strip() for s in col if s) for col in pivot_global_boss_df.columns]
pivot_global_boss_df.reset_index(inplace=True)
pivot_global_boss_df = pivot_global_boss_df.fillna(0)
pivot_global_boss_df = pivot_global_boss_df.round(3)

aggr_global_boss_df = aggr_global_boss_df.merge(pivot_global_boss_df, on=['guild_and_name'], how='left')

#create topline version of global export
global_detailed_toplines = global_aggr_raid_log[[
    'guild',
    'user_nicknames',
    "add_circles",
    "num_battles",
    'num_bombs',
    "avg_damage_battles",
    "num_battles_legendary",
    "avg_damage_battles_legendary",
    'num_battles_meta_mech_legendary',
    'num_battles_meta_custodes_legendary',
    'num_battles_meta_neuro_legendary',
    'num_battles_meta_multi_legendary'
]].merge(aggr_global_boss_df, on=['guild','user_nicknames'], how='left')

global_detailed_toplines = global_detailed_toplines.sort_values(by='total_points',ascending=False)

global_aggr_toplines = global_detailed_toplines[[
    "guild",
    "user_nicknames",
    "num_battles",
    "avg_damage_battles_legendary",
    "total_points"
]]

global_aggr_toplines['raid_season'] = raid_season
global_aggr_toplines['update_time'] = strftime("%Y-%m-%d %H:%M:%S", gmtime())

global_aggr_toplines = global_aggr_toplines[[
    'raid_season',
    'update_time',
    "guild",
    "user_nicknames",
    "num_battles",
    "avg_damage_battles_legendary",
    "total_points"
]]

#remove redundant columns
global_detailed_toplines = global_detailed_toplines.drop('guild_and_name', axis=1)
global_boss_df = global_boss_df.drop('guild_and_name', axis=1)


# In[77]:


#Export main file
output_file = 'global_toplines' + '.xlsx'

with pd.ExcelWriter(output_file, engine="openpyxl") as writer:

    global_aggr_toplines.to_excel(writer, sheet_name='Global_aggregated_toplines', index=False)
    global_detailed_toplines.to_excel(writer, sheet_name='Global_detailed_toplines', index=False)
    global_boss_df.to_excel(writer, sheet_name='Global_boss_df', index=False)
    global_aggr_raid_log.to_excel(writer, sheet_name='Full_alliance_detaield', index=False)
    us_aggr_raid_log.to_excel(writer, sheet_name='US_detailed', index=False)
    bi_aggr_raid_log.to_excel(writer, sheet_name='BI_detailed', index=False)
    vn_aggr_raid_log.to_excel(writer, sheet_name='VN_detailed', index=False)
    ky_aggr_raid_log.to_excel(writer, sheet_name='KY_detailed', index=False)

# Connect to Dropbox
dbx = dropbox.Dropbox(ACCESS_TOKEN)

# Local file you want to upload
local_file = 'global_toplines' + '.xlsx'

# Path in Dropbox (inside your app folder or full Dropbox)
dropbox_path = "/" +'global_toplines' + '.xlsx'

# Upload the file
with open(local_file, "rb") as f:
    dbx.files_upload(
        f.read(), 
        dropbox_path, 
        mode=dropbox.files.WriteMode.overwrite)

print(f"File uploaded to Dropbox at: {dropbox_path}")





