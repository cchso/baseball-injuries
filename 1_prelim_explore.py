import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres@127.0.0.1:5432/baseball')

pull_string = """
    WITH base AS (
        SELECT
            player_id
            , player_name
            , fgid
            , mlbid
            , brefid
            , espnid
            , bpid
        FROM master
        WHERE pos IN ('p', 'P') --pitchers only
    ),

    surgery AS (
        SELECT
            b.*
            , surgery_dt
            , s.lvl AS top_lvl_reached
            , s.age
        FROM base AS b
        LEFT JOIN surgery AS s ON b.fgid = s.fgid
        ORDER BY b.player_id
    )

    SELECT
        *
    FROM surgery
"""
pitchers = pd.read_sql_query(pull_string,con=engine)

pitcher_samp = np.array_split(pitchers,100)
id_list = pitcher_samp[0].mlbid

pitcher_list = id_list[0].astype('int').astype('str')
for i in range(1, len(id_list)) :
    pitcher_list += ", " + id_list[i].astype('int').astype('str')

statcast_string = """
    SELECT
        pitcher
        , player_name
        , game_date
        , pitch_type
        , release_speed
        , zone
        , pfx_x
        , pfx_z
        , plate_x
        , plate_z
        , vx0
        , vy0
        , vz0
        , ax
        , ay
        , az
        , sz_top
        , sz_bot
        , release_pos_x
        , release_pos_z
        , release_pos_y
        , effective_speed
        , release_spin_rate
        , release_extension
    FROM statcast
    WHERE pitcher IN ({0})
""".format(pitcher_list)
sc1 = pd.read_sql_query(statcast_string,con=engine)

sc1.game_date.head()

#get month and year from game_date
from datetime import datetime
sc1['game_date'] = pd.to_datetime(sc1['game_date'], format='%Y-%m-%d')
sc1['game_month'] = sc1.game_date.dt.month
sc1['game_year'] = sc1.game_date.dt.year
pitches = ['FF','FT','CU','CH','SL'] #let's look at the most common pitches at first
sc1 = sc1[sc1['pitch_type'].isin(pitches)]


#create some aggregations on columns, can be pruned later
stats = sc1.groupby(['pitcher','player_name', 'pitch_type', 'game_year', 'game_month'])['vx0', 'vy0', 'vz0', 'ax', 'ay', 'az','release_speed', 'release_extension', 'pfx_x', 'pfx_z'].agg(['mean','median','min','max'])
stats.columns = ["_".join(x) for x in stats.columns.ravel()]

#get number of pitches by type
pitchnums = sc1.groupby(['pitcher', 'player_name', 'pitch_type', 'game_year', 'game_month'])['pitch_type'].count()
pitchnums = pitchnums.to_frame().rename(columns={'pitch_type':'pitchcount'}).reset_index()

#total pitches
totalpitches = sc1.groupby(['pitcher','player_name','game_year', 'game_month'])['pitcher'].count()
totalpitches = totalpitches.to_frame().rename(columns={'pitcher':'totalpitches'}).reset_index()

stats_2 = stats.merge(pitchnums, how='left', on=['pitcher', 'player_name', 'pitch_type', 'game_year', 'game_month'])
stats_2 = stats_2.merge(totalpitches, how='left', on=['pitcher', 'player_name', 'game_year', 'game_month'])

#calculate a target definition - in this case, tommy john surgery
pitchers.head()

pitchers['injury_flag'] = np.where(pitchers['surgery_dt'].isnull(), 0, 1)
pitchers['surgery_dt'] = pd.to_datetime(pitchers['surgery_dt'], infer_datetime_format=True)

bad_df = pitchers[['mlbid','surgery_dt','injury_flag']].rename(columns={'mlbid':'pitcher'})

overall_df = stats_2.merge(bad_df, how = 'inner', on = 'pitcher')

#remove all data from after surgery dates
overall_df['surgery_year'] = overall_df['surgery_dt'].dt.year
overall_df['surgery_month'] = overall_df['surgery_dt'].dt.month

overall_df[(overall_df.pitcher == 346793) & (overall_df.pitch_type == 'FF')]

346793
