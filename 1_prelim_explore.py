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
            , CASE WHEN surgery_dt::DATE IS NULL THEN CURRENT_DATE ELSE surgery_dt::DATE END AS surgery_dt
            , s.lvl AS top_lvl_reached
            , s.age
        FROM base AS b
        LEFT JOIN surgery AS s ON b.fgid = s.fgid
        ORDER BY b.player_id
    )

    SELECT
        s.mlbid
        , s.player_name
        , s.surgery_dt
        , s.top_lvl_reached
        , s.age AS surgery_age
        , st.pitch_type
        , st.release_speed
    FROM surgery AS s
    LEFT JOIN statcast AS st ON s.mlbid = st.pitcher
    WHERE game_date <= surgery_dt
    LIMIT 200000
    """

df = pd.read_sql_query(pull_string,con=engine)

df.shape
