from pybaseball import statcast
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')
%matplotlib inline

def small_request(start_dt,end_dt):
    url = "https://baseballsavant.mlb.com/statcast_search/csv?all=true&hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfGT=R%7CPO%7CS%7C=&hfSea=&hfSit=&player_type=pitcher&hfOuts=&opponent=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt={}&game_date_lt={}&team=&position=&hfRO=&home_road=&hfFlag=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name&sort_col=pitches&player_event_sort=h_launch_speed&sort_order=desc&min_abs=0&type=details&".format(start_dt, end_dt)
    s=requests.get(url, timeout=None).content
    data = pd.read_csv(io.StringIO(s.decode('utf-8')))#, error_bad_lines=False) # skips 'bad lines' breaking scrapes. still testing this.
    return data

def large_request(start_dt,end_dt,d1,d2,step,verbose):
    """
    break start and end date into smaller increments, collecting all data in small chunks and appending all results to a common dataframe
    end_dt is the date strings for the final day of the query
    d1 and d2 are datetime objects for first and last day of query, for doing date math
    a third datetime object (d) will be used to increment over time for the several intermediate queries
    """
    error_counter = 0 # count failed requests. If > X, break
    no_success_msg_flag = False # a flag for passing over the success message of requests are failing
    print("This is a large query, it may take a moment to complete")
    dataframe_list = []
    #step = 3 # number of days per mini-query (test this later to see how large I can make this without losing data)
    d = d1 + datetime.timedelta(days=step)
    while d <= d2: #while intermediate query end_dt <= global query end_dt, keep looping
        # dates before 3/15 and after 11/15 will always be offseason
        # if these dates are detected, check if the next season is within the user's query
        # if yes, fast-forward to the next season to avoid empty requests
        # if no, break the loop. all useful data has been pulled.
        if ((d.month < 4 and d.day < 15) or (d1.month > 10 and d1.day > 14)):
            if d2.year > d.year:
                print('Skipping offseason dates')
                d1 = d1.replace(month=3,day=15,year=d1.year+1)
                d = d1 + datetime.timedelta(days=step+1)
            else:
                break

        start_dt = d1.strftime('%Y-%m-%d')
        intermediate_end_dt = d.strftime('%Y-%m-%d')
        data = small_request(start_dt,intermediate_end_dt)
        # append to list of dataframes if not empty or failed (failed requests have one row saying "Error: Query Timeout")
        if data.shape[0] > 1:
            dataframe_list.append(data)
        # if it failed, retry up to three times
        else:
            success = 0
            while success == 0:
                data = small_request(start_dt,intermediate_end_dt)
                if data.shape[0] > 1:
                    dataframe_list.append(data)
                    success = 1
                else:
                    error_counter += 1
                if error_counter > 2:
                    # this request is probably too large. Cut a day off of this request and make that its own separate request.
                    # For each, append to dataframe list if successful, skip and print error message if failed
                    tmp_end = d - datetime.timedelta(days=1)
                    tmp_end = tmp_end.strftime('%Y-%m-%d')
                    smaller_data_1 = small_request(start_dt, tmp_end)
                    smaller_data_2 = small_request(intermediate_end_dt,intermediate_end_dt)
                    if smaller_data_1.shape[0] > 1:
                        dataframe_list.append(smaller_data_1)
                        print("Completed sub-query from {} to {}".format(start_dt,tmp_end))
                    else:
                        print("Query unsuccessful for data from {} to {}. Skipping these dates.".format(start_dt,tmp_end))
                    if smaller_data_2.shape[0] > 1:
                        dataframe_list.append(smaller_data_2)
                        print("Completed sub-query from {} to {}".format(intermediate_end_dt,intermediate_end_dt))
                    else:
                        print("Query unsuccessful for data from {} to {}. Skipping these dates.".format(intermediate_end_dt,intermediate_end_dt))

                    no_success_msg_flag = True # flag for passing over the success message since this request failed
                    error_counter = 0 # reset counter
                    break


        if verbose:
            if no_success_msg_flag is False:
                print("Completed sub-query from {} to {}".format(start_dt,intermediate_end_dt))
            else:
                no_success_msg_flag = False # if failed, reset this flag so message will send again next iteration
        # increment dates
        d1 = d + datetime.timedelta(days=1)
        d = d + datetime.timedelta(days=step+1)

    # if start date > end date after being incremented, the loop captured each date's data
    if d1 > d2:
        pass
    # if start date <= end date, then there are a few leftover dates to grab data for.
    else:
        # start_dt from the earlier loop will work, but instead of d we now want the original end_dt
        start_dt = d1.strftime('%Y-%m-%d')
        data = small_request(start_dt,end_dt)
        dataframe_list.append(data)
        if verbose:
            print("Completed sub-query from {} to {}".format(start_dt,end_dt))

    # concatenate all dataframes into final result set
    final_data = pd.concat(dataframe_list, axis=0)
    return final_data


def statcast(start_dt=None, end_dt=None, team=None, verbose=True):
    """
    Pulls statcast play-level data from Baseball Savant for a given date range.
    INPUTS:
    start_dt: YYYY-MM-DD : the first date for which you want statcast data
    end_dt: YYYY-MM-DD : the last date for which you want statcast data
    team: optional (defaults to None) : city abbreviation of the team you want data for (e.g. SEA or BOS)
    If no arguments are provided, this will return yesterday's statcast data. If one date is provided, it will return that date's statcast data.
    """

    start_dt, end_dt = sanitize_input(start_dt, end_dt)
    # 3 days or less -> a quick one-shot request. Greater than 3 days -> break it into multiple smaller queries
    small_query_threshold = 5
    # inputs are valid if either both or zero dates are supplied. Not valid of only one given.

    if start_dt and end_dt:
        # how many days worth of data are needed?
        date_format = "%Y-%m-%d"
        d1 = datetime.datetime.strptime(start_dt, date_format)
        d2 = datetime.datetime.strptime(end_dt, date_format)
        days_in_query = (d2 - d1).days
        if days_in_query <= small_query_threshold:
            data = small_request(start_dt,end_dt)
        else:
            data = large_request(start_dt,end_dt,d1,d2,step=small_query_threshold,verbose=verbose)

        data = postprocessing(data, team)
        return data




start_dates = ['2012-03-25', '2012-07-02', '2013-03-25', '2013-07-02', '2014-03-25', '2014-07-02', '2015-03-25','2015-07-02', '2016-03-25','2016-07-02',  '2017-03-25', '2017-07-02']
end_dates = ['2012-07-01', '2012-11-01', '2013-07-01', '2013-11-01', '2014-07-01', '2014-11-01', '2015-07-01', '2015-11-01', '2016-07-01', '2016-11-01', '2017-07-01', '2017-11-01']

for i in range(len(start_dates)) :
    start = start_dates[i]
    end = end_dates[i]
    df = statcast(start_dt=start, end_dt=end)
    df = df[['pitcher', 'player_name', 'pitchtype', 'pitch_name', 'game_date', 'release_speed', 'release_pos_x', 'release_pos_y', 'release_pos_z', 'spin_dir', 'pfx_x', 'pfx_z', 'plate_x', 'plate_z', 'vx0', 'vy0', 'vz0'
    , 'ax', 'ay', 'az']]

    df.to_csv('C:/Users/Colin/Documents/GitHub/baseball-injuries/datasets/statcast_{0}_{1}.csv'.format(start, end))
