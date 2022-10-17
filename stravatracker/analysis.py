""" Defines functions to update the strava database file
Contains the following functions:
    check_last_timeout()
    strava_update()
        request_headers()
        create_id_list()
            return_json()
        get_new_activities()
            return_json()
dependencies:
    datimetime as dt
    pandas as pd
    json
    requests
    urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

Author: rakeshrgill rakeshrgill@gmail.com
Created: 2022/10/16
"""

import pandas as pd
import numpy as np
import datetime as dt
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def excel_clean(df):
    """ Returns excel_df
    Parameters
    ----------
    config: dictionary
    config['last_timeout_daily']
    config['last_timeout_15min']

    Returns
    -------
    bool
    true if can update, false if timeout

    Raises
    ------

    """
    strava_df = df

    # Break date into start time and date
    strava_df['start_date_local'] = pd.to_datetime(strava_df['start_date_local'])
    strava_df['start_time'] = strava_df['start_date_local'].dt.time
    strava_df['start_date_local'] = strava_df['start_date_local'].dt.date

    # Rename Activity Types
    strava_df['type'] = strava_df['type'].replace({'VirtualRide': 'Bike', 'Ride': 'Bike', 'WeightTraining': 'Strength', 'Workout': 'Strength'})
    strava_df['moving_time'] = strava_df['moving_time'] / 86400
    strava_df['elapsed_time'] = strava_df['elapsed_time'] / 86400
    strava_df['distance'] = strava_df['distance'] / 1000
    strava_df['average_pace_run'] = (1 / strava_df['average_speed']) * (1000 / 86400)
    strava_df['average_pace_swim'] = ((1 / strava_df['average_speed']) * (1000 / 86400)) / 10

    # Swim
    strava_df['excel_time'] = strava_df['moving_time']
    strava_df.loc[strava_df['type'] == 'Swim', "excel_time"] = strava_df['elapsed_time']

    # Core Sport Level
    strava_df.loc[(strava_df['type'] != 'Run') & (strava_df['type'] != 'Hike'), "average_pace_run"] = np.NaN
    strava_df.loc[strava_df['type'] != 'Swim', "average_pace_swim"] = np.NaN
    strava_df.loc[strava_df['type'] != 'Bike', "average_watts"] = np.NaN
    strava_df.loc[strava_df['type'] == 'Swim', "average_heartrate"] = np.NaN

    # Non distance sports
    strava_df.loc[strava_df['type'] == 'Strength', "distance"] = np.NaN
    strava_df.loc[strava_df['type'] == 'Yoga', "distance"] = np.NaN
    strava_df.loc[strava_df['type'] == 'RockClimbing', "distance"] = np.NaN

    key_cols = ['start_date_local',
                'type',
                'excel_time',
                'distance',
                'average_watts',
                'average_pace_run',
                'average_pace_swim',
                'calories',
                'average_heartrate'
                ]

    excel_df = strava_df[key_cols]
    return excel_df


def pandas_df_converter(excel_df):
    df = excel_df.copy()
    df['start_date_local'] = pd.to_datetime(df['start_date_local'])
    df = df.sort_values(by='start_date_local')
    df.reset_index(inplace=True)
    del df['index']
    df.loc[:, 'excel_time'] = df.loc[:, 'excel_time'] * 24
    return df


def graph_plots(pandas_df):
    fmt = mdates.DateFormatter('%b')
    filter_df = pandas_df.copy()
    start_year = min(filter_df['start_date_local']).year
    end_year = max(filter_df['start_date_local']).year
    year_array = list(range(start_year, end_year + 1, 1))
    graph_df = filter_df.copy()
    new_date_range = pd.date_range(start=(str(start_year) + "-01" + "-01"), end=(str(end_year) + "-12" + "-31"), freq="D")
    graph_df.set_index(["start_date_local"], inplace=True)
    # Graph 1: Duration
    everyday_series = graph_df.groupby([pd.Grouper(level='start_date_local', freq="D")])['excel_time'].sum()
    everyday_series = everyday_series.reindex(new_date_range, fill_value=0.00)
    everyday_series = everyday_series.groupby([pd.Grouper(level=0, freq="Y"), pd.Grouper(level=0, freq="D")]).sum()
    plt.figure()
    axs = everyday_series.groupby(level=0).cumsum().groupby([pd.Grouper(level=0, freq="Y")]).plot(legend=True, use_index=True)
    for ax in axs:
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(fmt)
        ax.set_title("Cumulative duration plot (by year)")
        ax.set_ylabel("Hours")
    plt.show()

    # Graph 2: Days of ex
    numdays_series = graph_df.reset_index().groupby([pd.Grouper(key="start_date_local", freq="D")])['start_date_local'].nunique()
    numdays_series = numdays_series.reindex(new_date_range, fill_value=0.00)
    numdays_series = numdays_series.groupby([pd.Grouper(level=0, freq="Y"), pd.Grouper(level=0, freq="D")]).sum()
    plt.figure()
    axs = numdays_series.groupby(level=0).cumsum().groupby([pd.Grouper(level=0, freq="Y")]).plot(legend=True, use_index=True)
    for ax in axs:
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(fmt)
        ax.set_title("Cumulative days of exercise (by year)")
        ax.set_ylabel("Days")
    plt.show()

    # Graph 3: Duration mean of the week
    everyday_series = graph_df.groupby([pd.Grouper(level='start_date_local', freq="D")])['excel_time'].sum()
    everyday_series = everyday_series.reindex(new_date_range, fill_value=0.00)
    plt.figure()
    ax = everyday_series.groupby([everyday_series.index.day_of_week]).mean().plot.bar(legend=False, use_index=True)
    ax.set_xticklabels(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])
    ax.set_title("Average duration by day of the week")
    ax.set_ylabel("Hours")
    plt.show()

    # Graph 4:  Duration mean of the week,BY YEAR
    ax = everyday_series.groupby([pd.Grouper(level=0, freq="Y"), everyday_series.index.day_of_week]).mean().unstack().plot.bar(legend=False, use_index=True)
    ax.set_xticklabels(year_array)
    ax.set_title("Average duration by day of the week, by year")
    ax.set_ylabel("Hours")
    plt.show()

    # Graph 5:  Days of ex of the week
    numdays_series = graph_df.reset_index().groupby([pd.Grouper(key="start_date_local", freq="D")])['start_date_local'].nunique()
    numdays_series = numdays_series.reindex(new_date_range, fill_value=0.00)
    plt.figure()
    ax = numdays_series.groupby(everyday_series.index.day_of_week).sum().plot.bar(legend=False, use_index=True)
    ax.set_xticklabels(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])
    ax.set_title("Days exercised by day of the week")
    ax.set_ylabel("Days exercised")
    plt.show()

    # Graph 6:  Days of ex of the week, by year
    ax = numdays_series.groupby([pd.Grouper(level=0, freq="Y"), everyday_series.index.day_of_week]).sum().unstack().plot.bar(legend=False, use_index=True)
    ax.set_xticklabels(year_array)
    ax.set_title("Days exercised by day of the week, by year")
    ax.set_ylabel("Hours")
    plt.show()


def return_table_ls(df, freq_ls):
    day_of_year = int(dt.datetime.today().strftime("%j"))
    table_ls = []
    for item in freq_ls:
        freq_str = item
        if freq_str == "Y":
            table_ls.append(create_table(df, freq_str))
            df = df[df['start_date_local'].dt.dayofyear <= day_of_year]
            table_ls.append(create_table(df, freq_str))
        else:
            table_ls.append(create_table(df, freq_str))
    return table_ls


def create_table(df, freq_str):
    duration_by_type = df.groupby([pd.Grouper(key='start_date_local', freq=freq_str), 'type'])['excel_time'].sum()
    duration_by_type.name = "duration"
    num_of_ex_by_type = df.groupby([pd.Grouper(key='start_date_local', freq=freq_str), 'type'])['excel_time'].count()
    num_of_ex_by_type.name = "number_of_ex"
    days_of_ex_by_type = df.groupby([pd.Grouper(key='start_date_local', freq=freq_str), 'type'])['start_date_local'].nunique()
    days_of_ex_by_type.name = "days_of_ex"
    # columns for all act
    duration_all = df.groupby([pd.Grouper(key='start_date_local', freq=freq_str)])['excel_time'].sum()
    duration_all.name = "duration"
    num_of_ex_all = df.groupby([pd.Grouper(key='start_date_local', freq=freq_str)])['excel_time'].count()
    num_of_ex_all.name = "number_of_ex"
    days_of_ex_all = df.groupby([pd.Grouper(key='start_date_local', freq=freq_str)])['start_date_local'].nunique()
    days_of_ex_all.name = "days_of_ex"
    # create data frame
    type_df = pd.concat([duration_by_type, num_of_ex_by_type, days_of_ex_by_type], axis=1, join="outer", ignore_index=False)
    all_df = pd.concat([return_data_frame_all(duration_all), return_data_frame_all(num_of_ex_all), return_data_frame_all(days_of_ex_all)], axis=1, join="outer", ignore_index=False)
    table_df = pd.concat([type_df, all_df], join='outer').sort_index()
    return table_df


def return_data_frame_all(series):
    df = series.to_frame()
    df['type'] = 'All'
    return df.reset_index().set_index(['start_date_local', 'type'])
