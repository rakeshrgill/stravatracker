""" Defines functions to update the strava database file
Contains the following classes:
TimeoutFifteen(Exception)
TimeoutDaily(Exception)

Contains the following functions:
    check_last_timeout()
    strava_update()
        request_headers()
        create_id_list()
            return_json()
        get_new_activities()
            return_json()
"""
import datetime as dt
import urllib3

import requests
import pandas as pd

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

__author__ = "rakeshrgill"
__email__ = "rakeshrgill@gmail.com"


class TimeoutFifteen(Exception):
    """Used to indicate a 15 minute timeout has occured"""
    pass


class TimeoutDaily(Exception):
    """Used to indicate a daily timeout has occured"""
    pass


def check_last_timeout(config):
    """ Checks if sufficient time has passed since the last timeout and returns a boolean. Prints error message when there is a need to wait

    Parameters
    ----------
    config : dict
        config variables (see read_json())
        config['last_timeout_daily']
        config['last_timeout_15min']

    Returns
    -------
    bool
        True if can update
    """
    utc_time = dt.datetime.utcnow()
    timeout_daily = dt.datetime.strptime(config['last_timeout_daily'], '%Y_%m_%d_%H%M')
    timeout_15min = dt.datetime.strptime(config['last_timeout_15min'], '%Y_%m_%d_%H%M')
    # Check Daily
    if utc_time.year == timeout_daily.year and utc_time.month == timeout_daily.month and utc_time.day == timeout_daily.day:
        print("Please wait for {} hours and {} minutes before updating again".format(23 - utc_time.hour, 60 - utc_time.minute))
        return False
    else:
        pass
    # Check 15 Min
    if utc_time.year == timeout_15min.year and utc_time.month == timeout_15min.month and utc_time.day == timeout_15min.day and utc_time.hour == timeout_15min.hour and (timeout_15min.minute // 15) == (utc_time.minute // 15):
        print("Please wait for {} minutes before updating again".format(15 - (utc_time.minute % 15)))
        return False
    else:
        pass
    return True


def strava_update(config, df):
    """Calls request_headers(), create_id_list() and get_new_activities()
    Handles errors and timeouts. Updates:
        config['last_timeout_daily']
        config['last_timeout_15min'
    Preconditions: sufficient time has passed since last timeout, config file contains necessary info

    Parameters
    ----------
    config : dict
        config variables (see read_json())
    df : pandas.DataFrame
        contains activities downloaded from strava, with segments dropped (see get_new_activities())

    Returns
    -------
    config, df
        dict, pandas.Dataframe
    """
    # Attempt to get headers and id
    try:
        headers = request_headers(config)
        id_list, config = create_id_list(headers, config, df)
    except TimeoutDaily:
        config['last_timeout_daily'] = dt.datetime.utcnow().strftime('%Y_%m_%d_%H%M')
        print("Activity List cannot be fetched.")
    except TimeoutFifteen:
        config['last_timeout_15min'] = dt.datetime.utcnow().strftime('%Y_%m_%d_%H%M')
        print("Activity List cannot be fetched.")
    except requests.exceptions.HTTPError:
        # An error occured in generating the prereq files, update cannot run
        print("Headers cannot be fetched.")
    else:
        # no errors, let's try to update
        if id_list != []:
            config, df = get_new_activities(headers, config, df, id_list)
            # config and df are updated by function regardless
        else:
            print("No new activities")
    finally:
        # always return config and df
        return config, df


def request_headers(config):
    """Requests and formats header for future API requests

    Parameters
    ---------
    config: dict
        config['client_id']
        config['client_secret']
        config['refresh_token']

    Returns
    ---------
    dict
        headers to be used to get data
    """
    # Initialise Variables
    payload = {
        'client_id': config['client_id'],
        'client_secret': config['client_secret'],
        'refresh_token': config['refresh_token'],
        'grant_type': 'refresh_token',
        'f': 'json'
    }
    url = 'https://www.strava.com/oauth/token'
    # Obtain Token for API
    print("Requesting Token...\n")
    response = requests.post(url, data=payload, verify=False)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError:
        # Whoops it wasn't a 200
        print("Error in request_headers occured")
        raise
    else:
        access_token = response.json()['access_token']
        print("Access Token = {}\n".format(access_token))
        headers = {'Authorization': 'Bearer ' + access_token}
        return headers


def create_id_list(headers, config, df):
    """Fetches list of ids to update

    Parameters
    ----------
    headers : dict
        see request_headers()
    config : dict
        config variables (see read_json())
    df : pandas.DataFrame
        contains activities downloaded from strava, with segments dropped (see get_new_activities())

    Returns
    -------
    list, dict
        id_list, config
    """
    # Extract all activities from atheletes profile
    print("Requesting id list from Strava")
    url = 'https://www.strava.com/api/v3/athlete/activities'
    page = 1
    json_obj_ls = []
    json_obj = 'blank'
    while json_obj != []:
        try:
            params = {'per_page': 200, 'page': page}
            json_obj = return_json(url, headers, params)
        except TimeoutDaily:
            print("Error in create_id_list occured.")
            raise
        except TimeoutFifteen:
            print("Error in create_id_list occured.")
            raise
        else:
            json_obj_ls.extend(json_obj)
            print("page number: {}".format(page))
            page += 1
    print("total pages: {}".format(page))
    # Convert the list of json into a pandas dataframe
    df_allactivities = pd.json_normalize(json_obj_ls)
    df_allactivities = df_allactivities.set_index('id')
    # Compare list of activities with saved activities to create list of activities to update
    if config['first_run'] is False:
        id_list = df_allactivities[~df_allactivities.index.isin(df['id'])].index.to_list()
        print("Activites to update: {}".format(len(id_list)))
        strava_id = df_allactivities.index.to_list()
        database_id = df['id'].to_list()
        extra_id_list = list(set(database_id) - set(strava_id))
        if len(extra_id_list) > 0:
            print("There are activities on local which do not exists in strava")
            print(extra_id_list)
        else:
            print("All activities on local exist on strava")
    else:
        id_list = df_allactivities .index.to_list()
    return id_list, config


def get_new_activities(headers, config, df, id_list):
    """Pulls data from strava based on id_list, updates df and config.
    Config and df must always be updated by the function.
    Preconditions: Takes in non empty id_list

    Parameters
    ----------
    headers : dict
        see request_headers()
    config : _type_
        config variables (see read_json())
    df : pandas.DataFrame
        Per format in function
    id_list : list
        see create_id_list()

    Returns
    -------
    dict, df
        config['remaining_updates']
        config['first_run']
        config['last_update']
        config['last_timeout_15min']
        config['last_timeout_daily']

    """
    activity_url = 'https://www.strava.com/api/v3/activities'
    urls = []
    for num in id_list:
        activityid = str(num)
        urls.append(activity_url + '/' + activityid)
    params = None
    json_obj_ls = []
    # Update database
    print("Fetching new activities")
    for url in urls:
        # Update till a timeout occurs
        try:
            json_obj = return_json(url, headers, params)
        except TimeoutDaily:
            print("Timeout in get_new_activities occured(TimeoutDaily)")
            config['last_timeout_daily'] = dt.datetime.utcnow().strftime('%Y_%m_%d_%H%M')
            break
        except TimeoutFifteen:
            print("Timeout in get_new_activities occured(Timeout15)")
            config['last_timeout_15min'] = dt.datetime.utcnow().strftime('%Y_%m_%d_%H%M')
            break
        else:
            # if there is no error
            json_obj_ls.append(json_obj)
    config['last_update'] = dt.datetime.today().strftime("%Y_%m_%d_%H%M")
    config['first_run'] = False
    if json_obj_ls == []:
        # nothing happened, do not update the df file
        print("No updates fetched from id_list")
        return config, df
    else:
        df_newactivities = pd.json_normalize(json_obj_ls)
    # Combine with old df
    if config['first_run'] is True:
        df = df_newactivities
    else:
        df = pd.concat([df, df_newactivities], ignore_index=True)
    # Sort and format
    df.sort_values('id', ascending=False, inplace=True)
    df = df.reset_index().drop(columns=['index', 'segment_efforts'])
    # Check for remaining updates
    new_id_list = df['id'].to_list()
    remainder_id_list = list(set(id_list) - set(new_id_list))
    if remainder_id_list != []:
        config['remaining_updates'] = True
    else:
        config['remaining_updates'] = False
    print("Database updated")
    return config, df


def return_json(url, headers, params):
    """creates a request and returns json and config

    Parameters
    ----------
    url : str
        per requests library
    headers : dict
        per requests library
    params : dict
        per requests library

    Returns
    -------
    dict
        json_obj

    Raises
    ------
    TimeoutDaily
        Daily timeout
    TimeoutFifteen
        Fifteen minute timeout
    """
    response = requests.get(url, headers=headers, params=params)
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        # Whoops it wasn't a 200
        limit_15min = int(response.headers['X-RateLimit-Limit'].split(",")[0])
        limit_daily = int(response.headers['X-RateLimit-Limit'].split(",")[1])
        usage_15min = int(response.headers['X-RateLimit-Usage'].split(",")[0])
        usage_daily = int(response.headers['X-RateLimit-Usage'].split(",")[1])
        print("Error: " + str(e))
        # Check error and modify config last_timeout
        if usage_daily >= limit_daily:
            print("Daily Limit Hit")
            raise TimeoutDaily
        if usage_15min >= limit_15min:
            print("15 Minute Limit Hit")
            raise TimeoutFifteen
    else:
        # if there is no error
        json_obj = response.json()
        return json_obj
