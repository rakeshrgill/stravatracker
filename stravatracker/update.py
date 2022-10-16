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
Created: 2022/07/10
"""

import datetime as dt
import requests
import urllib3
import pandas as pd
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# TODO: ensure config, df are returned in request, create and get new

# def test():
#    print("test")


def check_last_timeout(config):
    """ Checks if sufficient time has passed since the last timeout and returns a boolean. Prints error message when there is a need to wait

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
    """
    Preconditions: sufficient time has passed since last timeout, config file contains necessary info
    Updates config and df file
    Write df file to disk
    Parameters
    ----------
    config: dictionary
        dictionary containing the config variables (see read_config)
    df: pandas dataframe
        contains activities downloaded from strava, with segments dropped

    Returns
    -------
    config: dictionary
        dictionary containing the config variables (see read_config)
    df: pandas dataframe
        contains activities downloaded from strava, with segments dropped

    Raises
    ------

    """

    # Attempt to get headers and id
    try:
        try:
            headers = request_headers(config)
            id_list, config = create_id_list(headers, config, df)
        except Exception as e:
            # An error occured in generating the prereq files, update cannot run
            print("Update cannot be run")
            raise e
        # Attempt to update file
        try:
            if id_list != []:
                print("Updating database")
                config, df = get_new_activities(headers, config, df, id_list)
                # config and df are updated by function regardless
            else:
                print("No new activities")
        except RuntimeError as e:
            # TODO: Fix error handling
            print("df was not updated")
            raise e
    except Exception:
        print("Update Failed")

    else:
        print("Update was successful")
    finally:
        return config, df


def request_headers(config):
    """
    Requests and formats header for future API requests
    Parameters
    ---------
    config: dictionary
        config['client_id']
        config['client_secret']
        config['refresh_token']

    Returns
    ---------
    headers: dictionary
        request headers

    Raises
    ------
    requests.exceptions.HTTPError

    """

    payload = {
        'client_id': config['client_id'],
        'client_secret': config['client_secret'],
        'refresh_token': config['refresh_token'],
        'grant_type': 'refresh_token',
        'f': 'json'
    }

    # Initialise Variables
    url = 'https://www.strava.com/oauth/token'

    # Obtain Token for API
    print("Requesting Token...\n")
    response = requests.post(url, data=payload, verify=False)

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        # Whoops it wasn't a 200
        # raise error into strava update
        print("Error in request_headers occured")
        raise e
    else:
        # if there is no error
        access_token = response.json()['access_token']
        print("Access Token = {}\n".format(access_token))
        headers = {'Authorization': 'Bearer ' + access_token}
        return headers


def create_id_list(headers, config, df):
    """
    creates list of ids to update

    Parameters
    ---------
    headers: dictionary
        request header
    config: dictionary
        dictionary containing the config variables (see read_config)
    df: pandas dataframe
        contains activities downloaded from strava, with segments dropped

    Returns
    ---------
    id_list: list
        contains list of ids to update
    config: dictionary
        through return_json()
            config['last_timeout_daily']
            config['last_timeout_15min']

    Raises
    ------
    requests.exceptions.HTTPError
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
            json_obj, config = return_json(url, headers, params, config)
            # TEST: print("params is {}".format(params))
        except Exception:
            print("Error in create_id_list occured")
            # TODO: check if this makes sense raise e
            # raise error into strava update
            break
        else:
            # if there is no error
            json_obj_ls.extend(json_obj)
        finally:
            print("page number: {}".format(page))
            # TEST: safety stop
            if page > 9:
                print(params)
                print(json_obj)
            if page > 10:
                print(params)
                json_obj = []
            # END TEST
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
    """
    Preconditions: Takes in non empty id_list
    Updates config and df file
    Write df file to disk
    Parameters
    ----------
    headers: dictionary
        request headers
    config: dictionary
        dictionary containing the config variables (see read_config)
    df: pandas dataframe
        contains activities downloaded from strava, with segments dropped
    id_list: list
        contains list of ids to update

    Returns
    -------
    config: dictionary
        config['first_run']
        config['last_update']
        config['remaining_updates']
        through return_json()
            config['last_timeout_daily']
            config['last_timeout_15min']

    df: pandas dataframe
        contains activities downloaded from strava, with segments dropped

    Raises
    ------

    """

    activity_url = 'https://www.strava.com/api/v3/activities'
    urls = []
    for num in id_list:
        activityid = str(num)
        urls.append(activity_url + '/' + activityid)
    params = None
    json_obj_ls = []

    # TEST
    # urls = urls[0:1]

    for url in urls:
        try:
            json_obj, config = return_json(url, headers, params, config)
        except Exception:
            # if there is an error
            print("There is a an error in the request")
            break
        else:
            # if there is no error
            json_obj_ls.append(json_obj)
    if json_obj_ls == []:
        # do not update databse
        # nothing happened
        print("Empty list occured")
        time = dt.datetime.today().strftime("%Y_%m_%d_%H%M")
        config['last_update'] = time
        config['first_run'] = False
        return config, df

    else:
        df_newactivities = pd.json_normalize(json_obj_ls)
        print("New database created")

    # update and clean up df file
    if config['first_run'] is True:
        df = df_newactivities
    else:
        df = pd.concat([df, df_newactivities], ignore_index=True)
    df.sort_values('id', ascending=False, inplace=True)
    df = df.reset_index().drop(columns=['index', 'segment_efforts'])

    # check for remaining updates
    new_id_list = df['id'].to_list()
    remainder_id_list = list(set(id_list) - set(new_id_list))
    if remainder_id_list != []:
        config['remaining_updates'] = True
    else:
        config['remaining_updates'] = False

    # Update Config File (last update, first run)
    time = dt.datetime.today().strftime("%Y_%m_%d_%H%M")
    config['last_update'] = time
    config['first_run'] = False

    return config, df


def return_json(url, headers, params, config):
    """
    creates a request and returns json and config

    Parameters
    ---------
    url: url
        per requests library
    headers: headers
        per requests library
    params: params
        per request library
    config: dictionary
        config file from setup

    Returns
    ---------
    json_obj: dictionary
        json formatted object
    config: dictionary
        config['last_timeout_daily']
        config['last_timeout_15min']

    """

    response = requests.get(url, headers=headers, params=params)

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        # Whoops it wasn't a 200
        print("HTTPError")
        utc_timeout = dt.datetime.utcnow().strftime('%Y_%m_%d_%H%M')
        limit_15min = int(response.headers['X-RateLimit-Limit'].split(",")[0])
        limit_daily = int(response.headers['X-RateLimit-Limit'].split(",")[1])
        usage_15min = int(response.headers['X-RateLimit-Usage'].split(",")[0])
        usage_daily = int(response.headers['X-RateLimit-Usage'].split(",")[1])
        print("Error: " + str(e))
        # Check error and modify config last_timeout
        if usage_daily >= limit_daily:
            print("Daily Limit Hit")
            config['last_timeout_daily'] = utc_timeout
        else:
            pass
        if usage_15min >= limit_15min:
            print("15 Minute Limit Hit")
            config['last_timeout_15min'] = utc_timeout
        # Create empty JSON Object
        json_obj = None
        raise e
    except BaseException as e:
        print("Unknown error")
        raise e
    else:
        # if there is no error
        json_obj = response.json()
    finally:
        return json_obj, config
