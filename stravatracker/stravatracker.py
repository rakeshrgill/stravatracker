""" Contains main code for program
program()
    read_json()
    setup() - imported
    write_json()
    update_write()
    load_files()
    main_menu()

main_menu()
    check_last_timeout() - imported
    update_write()
    analysis()

analysis()
    excel_clean() - imported
    pandas_df_converter() - imported
    table_analysis()
    graph_plots() - imported

table_analysis()
update_write()
    strava_update() - imported
    write_json()
load_files()
read_json()
write_json()
"""
# TODO: Build test for update.py
# TODO: Build Test for stravatracker.py
# TODO: Build integration test for stravatracker and update.py
import os
import json

import pandas as pd

from update import strava_update, check_last_timeout
from first_run import setup
from analysis import excel_clean, pandas_df_converter, graph_plots, return_table_ls

__author__ = "rakeshrgill"
__email__ = "rakeshrgill@gmail.com"


def program():
    """Loads files, initiates setup and runs the main menu function
    program()
        read_json()
        setup() - imported
        write_json()
        update_write()
        load_files()
    main_menu()
    """
    # Universal Variables
    program = True
    # Initialise Files
    while program:
        # Needs to reload config and df files each loop
        # Load config file
        try:
            path = os.path.join('data', 'config.json')
            config = read_json(path)
        except (FileNotFoundError, ValueError):
            # First Run Trigger
            # Ensures that df file and config files exist
            ask_question = True
            while ask_question:
                answer = input("Would you like to setup the program \n1. Yes\n2. No\n")
                if answer == '1':
                    ask_question = False
                    try:
                        config = setup()
                    except SystemExit:
                        print("Config file setup was quit. Program will exit")
                        raise SystemExit(0)
                    else:
                        path = os.path.join('data', 'config.json')
                        write_json(config, path)
                        df = None
                        print("Running initial database setup")
                        update_write(config, df)
                        print("Initial database update complete")
                elif answer == '2':
                    ask_question = False
                    print("Config file does not exist. Program will exit")
                    raise SystemExit(0)
                else:
                    print("Type a valid answer")
        # Load df files, config file is as per setup()
        try:
            df = load_files(config)
        except FileNotFoundError:
            print("Error loading csv file. Program will exit")
            raise SystemExit(0)
        else:
            program = main_menu(config, df)


def main_menu(config, df):
    """ Main menu with 3 options: 1. Update Database, 2. Database Analysis, 3. Exit
    check_last_timeout() - imported
    update_write()
    analysis()

    Parameters
    ----------
    config : dict
        config variables (see read_json())
    df : pandas.DataFrame
        contains activities downloaded from strava, with segments dropped (see get_new_activities())

    Returns
    -------
    bool
        continue program loop?
    """
    ask_question = True
    while ask_question:
        # Display current status
        print("Database last updated on: {}".format(config['last_update']))
        print("Total activities: {}".format(df.shape[0]))
        # Check for remaining updates
        if config['remaining_updates'] is True:
            print("You still have activites to update from the previous, please choose option 1 when prompted.")
        else:
            pass
        answer = input("What would you like to do?\n1. Update Database\n2. Database Analysis\n3. Exit\n")
        # Check if the API has timed out
        can_update = check_last_timeout(config)
        if can_update:
            pass
        else:
            print("Please wait before updating again; option 1 is disabled")
        # Process answer
        if answer == '1':
            if can_update:
                update_write(config, df)
                ask_question = False
                return True
            else:
                print("Option disabled")
        elif answer == '2':
            analysis(config, df)
            ask_question = False
            return True
        elif answer == '3':
            print("Exit")
            ask_question = False
            return False
        else:
            print("Invalid Answer")


def analysis(config, df):
    """Calls analysis functions, saves excel csv to disk
    excel_clean() - imported
    pandas_df_converter() - imported
    table_analysis()
    graph_plots() - imported

    Parameters
    ----------
    config : dict
        config variables (see read_json())
    df : pandas.DataFrame
        contains activities downloaded from strava, with segments dropped (see get_new_activities())
    """
    print("Running Analysis")
    # Output to Excel
    excel_df = excel_clean(df)
    excel_df.to_csv('data' + '/' + r'excel_all_activities_{}.csv'.format(config['last_update']), index=False)
    # Pandas df conversion
    pandas_df = pandas_df_converter(excel_df)
    # Output to tables
    table_analysis(config, pandas_df)
    # Output to graphs
    graph_plots(pandas_df)
    print("Analysis Completed")


def table_analysis(config, pandas_df):
    """ Takes in database, runs conversion to tables, saves it as a CSV

    Parameters
    ----------
    config : dict
        config variables (see read_json())
    pandas_df : pandas.DataFrame
        converted database (see pandas_df_converter())
    """
    df = pandas_df.copy()
    # table making
    freq_ls = ["Y", "M"]
    table_ls = return_table_ls(df, freq_ls)
    # table saving
    table_ls[0].to_csv('data' + '/' + r'yearlytable_{}.csv'.format(config['last_update']), index=True)
    table_ls[1].to_csv('data' + '/' + r'yearly_todate_table_{}.csv'.format(config['last_update']), index=True)
    table_ls[2].to_csv('data' + '/' + r'monthly_table_{}.csv'.format(config['last_update']), index=True)
    table_ls[2].reset_index().pivot(index='start_date_local', columns='type', values=['duration', 'number_of_ex', 'days_of_ex']).reorder_levels(axis=1, order=[1, 0]).sort_index(axis=1, level=[0, 1], ascending=True, inplace=False).to_csv('data' + '/' + r'monthly_table_pivot_{}.csv'.format(config['last_update']), index=True)


def update_write(config, df):
    """Updates database and writes output to file

    Parameters
    ----------
    config : dict
        config variables (see read_json())
    df : pandas.DataFrame
        contains activities downloaded from strava, with segments dropped (see get_new_activities())
    """
    print("Updating database")
    config, df = strava_update(config, df)
    path = os.path.join('data', 'config.json')
    write_json(config, path)
    df.to_csv('data' + '/' + r'strava_activities_{}.csv'.format(config['last_update']), index=False)
    print("Databse and config written to disk")


def load_files(config):
    """Loads dataframe file from csv

    Parameters
    ----------
    config : dict
        config variables (see read_json())

    Returns
    -------
    df : pandas.DataFrame
        contains activities downloaded from strava, with segments dropped (see get_new_activities())

    Raises
    ------
    FileNotFoundError
        Missing csv file or directory
    """
    if os.path.exists('data'):
        try:
            df = pd.read_csv(os.path.join('data', r'strava_activities_{}.csv'.format(config['last_update'])))
            return df
        except FileNotFoundError:
            print("strava_activities missing")
            raise FileNotFoundError("Missing File")
    else:
        print("Folder not Found")
        raise FileNotFoundError("Folder not found")
        df = None
        return df


def read_json(path):
    """Reads the config.json file and returns a dictionary

    Parameters
    ----------
    path : pathname
        location of JSON file

    Returns
    -------
    dict
        config = {
            'first_run' : False,
            'last_update' : '2022_07_15_1344',
            'last_timeout_daily' : '2022_07_14_1413',
            'last_timeout_15min' : '2022_07_15_1400',
            'remaining_updates' : False,
            'client_id': '',
            'client_secret': '',
            'refresh_token': ''
        }

    Raises
    ------
    ValueError
        Config dictionary is in the wrong format
    """
    ls_keys = ['first_run', 'last_update',
               'last_timeout_daily', 'last_timeout_15min', 'remaining_updates',
               'client_id', 'client_secret', 'refresh_token']
    try:
        with open(path, 'r') as jsonfile:
            data = json.load(jsonfile)
            # print("Read successful")
            jsonfile.close()
        # Check if all the fields are present
        if all(key in data for key in ls_keys) and all(key in ls_keys for key in data):
            return data
        else:
            raise ValueError("Config file in invalid format")
    except FileNotFoundError:
        print("Config file not found")
        raise


def write_json(config, path):
    """Writes config.json using config dictionary

    Parameters
    ----------
    config : dict
        config variables (see read_json())
    path : pathname
        location of JSON file

    Raises
    ------
    ValueError
        Config dictionary is in the wrong format
    """
    ls_keys = ['first_run', 'last_update',
               'last_timeout_daily', 'last_timeout_15min', 'remaining_updates',
               'client_id', 'client_secret', 'refresh_token']
    if all(key in config for key in ls_keys) and all(key in ls_keys for key in config):
        with open(path, 'w') as jsonfile:
            json.dump(config, jsonfile)  # Writing to the file
            # print("Write successful")
        jsonfile.close()
    else:
        raise ValueError("Config dictionary is in invalid format")


# TODO: Check for requirements
program()
