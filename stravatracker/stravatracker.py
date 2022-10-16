""" Contains main code for program
Contains the following functions
- program
- main menu
- read_json
- write_json
- load_files

Author: rakeshrgill rakeshrgill@gmail.com
Created: 2022/07/04
"""
# TODO: Build test for update.py
# TODO: Build Test for stravatracker.py
# TODO: Build integration test for stravatracker and update.py
# TODO: Fix location of print statements
# TODO: Error handling
# TODO: Definitions for functions and modules

# Import files
import os
import pandas as pd
import json

from update import strava_update
from update import check_last_timeout
from first_run import setup
# from update import test
from analysis import excel_clean
from analysis import pandas_df_converter
from analysis import graph_plots
from analysis import return_table_ls


def read_json(path):
    """
    Reads the config.json file and returns a dictionary

    Parameters
    ----------

    Returns
    -------
    dictionary
        dictionary containing the config variables
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
    FileNotFoundError
        when the config file is misisng
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
            raise Exception("Config file in invalid format")
    except FileNotFoundError as e:
        print("Config file not found")
        raise e


def write_json(path, config):
    """
    Writes config.json using config dictionary

    Parameters
    ----------
    config: dictionary
        dictionary containing the config variables (see read_json)

    Returns
    -------

    Raises
    ------

    """
    # Config file checked at start and end
    ls_keys = ['first_run', 'last_update',
               'last_timeout_daily', 'last_timeout_15min', 'remaining_updates',
               'client_id', 'client_secret', 'refresh_token']
    if all(key in config for key in ls_keys) and all(key in ls_keys for key in config):
        with open(path, 'w') as jsonfile:
            json.dump(config, jsonfile)  # Writing to the file
            # print("Write successful")
        jsonfile.close()
    else:
        raise Exception("Config file in invalid format")


def load_files(config):
    """
    If first run is true:
        Returns either filenotfound OR
    If first run is not true:
        Returns the stored dataframe
        OR
        FileNotFound Error

    Parameters
    ----------
    config: dictionary
        dictionary containing the config variables (see read_json)

    Returns
    ------
    df: pandas dataframe
        contains activities downloaded from strava, with segments dropped

    Raises
    ------
    FileNotFoundError
        Either 'data' Folder is missing OR
        Files are missing
    """

    # only affects config and reading, not writing to df

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


def update_write(config, df):
    # TODO: Fill in defintion
    print("Updating database")
    config, df = strava_update(config, df)
    path = os.path.join('data', 'config.json')
    write_json(path, config)
    df.to_csv('data' + '/' + r'strava_activities_{}.csv'.format(config['last_update']), index=False)
    print("Update completed and written to disk")


def analysis(config, df):
    print("Running Analysis")
    # Output to Excel
    excel_df = excel_clean(df)
    excel_df.to_csv('data' + '/' + r'excel_all_activities_{}.csv'.format(config['last_update']), index=False)
    # Pandas df conversion
    pandas_df = pandas_df_converter(excel_df)
    # Output to tables
    table_analysis(pandas_df, config)
    # Output to graphs
    graph_plots(pandas_df)
    print("Analysis Completed")


def table_analysis(pandas_df, config):
    df = pandas_df.copy()
    # table making
    freq_ls = ["Y", "M"]
    table_ls = return_table_ls(df, freq_ls)
    # table saving
    table_ls[0].to_csv('data' + '/' + r'yearlytable_{}.csv'.format(config['last_update']), index=True)
    table_ls[1].to_csv('data' + '/' + r'yearly_todate_table_{}.csv'.format(config['last_update']), index=True)
    table_ls[2].to_csv('data' + '/' + r'monthly_table_{}.csv'.format(config['last_update']), index=True)
    table_ls[2].reset_index().pivot(index='start_date_local', columns='type', values=['duration', 'number_of_ex', 'days_of_ex']).reorder_levels(axis=1, order=[1, 0]).sort_index(axis=1, level=[0, 1], ascending=True, inplace=False).to_csv('data' + '/' + r'monthly_table_pivot_{}.csv'.format(config['last_update']), index=True)


def main_menu(df, config):
    """
    Takes in files
    Offers user the following options
    1. Update Database, export to Excel and shutdown
    2. Update Database
    3. Database Analysis
    4. Exit

    Parameters
    ----------
    df: pandas dataframe
        contains activities downloaded from strava, with segments dropped
    config: dictionary
        dictionary containing the config variables (see read_json)

    Returns
    -------

    Raises
    ------

    """

    ask_question = True
    while ask_question:
        # Display current status
        print("Database last updated on: {}".format(config['last_update']))
        print("Total activities: {}".format(df.shape[0]))

        # Check for remaining updates
        if config['remaining_updates'] is True:
            print("You still have activites to update from the previous, please choose option 2 when prompted.")
        else:
            pass

        # Check if the API has timed out
        can_update = check_last_timeout(config)
        if can_update:
            pass
        else:
            print("Please wait before updating again; option 1 and 2 are disabled")

        answer = input("What would you like to do?\n1. Update Database, export to Excel and shutdown\n2. Update Database\n3. Database Analysis\n4. Exit\n")
        if answer == '1':
            if can_update:
                print("please wait before updating again; option 1 and 2 are disabled")
                update_write(config, df)
                ask_question = False
                return False
            else:
                print("Option disabled")
        elif answer == '2':
            if can_update:
                update_write(config, df)
                ask_question = False
                return True
            else:
                print("Option disabled")
        elif answer == '3':
            analysis(config, df)
            ask_question = False
            return True
        elif answer == '4':
            print("Exit")
            ask_question = False
            return False
        else:
            print("Invalid Answer")


def program():
    """
    main program loop
    Loads files and runs the main_menu function

    Parameters
    ----------

    Returns
    -------


    ------

    """

    # Universal Variables
    program = True

    # Initialise Files
    while program:
        # Needs to reload config and df files regularly
        # Load config file
        try:
            path = os.path.join('data', 'config.json')
            config = read_json(path)
        except FileNotFoundError:
            # First Run Trigger
            ask_question = True
            while ask_question:
                answer = input("Would you like to setup the program \n1. Yes\n2. No")
                if answer == '1':
                    ask_question = False
                    try:
                        config = setup()
                    except Exception:
                        print("Config file setup failed. Program will exit")
                        exit()
                    else:
                        path = os.path.join('data', 'config.json')
                        write_json(path, config)
                        df = None
                        print("Running initial database setup")
                        update_write(config, df)
                        print("Initial database update complete")
                elif answer == '2':
                    ask_question = False
                    print("Config file does not exist. Program will exit")
                    exit()
                else:
                    print("Type a valid answer")

        # Load df files
        try:
            df = load_files(config)
        except Exception:
            print("Error loading files")
            program = False
            break

        program = main_menu(df, config)


# TODO: Check for requirements
program()
