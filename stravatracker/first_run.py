""" Defines functions for initial setup
Contains the following functions:
    setup()

"""
import urllib3
import webbrowser

import requests
from PIL import Image

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

__author__ = "rakeshrgill"
__email__ = "rakeshrgill@gmail.com"


def setup():
    """Runs steps 0 to 4

    Returns
    -------
    dict
        config file

    Raises
    ------
    SystemExit
        If the setup was terminated by user

    """
    # Step 0
    # Create config file
    print("Running initial database setup.")
    config = {
        'first_run': True,
        'last_update': '2022_01_01_1200',
        'last_timeout_daily': '2022_01_01_1200',
        'last_timeout_15min': '2022_01_01_1200',
        'remaining_updates': False,
        'client_id': '',
        'client_secret': '',
        'refresh_token': ''
    }
    # Read the image
    im1a = Image.open("setup/step1a.png")
    im1b = Image.open("setup/step1b.png")
    im2 = Image.open("setup/step2.png")
    # Step 1 Create Strava API on Webbrowser
    ask_question = True
    while ask_question:
        print("Step 1: Login to Strava and create an application. Fill in the fields per the instructions and upload an image as the logo")
        webbrowser.open("https://www.strava.com/settings/api", new=1)
        im1a.show()
        im1b.show()
        answer = input("What would you like to do?\n1. Next Step\n2. Repeat\n3. Exit\n")
        if answer == '1':
            ask_question = False
        elif answer == '2':
            ask_question = True
        elif answer == '3':
            ask_question = False
            raise SystemExit(0)
        else:
            print("Invalid Answer")
    # Step 2 Get Client ID and Client Secret from Webbrowser
    ask_question = True
    while ask_question:
        print("Step 2: Key in client id and client secret when prompted")
        webbrowser.open("https://www.strava.com/settings/api", new=1)
        im2.show()
        client_id_question = True
        while client_id_question:
            client_id = input("Please key in the client id shown on the site\n")
            # Check for empty
            if client_id == '':
                print("Empty client id")
                client_id_question = (True)
            else:
                client_id_question = False
                pass
            # Check for integer
            try:
                client_id = int(client_id)
            except ValueError:
                print("Not an Integer")
                client_id_question = (True)
                pass
            else:
                client_id_question = (client_id_question and False)

        client_secret_question = True
        while client_secret_question:
            client_secret = input("Please key in the client secret shown on the site\n")
            # Check for empty
            if client_secret == '':
                print("Empty client id")
                client_secret_question = (True)
            else:
                client_secret_question = (False)
                pass
        config['client_id'] = str(client_id)
        config['client_secret'] = str(client_secret)
        print("Client id and client secret saved.")
        answer = input("What would you like to do?\n1. Next Step\n2. Repeat\n3. Exit\n")
        if answer == '1':
            ask_question = False
        elif answer == '2':
            ask_question = True
        elif answer == '3':
            ask_question = False
            raise SystemExit(0)
        else:
            print("Invalid Answer")
    # Step 3 Obtain code from Webbrowser
    ask_question = True
    while ask_question:
        print("Step 3: Authenticate the application. The page will redirect to a new url in the format\nhttp://localhost/?state=&code=this_code_here&scope=read,activity:read_all.\nInput this code into the program.")
        url = "https://www.strava.com/oauth/authorize?client_id={}&redirect_uri=http://localhost&response_type=code&scope=activity:read_all".format(config['client_id'])
        webbrowser.open(url, new=1)
        code_question = True
        while code_question:
            code = input("Please key in the code in the redirect url\n")
            # Check for empty
            if code == '':
                print("Empty client id")
                code_question = (True)
            else:
                code_question = (False)
                pass
        print("Code saved.")
        answer = input("What would you like to do?\n1. Next Step\n2. Repeat\n3. Exit\n")
        if answer == '1':
            ask_question = False
        elif answer == '2':
            ask_question = True
        elif answer == '3':
            ask_question = False
            raise SystemExit(0)
        else:
            print("Invalid Answer")
    # Step 4 Obtain Client Refresh Code
    ask_question = True
    while ask_question:
        print("Requesting request token with read_all authorisation")
        payload = {
            'client_id': config['client_id'],
            'client_secret': config['client_secret'],
            'code': code,
            'grant_type': 'authorization_code',
            'f': 'json'
        }
        url = 'https://www.strava.com/oauth/token'
        # Obtain Token for API
        print("Requesting Refresh Token...\n")
        response = requests.post(url, data=payload, verify=False)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError:
            # Whoops it wasn't a 200
            # raise error into strava update
            print("Error in request_headers occured")
            raise
        else:
            # if there is no error
            refresh_token = response.json()['refresh_token']
            print("Refresh Token = {}\n".format(refresh_token))
            config['refresh_token'] = refresh_token
        answer = input("What would you like to do?\n1. Next Step and Exit \n2. Repeat\n")
        if answer == '1':
            ask_question = False
        elif answer == '2':
            ask_question = True
        else:
            print("Invalid Answer")
    return config
