#!/usr/bin/env python3

'''
This file contains code to facilitate data mining from the command line. The
user calls this file from the command line, specifying which data types and
optionally giving a date range for which to collect data. This file takes care
of getting necessary user input and then feeding that into the corresponding
functions for each data type.
'''

import pepco, gshp, solar, weather
import sqlite3, datetime, json
from sys import argv


def get(conn, which, saved_input=None, file_input=None, start=None, end=None):
    """
    Handles the process of mining data.

    :param conn: An sqlite3 database connection
    :param which: A string containing the letters g, s, p, and/or w to indicate
    the Ground Source Heat Pump, Solar, Pepco, and Weather data respectively
    :param saved_input: An optional string specifying an input entry previously 
    saved in the database
    :param file_input: An optional string specifying a JSON file containing the 
    users input
    :param start: The start of the time span to collect data on
    :param end: The end of the time span to collect data on
    """
    # Get input before starting to mine
    opts = get_input(conn, which, saved_input, file_input)

    # Start mining
    if "p" in which:
        if start or end: print("Pepco ignores dates for now")
        get_pepco(conn, opts["pepco"])
    if "s" in which:
        get_solar(conn, start, end, opts["solar"])
    if "w" in which:
        get_weather(conn, start, end, opts["weather"])
    if "g" in which:
        get_gshp(conn, start, end, opts["gshp"])


def get_input(conn, which, saved_input=None, file_input=None):
    """
    Handles getting all necessary user input before mining data.

    :param conn: An sqlite3 database connection
    :param which: A string containing the letters g, s, p, and/or w to indicate
    the Ground Source Heat Pump, Solar, Pepco, and Weather data respectively
    :param saved_input: An optional string specifying an input entry previously 
    saved in the database
    :param file_input: An optional string specifying a JSON file containing the 
    users input
    """
    cur = conn.cursor()

    # Check if the saved inputs table is in the database
    cur.execute("""SELECT name FROM sqlite_master 
        WHERE type='table' and name='saved_inputs'""")
    if not cur.fetchall(): # Table isn't in database
        cur.execute("CREATE TABLE saved_inputs(name TEXT PRIMARY KEY, input TEXT)")
        conn.commit()
        loadable=False
    else: # Table is in database
        cur.execute("SELECT name FROM saved_inputs")
        saved = cur.fetchall()
        loadable = len(saved) > 0 # Check if empty

    # If the user supplied a file
    if file_input:
        with open(file_input, 'r') as f:
            opts = json.load(f)
        changed = True
    else: # Otherwise check the database
        if loadable and not saved_input:
            load = yes_or_no("Load from saved inputs")
        elif loadable and saved_input:
            load = True
        else:
            load = False

        if load: # Load from saved input
            if not saved_input:
                cur.execute("SELECT name FROM saved_inputs")
                saved = cur.fetchall()

                print([s[0] for s in saved])
                saved_input = input("Which of these inputs would you like to use: ")
                if not saved_input: return get_input(conn, which, saved_input) # Restart if they don't answer
                
            cur.execute("SELECT name, input FROM saved_inputs WHERE name=(?)", (saved_input,))
            opts = json.loads(cur.fetchone()[1])
        else: # Get the users input
            opts = {}
        
        changed = False

    # Make sure we have all the input we need.
    if "p" in which and "pepco" not in opts:
        opts["pepco"] = pepco_input()
        changed = True
    if "s" in which and "solar" not in opts:
        opts["solar"] = solar_input()
        changed = True
    if "w" in which and "weather" not in opts:
        opts["weather"] = weather_input()
        changed = True
    if "g" in which and "gshp" not in opts:
        opts["gshp"] = gshp_input()
        changed = True

    # Offer to save the options
    if changed:
        print(" -- Save Input -- ")
        save = yes_or_no("Save this input? (info will be stored on disk)")
        if save:
            name = input("Enter a name for this input: ")
            cur.execute("""INSERT OR REPLACE INTO saved_inputs(name, input) 
                VALUES(?,?)""", (name, json.dumps(opts)))
            conn.commit()

    return opts


def pepco_input():
    '''
    Prompts the user for all inputs necessary for mining the Pepco data.
    '''
    print(" -- Pepco Input -- ")
    i = "placeholder"
    files = []
    print("Enter the locations of Pepco Green Button data files and hit return when you're done")
    while not files or i:
        i = input("File " + str(len(files) + 1) + (" (required)" if not files else "") + ": ")
        if i: files.append(i)

    print(files)
    if not yes_or_no("Are these correct"):
        return pepco_input()

    return {"files": files}


def get_pepco(conn, opts=None):
    '''
    Mines the data from pepco into a json file.
    '''
    if not opts: opts = pepco_input()
    print(" --- Pepco --- ")

    pepco.get_data(conn, opts["files"])

    print(" --- Pepco Finished --- ")


def solar_input():
    '''
    Prompts the user for all inputs necessary for mining the Solar data.
    '''
    print(" -- Solar Input -- ")

    api_key = important_info("Enter your SolarEdge API Key")
    site_id = important_info("Enter your SolarEdge site id")

    return {"api_key":api_key, "site":site_id}


def get_solar(conn, start=None, end=None, opts=None):
    '''
    Mines the data from SolarEdge into a json file.
    '''
    if not opts: opts = solar_input()
    if not start: start = datetime.datetime(2016, 1, 27)
    if not end: end = datetime.datetime.today()

    print(" --- Solar --- ")

    solar.get_data(conn, opts["site"], opts["api_key"], start, end)

    print(" --- Solar Finished --- ")


def weather_input():
    '''
    Prompts the user for all inputs necessary for mining the Weather data.
    '''
    print(" -- Weather Input -- ")

    api_key = important_info("Enter your NCDC API key")

    return {"api_key": api_key}


# TODO -- Re-write the entire weather folder to be more user friendly.
def get_weather(conn, start=None, end=None, opts=None):
    '''
    Mines the weather data into a json file.
    '''
    if not opts: opts = weather_input()
    if not start: start = datetime.datetime(2014, 1, 1)
    if not end: end = datetime.datetime.today()

    print(" --- Weather --- ")

    weather.get_data(conn, opts["api_key"], start, end)

    print(" --- Weather Finished --- ")


def gshp_input():
    '''
    Gather information necessary to mine Ground Source Heat Pump data.
    '''
    print(" -- GHSP Input -- ")
    
    username = important_info("Enter your Symphony Username")
    password = important_info("Enter your Symphony Password")

    return {"username": username, "password": password}


def get_gshp(conn, start=None, end=None, opts=None):
    '''
    Mine the Ground Source Heat Pump data.
    '''
    if not opts: opts = gshp_input()
    if not start: start = datetime.datetime(2016, 10, 31)
    if not end: end = datetime.datetime.today()

    print(" --- GSHP --- ")

    gshp.get_data(conn, opts["username"], opts["password"], start, end)

    print(" --- GSHP Finished --- ")


def yes_or_no(prompt):
    i = "placeholder"
    while i != "Y" and i.lower() != "n":
        i = input(prompt+" (Y/n): ")
    return i == "Y"


def important_info(prompt):
    confirmed = False
    while not confirmed:
        i = input(prompt+": ")
        if i:
            confirmed = yes_or_no("Is this correct - "+i)
    return i


usage = '''
Usage:

run.py [s|p|g|w] [database]

-i [input name] : Name of input set saved in database
-f [file name]  : Load inputs from a JSON file
-s [YYYY-MM-DD] : Start date
-e [YYYY-MM-DD] : End date

s - solar
p - pepco
g - ground source heat pump
w - weather

You can combine any of the dataset options to mine multiple datasets.

If you supply no dates the program will get all available data.
If you only supply a start date the program will get all data from start date to today.
If you only suplly an end data the program will get all available data before the end date.
If you supply both dates the program gets all possible data within that range. 
Use export.py to convert downloaded data into a readable csv format.'''

# TODO -- ADD Defaults Option (--defaults allows you to use all the default options)
if __name__ == "__main__":
    try:
        if len(argv) < 3:
            print(usage)
        elif set(argv[1]).issubset(set("spgw")):
            which = argv[1]
            conn = sqlite3.connect(argv[2])

            saved_input = None
            if "-i" in argv[3:]:
                saved_input = argv[argv.index("-i")+1]

            file_input = None
            if "-f" in argv[3:]:
                file_input = argv[argv.index("-f")+1] 
            
            start = None
            if "-s" in argv[3:]:
                d = argv[argv.index("-s")+1]
                start = datetime.datetime.strptime(d, "%Y-%m-%d")
            
            end = None
            if "-e" in argv[3:]:
                d = argv[argv.index("-e")+1]
                end = datetime.datetime.strptime(d, "%Y-%m-%d")

            get(conn, which, saved_input, file_input, start, end)
        else:
            print(usage)

    except KeyboardInterrupt:
        print("\nKeyboard Interrupt")
        exit()
