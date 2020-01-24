import requests
import pandas as pd
from datetime import datetime
import argparse
import sys
import json


DATA_STORE = None
def extract_data(csv_file="data.csv"):
    global DATA_STORE
    # Check if save file exists
    #
    try:
        with open(csv_file, "r") as f:
            contents = f.read()
            if contents != "":
                # Convert dates
                #
                lines = ""
                with open(csv_file, "r") as save_file:
                    for line in save_file:
                        lines += line.replace("/15", "/2015")
                with open(csv_file+ ".tmp", "w") as save_file:
                    save_file.write(lines)

                # Read stored file
                #
                DATA_STORE = pd.read_csv(csv_file +".tmp")
                DATA_STORE.columns = DATA_STORE.columns.str.lower()
                DATA_STORE[u'date'] = pd.to_datetime(DATA_STORE[u'date'], format="%m/%d/%Y %H:%M")
                return
    except IOError:
        pass

    # Get data from website
    #
    endpoint="http://lameapi-env.ptqft8mdpd.us-east-2.elasticbeanstalk.com/data"
    print "Calling endpoint ", endpoint, " for data"
    response = requests.get(endpoint)

    # Check response status
    #
    if response.status_code != 200:
        print "Obtained non-200 http status code ", response.status_code
        return

    # Extract json body
    #
    data = ""
    try:
        json_body = response.json()
        data = json_body.get("data")
    except ValueError:
        print "Response has no json body"
        return
    if data == None:
        print "Data is none"
        return
    if data == "":
        print "Data is blank"
        return 
    
    # Save data on a csv file
    #
    with open(csv_file, "w") as save_file:
        save_file.write(data)
    
    # Convert dates
    #
    lines = ""
    with open(csv_file, "r") as save_file:
        for line in save_file:
            lines += line.replace("/15", "/2015")

    with open(csv_file+ ".tmp", "w") as save_file:
        save_file.write(lines)

    # Convert data to pandas object
    #
    DATA_STORE = pd.read_csv(csv_file +".tmp")
    DATA_STORE.columns = DATA_STORE.columns.str.lower()
    DATA_STORE[u'date'] = pd.to_datetime(DATA_STORE[u'date'], format="%m/%d/%Y %H:%M")


def kpi(kpi_list=['humidity', 'temperature', 'light', 'co2', 'humidityratio', 'occupancy'],
            start="2/3/15 00:00", end="2/10/15 00:00"):
    global DATA_STORE

    # convert datesstrings
    #
    start = start.replace("/15", "/2015")
    end = end.replace("/15", "/2015")

    # convert datestring to datetime object
    #
    start_date = datetime.strptime(start, "%m/%d/%Y %H:%M")
    end_date = datetime.strptime(end, "%m/%d/%Y %H:%M")

    # Convert datetime object to string 
    #
    start = start_date.strftime("%Y-%m-%d %H:%M:00")
    end = end_date.strftime("%Y-%m-%d %H:%M:00")

    # Convert kpi_list to unicode
    #
    kpi_list = [ unicode(kpi_column, 'utf-8') for kpi_column in kpi_list]

    # Load data if it does not exist
    #
    while DATA_STORE is None:
        extract_data()
    
    data = DATA_STORE.query("date >= '{}'".format(start))
    data = data.query("date <= '{}'".format(end))

    if data.empty:
        print ("Obtained empty data set")
        return None

    result = dict()
    for kpi in kpi_list:
        data_col = data[kpi]
        data_dict = dict()

        # Get first value
        #
        data_dict["first"] =  data_col.iloc[0]

        # Get last value
        #
        data_dict["last"] =  data_col.iloc[-1]

        # Get median
        #
        data_dict["median"] = data_col.median()

        # Get lowest
        #
        data_dict["lowest"] = data_col.min()

        # Get highest
        #
        data_dict["highest"] = data_col.max()

        # Get mode
        #
        data_dict["mode"] = [value for value in data_col.mode().values]

        # Get average
        #
        data_dict["average"] = data_col.mean()

        # Get percent change
        #
        data_dict["percent_change"] = [value for value in data_col.pct_change().values]

        result[kpi] = data_dict
    return result

if __name__ == "__main__":
    extract_data()


    parser = argparse.ArgumentParser(description="Obtain kpis")
    parser.add_argument("--start", help="Start date", type=str, nargs=1,
        metavar="DATE", required=False, default="2/3/15 00:00")
    parser.add_argument("--end", help="End date", type=str, nargs=1, metavar="DATE",
        required=False, default="2/10/15 00:00")
    parser.add_argument("--kpi", help="List of KPIS delimited by ','", type=str,
        nargs=1, metavar="KPI_LIST",
        required=False, default="humidity,temperature,light,co2,humidityratio,occupancy")

    # Get command line arguments
    #
    args = parser.parse_args()
    kpi_list = [kpi_ for kpi_ in args.kpi[0].split(',')]

    # Run KPI generation
    result = kpi(kpi_list=kpi_list, start=args.start, end=args.end)
    print json.dumps(result)

