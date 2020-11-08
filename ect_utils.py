''' Helper functions to download ADS-B tracks for Eurocontrol flights '''

import pandas as pd
# import modin.pandas as mpd
import os, logging, time
from datetime import datetime
import traffic
from traffic.data import opensky, aircraft
from pandarallel import pandarallel
pandarallel.initialize(nb_workers=40)
pd.set_option('display.max_rows', None)
logging.basicConfig(level=logging.DEBUG)

# declare data

DATA_ROOT = "/home/DATA/EUROCONTROL/"  # flights and plans
TRACK_ROOT = "/home/DATA/EUROCONTROL/TRACKS/"  # destination dir to save track csv

AIRCRAFT = aircraft.data
AIRCRAFT.registration = AIRCRAFT.registration.parallel_apply(lambda x: x.replace("-", "").upper())


def convert_datetime(dt):
    return datetime.strptime(dt, '%d-%m-%Y %H:%M:%S')


def flight_duration(flight):
    delta = flight.stop - flight.start
    return int(delta.seconds/3600)+1



def get_plans_csv(year, month, data_root=DATA_ROOT):
    ''' Retrieve csv path to the flightplans data file given year and month '''
    months = [3, 6, 9, 12]
    last_days = [31, 30, 30, 31]
    last_day = last_days[months.index(month)]        
    month = str(month).zfill(2)
    suffix = "_{}{}01_{}{}{}.csv.gz".format(year, month, year, month, last_day)    
    plans_csv  = os.path.join(data_root, "{}{}".format(year, str(month).zfill(2)), "Flight_Points_Filed" + suffix)
    assert os.path.isfile(plans_csv)
    return plans_csv



def load_plans(year, month):
    return pd.read_csv(get_plans_csv(year, month))



def load_flight_plan(flight, plans):
    plan = plans[plans["ECTRL ID"]==flight["ECTRL ID"]].reset_index(drop=True)
    return plan    



def get_flights_csv(year, month, data_root=DATA_ROOT):
    ''' Retrieve csv path to the flights data file given year and month '''
    months = [3, 6, 9, 12]
    last_days = [31, 30, 30, 31]
    last_day = last_days[months.index(month)]        
    month = str(month).zfill(2)
    suffix = "_{}{}01_{}{}{}.csv.gz".format(year, month, year, month, last_day)
    flights_csv = os.path.join(data_root, "{}{}".format(year, str(month).zfill(2)), "Flights" + suffix)
    assert os.path.isfile(flights_csv)
    return flights_csv



def find_track_csv(flight, track_root=TRACK_ROOT):
    """ Construct a list of all csv files for a flight 
        Input is a flight (row) from flights dataframe
    """
    start_date, stop_date = flight.start.date(), flight.stop.date()
    start_hour, stop_hour = flight.start.hour, flight.stop.hour
    if start_date == stop_date:
        # flight completed within one day, then just find which hours the flight spanned
        year, month, day = start_date.year, start_date.month, start_date.day
        csv_path = os.path.join(
            track_root, 
            "{}{}".format(year, str(month).zfill(2)), 
            "{}-{}-{}".format(year, str(month).zfill(2), str(day).zfill(2))
        )       
        csv_files = ["{}-{}-{}_{}.csv.gz".format(year, str(month).zfill(2), str(day).zfill(2), str(hr).zfill(2)) for hr in range(start_hour, stop_hour+1)]
        csv_files = [os.path.join(csv_path, file) for file in csv_files]
    if start_date < stop_date:
        # Flights took longer than one day
        # Assuming we only deal with flights within a month (year and month are the same)
        start_year, start_month = start_date.year, start_date.month,
        start_day, stop_day =  start_date.day, stop_date.day      
        num_day = stop_day - start_day
        csv_files = []
        for day_count, day in enumerate(range(start_day, stop_day+1)):
            csv_path = os.path.join(
                track_root, 
                "{}{}".format(start_year, str(start_month).zfill(2)), 
                "{}-{}-{}".format(start_year, str(start_year).zfill(2), str(day).zfill(2))
            ) 
            if day_count == 0:
                start_hr, stop_hr = start_hour, 23
            elif day_count == num_day:
                start_hr, stop_hr = 0, stop_hour
            else:
                start_hr, stop_hr = 0, 23
            csv_day = ["{}-{}-{}_{}.csv.gz".format(
                start_year, 
                str(start_month).zfill(2), 
                str(day).zfill(2), 
                str(hr).zfill(2)) for hr in range(start_hr, stop_hr+1)] 
            csv_day = [os.path.join(csv_path, file) for file in csv_day]
            csv_files += csv_day
    return csv_files
    

   
def load_flights(year, month, track_root=TRACK_ROOT):
    ''' Read flights of a month and perform basic filter and track data locating '''
    # get path to flights csv and read to pandas dataframe
    flights = pd.read_csv(get_flights_csv(year, month))
    
    # add start, stop datetime objects for all flights
    flights["start"] = flights["ACTUAL OFF BLOCK TIME"].parallel_apply(convert_datetime)
    flights["stop"] = flights["ACTUAL ARRIVAL TIME"].parallel_apply(convert_datetime)
    flights["duration"] = flights.parallel_apply(flight_duration, axis=1)
    
    # FILTER: keep only flights shorter than 5 hours
    flights = flights[flights.duration<=5]

    # FILTER: Keep only flights strictly between First and Last day of the month
    START = datetime(year, month,   1, 0, 0, 0)
    STOP  = datetime(year, month+1, 1, 0, 0, 0)
    flights = flights[(flights.start>=START) & (flights.stop<STOP)]
    
    # Filter by flight type
    flights = flights[(flights["STATFOR Market Segment"].isin(["Traditional Scheduled", "Lowcost"])) & (flights["ICAO Flight Type"]=="S")]

    # Add aircraft information (icao24)
    flights = flights.merge(AIRCRAFT, left_on="AC Registration", right_on="registration", how="left")
    flights = flights[~flights.icao24.isnull()]
    
    # Get all track csv dir
    flights["csv"] = flights.parallel_apply(find_track_csv, axis=1)
    flights.reset_index(drop=True, inplace=True)
    return flights



def load_flown_track(flight):
    ''' Read flown track of a flight from a set of track csv files 
        Input is a row from flights dataframe
    '''
    assert "csv" in flight, "csv field must be constructed before calling this function"
    print("Total files to read:", len(flight.csv))
    flight_icao24 = flight["icao24"]
    track = pd.DataFrame()
    for count, csv in enumerate(flight.csv):
        print("reading csv file ", count, csv)
        df = pd.read_csv(csv)
        df = df[df["icao24"]==flight.icao24]
        track = pd.concat([track, df], ignore_index=True)
    df = None
    return track
