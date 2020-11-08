from datetime import datetime
import argparse, os
import traffic
import shutil
from traffic.data import opensky
from ect_utils import dates_in_month
from joblib import Parallel, delayed

# declare data
data_root = "/home/DATA/EUROCONTROL"  # flights and plans
track_root = "/home/DATA/EUROCONTROL/TRACKS"  # destination dir to 


def download_hr_data(date, hr, month_save_dir):
    # date save dir
    date_save_dir = os.path.join(month_save_dir, date)
    if not os.path.exists(date_save_dir):
        os.mkdir(date_save_dir)

    # target csv file
    target_csv = os.path.join(date_save_dir, "{}_{}.csv.gz".format(date, str(hr).zfill(2)))
    print("\nTarget file:", target_csv)
    if os.path.exists(target_csv):
        print("Target file exists")
        return 1
    
    # get midnight hour
    year, month, day = date.split("-")
    start = int(datetime(int(year), int(month), int(day), 0, 0, 0).timestamp())  # this is 00:00 AM of the day
    start = start + 3600*hr
    stop  = start + 3600
    
    # download data
    query_string = "SELECT * FROM state_vectors_data4 WHERE hour>={} and hour<{}".format(start, stop)
    print("Querying:", query_string)
    print(date, hr, "Start downloading at", datetime.now())

    data = opensky._impala(query_string, columns="\t".join(opensky._impala_columns), cached=False)
    print(date, hr, "Completed downloading at", datetime.now())
    data.to_csv(target_csv, index=False)
    print(date, hr, "Completed saving at", datetime.now(), "\n")
    return 2



if __name__ == "__main__":
    # parse input arguments
    parser = argparse.ArgumentParser(description='OpenSky Traffic Data Downloader')
    parser.add_argument('-y', action="store", dest="year" , type=str)
    parser.add_argument('-m', action="store", dest="month", type=str)    
    params = parser.parse_args()

    # save dir
    month_save_dir = os.path.join(track_root, "{}{}".format(params.year, params.month.zfill(2)))
    if not os.path.exists(month_save_dir):
        os.mkdir(month_save_dir)
    print(month_save_dir)
    
    # loop through dates in month
    all_dates = dates_in_month(params.year, params.month)
    hours = [range(0, 6), range(6, 12), range(12, 18), range(18, 24)]

    for date in all_dates:
        # result = [download_hr_data(date, hr, month_save_dir) for hr in range(0, 2)]
        for hrs in hours:            
            result = Parallel(n_jobs=2)(delayed(download_hr_data)(date, hr, month_save_dir) for hr in hrs)
            #shutil.rmtree("/home/phu/.cache/traffic/opensky", ignore_errors=True)
            #print("\nCache cleared.\n")
