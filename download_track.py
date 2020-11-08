import traffic
from traffic.data import opensky
from ect_utils import get_monthly_track, download_single_track

# declare data
data_root = "/home/DATA/EUROCONTROL/"  # flights and plans
track_root = "/home/DATA/EUROCONTROL/TRACKS/"  # destination dir to save track csv

flights = get_monthly_track(data_root, track_root, "201809", "2018-08-31", "2018-10-01", icao24_limit=None)