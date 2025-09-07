from Requestify import Requestify
import time

from logging_config import setup_logging
import logging


# Logging configuration
setup_logging()
logger = logging.getLogger()  # start logging


def time_estimate(station_current, NUM_STATIONS, start_date=None):
    NUM_MAX_POINTS = 1000
    TIMEOUT = 4

    url = f"/v3/locations/{station_current}"  # set the url

    conn = Requestify()
    _, response = conn.get(url)

    result_raw = response[0]  # get the json result

    station_id = result_raw["id"]  # get the station id
    station_name = result_raw["name"]  # get the station name
    first_date = result_raw["datetimeFirst"]["utc"]
    last_date = result_raw["datetimeLast"]["utc"]

    # convert the dates to timestamp
    first_date = time.mktime(time.strptime(first_date, "%Y-%m-%dT%H:%M:%SZ"))
    last_date = time.mktime(time.strptime(last_date, "%Y-%m-%dT%H:%M:%SZ"))

    # calculate variables
    points = (last_date - first_date) / 3600  # points == hours
    pages = points / NUM_MAX_POINTS  # pages ==
    time_per_station = pages * TIMEOUT
    time_stations_s = time_per_station * NUM_STATIONS
    time_stations_m = time_stations_s / 60
    # time_all_stations_h = time_stations_s / 3600

    # expected_end = time.mktime(time.strptime(start_date, "%Y-%m-%dT%H:%M:%SZ"))
    # expected_end = expected_end + time_stations_s + TIMEOUT
    # expected_end = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.localtime(expected_end))

    elapsed_str = f"{time_stations_s:.2f} seconds / {time_stations_m:.2f} minutes"

    # logger.info(f"there are at least {points} entries in the dataset")
    logger.info(f"analyzing {station_name} - {station_id}")
    logger.info(f"this will take at least {elapsed_str}")
    # logger.info(f"expected end: {expected_end}")

    time.sleep(TIMEOUT)

    return time_stations_m


# time_estimate(station_list, time.strftime("%Y-%m-%dT%H:%M:%SZ"))
