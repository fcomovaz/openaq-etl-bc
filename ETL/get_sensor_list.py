from Requestify import Requestify
from time_counter import time_estimate
import time
import csv
import os
from logging_config import setup_logging
import logging


# Logging configuration
setup_logging()
logger = logging.getLogger()  # start logging

logger.info("......................................")
logger.info("Starting get_sensor_list.py")
logger.info("......................................")


# Get stations in a list -- REQUIRED
try:
    with open(".vars/stations.txt", "r") as f:
        lines = f.readlines()
        station_list = [line.strip() for line in lines]
    logger.info("Stations list loaded")
except Exception as e:
    logger.error(e)
    exit()  # cannot continue if the file is missing


# In case of interruption in the pipeline
# Get the index of the last processed station as index
try:
    with open(".vars/current-station-idx.txt", "r") as f:
        station_idx = int(f.read())
    if station_idx >= len(station_list):
        logger.warning(
            f"No more stations to process - {station_idx}/{len(station_list)}"
        )
        with open(".vars/current-station-idx.txt", "w") as f:
            f.write("0")
        exit()
except Exception as e:
    logger.warning(e)
    logger.info("Creating current-station-idx.txt file > 0")
    try:
        with open(".vars/current-station-idx.txt", "w") as f:
            f.write("0")
        station_idx = 0
    except Exception as e:
        logger.error(e)
        exit()
    logger.info("current-station-idx.txt created")


# Create the csv file with the headers if the file does not
columns_csv = [
    "station_id",
    "station_name",
    "station_longitude",
    "station_latitude",
    "sensor_id",
    "sensor_pollutant",
    "sensor_unit",
]
try:
    if not os.path.exists(".vars/sensor_list.csv") or station_idx == 0:
        with open(".vars/sensor_list.csv", "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(columns_csv)
        logger.info("sensor_list.csv blank file created")
except Exception as e:
    logger.error(e)
    exit()

total_time_required = 0  # initialize the total time required

for i in range(station_idx, len(station_list)):
    station_current = station_list[i]  # set the current station id
    url = f"/v3/locations/{station_current}"  # set the url

    # now = time.strftime("%Y-%m-%dT%H:%M:%SZ")
    total_time_required += time_estimate(station_current, len(station_list))

    conn = Requestify()
    _, response = conn.get(url)

    result_raw = response[0]  # get the json result
    station_id = result_raw["id"]  # get the station id
    station_name = result_raw["name"]  # get the station name
    station_lcl = result_raw["locality"]  # get the station locality
    station_long = result_raw["bounds"][0]  # get the station longitude
    station_lat = result_raw["bounds"][1]  # get the station latitude
    sensors = result_raw["sensors"]  # get the sensor list

    try:
        with open(".vars/sensor_list.csv", "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            for sensor in sensors:
                # verbose
                # print(
                #     f"{station_id},{station_name},{station_lcl},{sensor['id']},"
                #     + f"{sensor['parameter']['name']},{sensor['parameter']['units']}"
                # )
                # write to the csv
                writer.writerow(
                    [
                        station_id,
                        station_name,
                        station_long,
                        station_lat,
                        sensor["id"],
                        sensor["parameter"]["name"],
                        sensor["parameter"]["units"],
                    ]
                )

        logger.info(f"Station {station_name} - {station_lcl} processed")

    except Exception as e:
        logger.error(e)

    try:
        with open(".vars/current-station-idx.txt", "w") as f:
            f.write(str(i + 1))
        logger.info(f"next station index set to {i + 1}")
    except Exception as e:
        logger.error(e)

    # artificial delay to avoid being blocked
    time.sleep(4)
    logger.info("--------------------------------------------------")

logger.info("......................................")
logger.info("get_sensor_list.py completed")
logger.info("......................................")

logger.warning("##################################################")
logger.warning(f"Next stage can take: {total_time_required} minutes")
logger.warning("##################################################")
