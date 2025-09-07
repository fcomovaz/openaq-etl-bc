from Requestify import Requestify
import time
import csv
import os
from logging_config import setup_logging
import logging


# Logging configuration
setup_logging()
logger = logging.getLogger()  # start logging

logger.info("......................................")
logger.info("Starting get_sensor_values.py")
logger.info("......................................")

# read the csv file with the sensor list
try:
    with open(".vars/sensor_list.csv", "r", encoding="utf-8") as f:
        lines = f.readlines()
        sensor_rows = [line.strip().split(",") for line in lines]
    logger.info("Sensor list loaded")
except Exception as e:
    logger.error(e)
    exit()  # cannot continue if the file is missing

# In case of interruption in the pipeline
# Get the index of the last processed sensor as index
try:
    with open(".vars/current-sensor-idx.txt", "r") as f:
        sensor_idx = int(f.read())
    if sensor_idx >= len(sensor_rows):
        logger.warning(f"No more sensors to process - {sensor_idx}/{len(sensor_rows)}")
        with open(".vars/current-sensor-idx.txt", "w") as f:
            f.write("0")
        exit()
except Exception as e:
    logger.warning(e)
    logger.info("Creating current-sensor-idx.txt file > 0")
    try:
        with open(".vars/current-sensor-idx.txt", "w") as f:
            f.write("0")
        sensor_idx = 0
    except Exception as e:
        logger.error(e)
        exit()
    logger.info("current-sensor-idx.txt created")


# Create the csv file with the headers if the file does not
columns_csv = [
    "station_id",
    "station_name",
    "station_longitude",
    "station_latitude",
    "sensor_id",
    "sensor_pollutant",
    "sensor_unit",  # below are the extra columns
    "sensor_value",
    "sensor_timestamp",
]
try:
    if not os.path.exists(".vars/sensor_values.csv"):
        with open(
            ".vars/sensor_values.csv", "w", newline="", encoding="utf-8-sig"
        ) as f:
            writer = csv.writer(f)
            writer.writerow(columns_csv)
        try:
            with open(".vars/current-page-idx.txt", "w") as f:
                f.write("1")
            logger.info("page count reset to 1")
        except Exception as e:
            logger.error(e)
            exit()

        sensor_idx = 0  # reset sensor counter
        logger.info("sensor_values.csv blank file created")
except Exception as e:
    logger.error(e)
    exit()

# In case of interruption in the pipeline
# Get the index of the last processed page as index
with open(".vars/current-page-idx.txt", "r") as f:
    page_idx = int(f.read())

# extract only the sensor id
sensor_ids = [row[4] for row in sensor_rows]
sensor_ids = sensor_ids[1:]  # remove the header

# select the column sensor_id
for i in range(sensor_idx, len(sensor_rows) - 1):
    sensor_current = sensor_ids[i]  # set the current sensor id

    for page in range(page_idx, 100):
        url = f"/v3/sensors/{sensor_current}/measurements?limit=1000&page={page}"  # set the url
        logger.info(f"Processing sensor {i+1}/{len(sensor_rows)} -  page {page}")

        conn = Requestify()
        check, response = conn.get(url)

        found = check["meta"]["found"]  # get the number of values in the page
        if found == ">1000":
            data_points_length = 1000  # default pagination limit
        else:
            data_points_length = found  # here will be <1000

        sensor_values = []
        for j in range(data_points_length):
            pol_value = response[j]["value"]
            time_value = response[j]["period"]["datetimeFrom"]["utc"][:-1]

            temp = sensor_rows[i + 1].copy()
            temp.extend([pol_value, time_value])
            sensor_values.append(temp)

        with open(".vars/sensor_values.csv", "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            for k in range(data_points_length):
                writer.writerow(
                    [
                        sensor_values[k][0],
                        sensor_values[k][1],
                        sensor_values[k][2],
                        sensor_values[k][3],
                        sensor_values[k][4],
                        sensor_values[k][5],
                        sensor_values[k][6],
                        sensor_values[k][7],
                        sensor_values[k][8],
                    ]
                )

        try:
            with open(".vars/current-page-idx.txt", "w") as f:
                f.write(str(page + 1))
            logger.info(f"next page index set to {page + 1}")
        except Exception as e:
            logger.error(e)

        if found != ">1000":
            logger.info("Last page reached")
            break
        # artificial delay to avoid being blocked
        time.sleep(4)

    try:
        with open(".vars/current-sensor-idx.txt", "w") as f:
            f.write(str(i + 1))
        logger.info(f"next sensor index set to {i + 1}")
    except Exception as e:
        logger.error(e)

    logger.info("--------------------------------------------------")


logger.info("......................................")
logger.info("get_sensor_values.py completed")
logger.info("......................................")