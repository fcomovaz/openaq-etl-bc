import time
from logging_config import setup_logging
import logging


# Logging configuration
setup_logging()
logger = logging.getLogger()  # start logging

files = [
    "get_sensor_list.py",  # -> .vars/sensor_list.csv
    "get_sensor_values.py",  # -> .vars/sensor_values.csv
    "convert_sensor_values_wide.py",  # -> .vars/sensor_values_wide.csv
    "collapse_sensor_values_wide.py",  # -> .vars/sensor_values_collapsed.csv

]
for file in files:
    time0 = time.asctime(time.gmtime(time.time()))
    logger.info("==================================================")
    logger.info("==================================================")
    logger.info(f"====> Running     -> {file} ")
    logger.info(f"====> Started at  -> {time0} ")
    # logger.info("==================================================")
    exec(open(file).read())
    time1 = time.asctime(time.gmtime(time.time()))
    # logger.info("==================================================")
    logger.info(f"====> Finished at -> {time1} ")
    t_0 = time.mktime(time.strptime(time0, "%a %b %d %H:%M:%S %Y"))
    t_1 = time.mktime(time.strptime(time1, "%a %b %d %H:%M:%S %Y"))
    elapsed = t_1 - t_0
    logger.info(f"====> Processed   -> {file}")
    logger.info(f"====> Elapsed     -> {elapsed} seconds")
    logger.info("==================================================")
    logger.info("==================================================")
