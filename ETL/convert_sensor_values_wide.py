import csv
from collections import defaultdict

from logging_config import setup_logging
import logging


# Logging configuration
setup_logging()
logger = logging.getLogger()  # start logging

logger.info("......................................")
logger.info("Starting convert_sensor_values_wide.py")
logger.info("......................................")

# Leer el archivo original
datos_por_estacion = defaultdict(dict)
pollutants = ["co", "no", "no2", "nox", "o3", "so2", "pm10", "pm25"]

logger.info(f"pollutants to be extracted: {', '.join(pollutants)}")
logger.info("For different pollutants, change line 18 in this script")

INPUT_FILE = ".vars/sensor_values.csv"
OUTPUT_FILE = ".vars/sensor_values_wide.csv"


with open(INPUT_FILE, "r", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)

    for fila in reader:
        # Crear una clave única para cada combinación estación-timestamp
        clave = (
            fila["station_id"],
            fila["station_name"],
            fila["station_longitude"],
            fila["station_latitude"],
            fila["sensor_id"],
            fila["sensor_timestamp"],
        )

        if clave not in datos_por_estacion:
            # Inicializar con todos los pollutants vacíos
            datos_por_estacion[clave] = {cont: "" for cont in pollutants}
            datos_por_estacion[clave].update(
                {
                    "station_id": fila["station_id"],
                    "station_name": fila["station_name"],
                    "station_longitude": fila["station_longitude"],
                    "station_latitude": fila["station_latitude"],
                    "sensor_id": fila["sensor_id"],
                    "sensor_unit": fila["sensor_unit"],
                    "sensor_timestamp": fila["sensor_timestamp"],
                }
            )

        # Agregar el valor del pollutant si existe
        pollutant = fila["sensor_pollutant"]
        if pollutant in pollutants:
            datos_por_estacion[clave][pollutant] = fila["sensor_value"]

# Escribir el nuevo archivo
with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
    campos = (
        [
            "station_id",
            "station_name",
            "station_longitude",
            "station_latitude",
            "sensor_id",
        ]
        + pollutants
        + ["sensor_unit", "sensor_timestamp"]
    )

    writer = csv.DictWriter(f, fieldnames=campos)
    writer.writeheader()

    for datos in datos_por_estacion.values():
        writer.writerow(datos)

logger.info(f"Output file saved to {OUTPUT_FILE}")

logger.info("......................................")
logger.info("Finished convert_sensor_values_wide.py")
logger.info("......................................")
