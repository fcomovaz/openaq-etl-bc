import csv
from collections import defaultdict
from datetime import datetime

from logging_config import setup_logging
import logging


# Logging configuration
setup_logging()
logger = logging.getLogger()  # start logging

logger.info("......................................")
logger.info("Starting collapse_sensor_values_wide.py")
logger.info("......................................")

# columnas esperadas de contaminantes (ajusta si faltan)
pollutants = ["co", "no", "no2", "nox", "o3", "so2", "pm10", "pm25"]

logger.info(f"pollutants to be extracted: {', '.join(pollutants)}")
logger.info("For different pollutants, change line 18 in this script")

INPUT_FILE = ".vars/sensor_values_wide.csv"
OUTPUT_FILE = ".vars/sensor_values_collapsed.csv"


def floor_hour(dt):
    return dt.replace(minute=0, second=0, microsecond=0)


# estructura: key -> dict: pollutant -> list(values), other cols -> single value
data = {}  # key -> record

with open(INPUT_FILE, newline="", encoding="utf-8-sig") as f:
    reader = csv.DictReader(f)
    headers = reader.fieldnames

    for row in reader:
        # parse timestamp (ignora errores)
        ts_raw = row.get("sensor_timestamp", "")
        try:
            ts = datetime.fromisoformat(ts_raw)
        except Exception:
            # intentar formato alterno
            try:
                ts = datetime.strptime(ts_raw, "%Y-%m-%d %H:%M:%S")
            except Exception:
                continue
        ts = floor_hour(ts)

        # construir llave por estación + hora
        key = (
            row.get("station_id", ""),
            row.get("station_name", ""),
            row.get("station_longitude", ""),
            row.get("station_latitude", ""),
            ts.isoformat(),
        )

        if key not in data:
            data[key] = {"counts": {p: [] for p in pollutants}, "others": {}}

        # para cada pollutant, si el campo tiene valor lo agregamos
        for p in pollutants:
            v = row.get(p, "")
            if v is not None and v != "":
                try:
                    fv = float(v)
                    data[key]["counts"][p].append(fv)
                except Exception:
                    pass

        # # guardar columnas "otras" (sensor_id, sensor_unit, etc.) si no existen ya
        # for col in headers:
        #     if col in ["sensor_timestamp"] + pollutants:
        #         continue
        #     if col not in data[key]["others"]:
        #         val = row.get(col, "")
        #         if val != "":
        #             data[key]["others"][col] = val

# construir salida
# out_fields = ["station_id","station_name","station_longitude","station_latitude","sensor_timestamp"] + pollutants + list({k for d in data.values() for k in d["others"].keys()})
out_fields = [
    "station_id",
    "station_name",
    "station_longitude",
    "station_latitude",
    "sensor_timestamp",
] + pollutants
out_rows = []

for key, rec in data.items():
    sid, sname, slon, slat, ts = key
    row = {
        "station_id": sid,
        "station_name": sname,
        "station_longitude": slon,
        "station_latitude": slat,
        "sensor_timestamp": ts,
    }
    # promediar listas
    for p in pollutants:
        vals = rec["counts"].get(p, [])
        if vals:
            row[p] = sum(vals) / len(vals)
        else:
            row[p] = ""
    # otras columnas
    row.update(rec["others"])
    out_rows.append(row)

# escribir CSV
with open(OUTPUT_FILE, "w", newline="", encoding="utf-8-sig") as f:
    writer = csv.DictWriter(f, fieldnames=out_fields)
    writer.writeheader()
    for r in out_rows:
        writer.writerow(r)

logger.info(f"Output file saved to {OUTPUT_FILE}")

logger.info("......................................")
logger.info("Finished convert_sensor_values_wide.py")
logger.info("......................................")
