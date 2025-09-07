
## Required Files

### For ETL folder

The ETL process uses the OpenAQ API to extract the historical data of each station. A log file is generated called `etl_requests.log` to store the process information (give it a look when executing).

| File  | Description |
|:-----:|:-----------:|
| `.vars/openaq-key.txt` | API key for OpenAQ |
| `.vars/stations.txt` | List of stations to process (extracted from OpenAQ) |


For example **235220** in the URL [https://explore.openaq.org/locations/235220](https://explore.openaq.org/locations/235220). Example in the content of `.vars/stations.txt` (list of station ids given by OpenAQ).
```
12
13
14
```

You can get an API key from [https://explore.openaq.org/register](https://explore.openaq.org/register). After that you can consult it in [https://explore.openaq.org/account](https://explore.openaq.org/account). Example in `.vars/openaq-key.txt`:
```
1234567890abcdefghijklmnopqrstuvwxyz
```

The expected folder structure would be:
```
.
├── ETL
│   ├── Requestify.py
│   ├── time_counter.py
│   ├── logging_config.py
│   ├── main.py
│   ├── get_sensor_list.py
│   ├── get_sensor_values.py
│   ├── convert_sensor_values_wide.py
│   ├── collapse_sensor_values_wide.py
│   └── .vars
│       ├── openaq-key.txt
│       └── stations.txt
