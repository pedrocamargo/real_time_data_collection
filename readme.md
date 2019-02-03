Collection of scripts to download real-time transportation data

All scripts are designed to create output files with the following characteristics:
* One SQLite file per day (GMT)
* One log file
* All outputs in the same folder as the script

Everything has been tested on **Python 3.6** 

# Instructions
To call these scripts, the calls look like the following

## JCDecaux bike data

### For a single city

To download the data, the python call is the following

**python /path/to/script/download.py  API_KEY_TO_BE_OBTAINED_BY_THE_USER  INTERVAL CITY_OF_INTEREST**

INTERVAL is the pause between queries (in minutes) 

### For all cities available

To download the data, the python call is the following

**python /path/to/script/download.py  API_KEY_TO_BE_OBTAINED_BY_THE_USER  INTERVAL**

INTERVAL is the pause between queries (in minutes) 

## Translink (Queensland, Australia) GTFS RT
 Translink does NOT require an API key to access its GTFS-RT feed, so the query call is simpler

To download the data, the python call is the following

**python /path/to/script/gtfs.py  INTERVAL**

INTERVAL is the pause between queries (in minutes) 
 
## Brisbane Traffic data

To download the data, the python call is the following

**python /path/to/script/download_bne.py  API_KEY_TO_BE_OBTAINED_BY_THE_USER  INTERVAL**

INTERVAL is the pause between queries (in minutes) 