# Generates a SQLite database used to discover files
This script generates a SQLite database based upon the structure outlined at https://github.com/terraref/reference-data/issues/279

**NOTE**: running this script uses the Globus SDK and requires a web interface to obtain a Native App Authorization Code.
As part of obtaining the code, you will be asked for a label; you may want to reuse the same label every time you run this script.

Please be sure to read up on the assumptions this script makes (see below).

## Database schema
The purpose of the script is to generate a database that can be used for file discovery.
This section outlines the views and underlying tables that are available.

Note that the database is restricted to the dates specified when it was built.

### View: cultivar_files
This view is intended to map cultivars to specific files.

| plot_id | plot_name | season |  file_id | folder | filename | format | sensor | start_time | finish_time | gantry_x | gantry_y | gantry_z | cultivar_name |
|---------|-----------|--------|----------|--------|----------|--------|--------|------------|-------------|----------|----------|----------|---------------|

* plot_id: :unique plot identifier
* plot_name: name of the plot
* season: the name of the season
* file_id: unique identifier of a file
* folder: the path to the file (on Globus, relative to the TERRA REF endpoint)
* filename: the name of the file
* format: the format of the file (the file extension)
* sensor: the sensor associated with the file
* start_time: the starting time of the file content capture
* finish_time: the ending time of the file content capture (may be the same as the start_time)
* gantry_x: the X position of the Gantry at capture start
* gantry_Y: the Y position of the Gantry at capture start
* gantry_Z: the Z position of the Gantry at capture start

### View: weather_files
| timestamp | temperature | illuminance | precipitation | sun_direction | wind_speed | wind_direction | relative_humidity | file_id | folder | filename | format | sensor | start_time | finish_time | gantry_x | gantry_y | gantry_z |
|-----------|-------------|-------------|---------------|---------------|------------|----------------|-------------------|---------|--------|----------|--------|----------|------------|-------------|----------|----------|----------|

* timestamp: the timestamp of the weather capture
* temperature: the temperature in `degrees Celsius`
* illuminance: the brightness in `kilo Lux`
* precipitation: the precipitation in `mm/h`
* sun_direction: the angle of the sun in `degrees`
* wind_speed: the speed of the wind in `m/s`
* wind_direction: the wind direction in `degrees`
* relative_humidity: the relative humidity in `relative humidity percent`
* file_id: unique identifier of a file
* folder: the path to the file (on Globus, relative to the TERRA REF endpoint)
* filename: the name of the file
* format: the format of the file (the file extension)
* sensor: the sensor associated with the file
* start_time: the starting time of the file content capture
* finish_time: the ending time of the file content capture (may be the same as the start_time)
* gantry_x: the X position of the Gantry at capture start
* gantry_Y: the Y position of the Gantry at capture start
* gantry_Z: the Z position of the Gantry at capture start

### Table: experimental_info
| id | plot_name | season_id | season | cultivar_id | plot_bb_min_lat | plot_bb_min_lon | plot_bb_max_lat | plot_bb_max_lon |
|----|-----------|-----------|--------|-------------|-----------------|-----------------|-----------------|-----------------|

* id: a unique identifier of the experiment
* plot_name: the name of a plot in the experiment
* season_id: the identifier of a season
* season: the season of the experiment
* cultivar_id: the ID of the cultivar associated with the plot
* plot_bb_min_lat: the minimum latitude (Y) value of the plot's boundary
* plot_bb_min_lon: the minimum longitude (X) value of the plot's boundary
* plot_bb_max_lat: the maximum latitude (Y) value of the plot's boundary 
* plot_bb_max_lon: the maximum longitude (X) value of the plot's boundary

### Table: cultivars
| id | name |
|----|------|

* id: the unique ID of a cultivar
* name: the name of the cultivar

### Table: files
| id | path | filename | format | sensor | start_time | finish_time | gantry_x | gantry_y | gantry_z | season_id |
|----|------|----------|--------|--------|------------|-------------|----------|----------|----------|-----------|

* path: the path to the file relative to the Globus endpoint
* filename: the name of the file
* format: the format of the file
* sensor: the sensor the file is associated with
* start_time: the starting time of the file content capture
* finish_time: the ending time of the file content capture (may be the same as the start_time)
* gantry_x: the X position of the Gantry at capture start
* gantry_Y: the Y position of the Gantry at capture start
* gantry_Z: the Z position of the Gantry at capture start
* season_id: the ID of the season this file is associated with

### Table: weather <a name="weather" />
| id | timestamp | temperature | illuminance | precipitation | sun_direction | wind_speed | wind_direction | relative_humidity |
|----|-----------|-------------|-------------|---------------|---------------|------------|----------------|-------------------|

* id: the unique ID of the weather entry
* timestamp: the timestamp of the weather capture
* temperature: the temperature in `degrees Celsius`
* illuminance: the brightness in `kilo Lux`
* precipitation: the precipitation in `mm/h`
* sun_direction: the angle of the sun in `degrees`
* wind_speed: the speed of the wind in `m/s`
* wind_direction: the wind direction in `degrees`
* relative_humidity: the relative humidity in `relative humidity percent`

### Table: weather_file_map
A utility table used to map a file's start and finish times to weather entries.
The complete weather available for a file is bracketed by the min_weather_id and max_weather_id indexes into the [weather](#weather) table. 

| id | file_id | min_weather_id | max_weather_id |
|----|---------|----------------|----------------|

* id: the unique ID of each entry
* file_id: the ID of a file
* min_weather_id: the ID of a weather entry that is less than or equal to the file start_time value 
* max_weather_id: the ID of a weather entry that is greater than or equal to the file finish_time value 

## Dependencies
The main dependency of this script is to the Globus accessing the TERRA REF data.
It's expected that Globus Connect Personal is running locally and that there's a TERRA REF endpoint defined.

Calls are made to the BETYdb `API` to extract experiment information.
If a suitable JSON file is available locally, it can be specified on the command line and bypass the BETYdb API call.

Calls are also made to the TERRA REF `BRAPI` interface to retrieve the experiment, plot, and cultivar information. 

## Sample Command Line
Below is a sample command line.
Please see the section below for additional information on the command line parameters.

```python3 generate.py --BETYDB_URL <url> --BETYDB_KEY <key> stereoTop=/home/myself/globus_data/stereoTop "2018-05-08" /home/myself/test.db```

This command line specifies the `url` and `key` needed to access BETYdb through command line options.

The `stereoTop=raw_data/stereoTop` portion of the command line identifies the stereoTop sensor and where to find the related files at the Globus TERRA REF endpoint.

The dates of interest are specified as `"2018-05-08"`; in this case only one date is specified.

Finally, the output file is specified: `/home/myself/test.db`

## Command Line Parameters
Please run the script with the `-h` option to get an updated list of options that are available.

### Mandatory parameters
* sensor_paths: a comma separated list of sensor and path pairs.
The sensors and path pairs are separated by an equal sign.
See below for more information on how to specify sensor paths.
* dates: a comma separated list of dates and date ranges.
All date values are expected for match the format of `YYYY-MM-DD`.
Date ranges consist of the lower date and upper date, separated by a colon.
* output_file: the full path to the output file to write to

**Sensor Notes**: \
There is no requirement that the sensor name portion of the sensor path match what TERRA REF defines as a sensor.
The path portion of the sensor path pair can be either a relative path from the Globus endpoint (relative to 'ua_mac'), or a path to where the files are stored locally. 

### Optional parameters
* --BETYDB_URL: the URL of the BETYdb instance to access
* --BETYDB_KEY: the key to use when accessing the BETYdb instance
* --BRAPI_URL: an alternative BRAPI endpoint to use 
* --debug: enables displaying of debug messages; setting this will display quite a bit of information
* --globus_endpoint: override the default name of the Globus Personal Connect endpoint for TERRA REF
* --experiment_json: path to the file containing the experiment JSON previously fetched from BETYdb.
When specified, the BETYDB_URL and BETYDB_KEY don't need to be specified (although a warning is generated).
This file is used to satisfy all experiment data for the dates specified
* --cultivar_json: path to the file containing the complete set of cultivar JSON retrieved from BRAPI.
This file is used to satisfy all cultivar data for the dates specified
