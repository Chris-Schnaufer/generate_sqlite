# Generates a SQLite database used to discover files
This script generates a SQLite database based upon the structure outlined at https://github.com/terraref/reference-data/issues/279

Please be sure to read up on the [dependencies](#dependencies) this script has.

You can also skip over the command line to the [database schema](#schema) portion of this document.

## Sample Command Line
Run the following command to get the full list of supported command line arguments.
This is especially helpful in determining supported sensor/data products.
```python3 generate.py -h```
Note that it may be possible to run the script without specifying "python3" on your system.

Next is a sample command line.
Please see the section below for additional information on the command line parameters.

```python3 generate.py --debug --BETYDB_URL <betydb_url> --BETYDB_KEY <key> --BRAPI_URL <brapi_url> --gene_marker_file "Markers of Interest-Table 1.csv" --gene_marker_file_key 3 --cultivar_gene_map_file "PresentAbsent by Cultivar-Table 1.csv" --cultivar_gene_file_key 0  --cultivar_gene_map_file_ignore 7 RGB "2018-05-08" /home/myself/test.db```

This command line specifies the `betydb_url` and `key` needed to access BETYdb through command line options `--BETYDB_URL` and `--BETYDB_KEY`.

The BRAPI API instance to access is specified by the `--BRAPI_URL` command line option where `<brapi_url>` is replaced with the actual URL.

Optional genetic information is provided through two CSV files: gene marker information and cultivar gene information.

The `RGB` portion of the command line identifies the RGB data/sensor as the files of interest.
Additional sensors and data products are supported; these are listed when running the script with the `-h` flag.

The dates of interest are specified as `"2018-05-08"`; in this case only one date is specified.

Finally, the output file is specified: `/home/myself/test.db`

## Command Line Parameters
Please run the script with the `-h` option to get an updated list of options that are available.

### Mandatory parameters
* sensors: a comma separated list of sensors.
Run the script with the `-h` command line flag to see the supported sensors.
* dates: a comma separated list of dates and date ranges.
All date values are expected for match the format of `YYYY-MM-DD`.
Date ranges consist of the lower date and upper date, separated by a colon.
* output_file: the full path to the output file to write to

### Optional parameters
* --BETYDB_URL: the URL of the BETYdb instance to access
* --BETYDB_KEY: the key to use when accessing the BETYdb instance
* --BRAPI_URL: an alternative BRAPI endpoint to use 
* --debug: enables displaying of debug messages; setting this will display quite a bit of information
* --experiment_json: path to the file containing the experiment JSON previously fetched from BETYdb.
When specified, the BETYDB_URL and BETYDB_KEY don't need to be specified (although a warning is generated).
This file is used to satisfy all experiment data for the dates specified
* --cultivar_json: path to the file containing the complete set of cultivar JSON retrieved from BRAPI.
This file is used to satisfy all cultivar data for the dates specified
* --gene_marker_file: the path to the CSV file containing genetic information.
There is no inherent column information expected; the table will be generated based upon the CSV header.
* --gene_marker_file_key: the numeric column index, starting at zero, containing the key values (defaults to column zero)
* --gene_marker_file_ignore: the number of starting lines to ignore in gene_marker_file file before the header (defaults to no rows skipped)
* --cultivar_gene_map_file: the path to the cultivar to gene mapping file.
There is no inherent column information expected; the table will be generated based upon the CSV header.
* --cultivar_gene_file_key: the numeric column index, starting at zero, containing the key values (defaults to column zero)
* --cultivar_gene_map_file_ignore: the number of starting lines to ignore in cultivar_gene_map_file file before the header (defaults to no rows skipped)

## Environment variables <a name="environ_vars" />
For security purposes it's possible to specify the BETYdb and BRAPI connection information using environment variables.
The environment variable names of `BETTYDB_URL`, `BETYDB_KEY`, and `BRAPI_URL` are supported.
If one or more of these are specified, they will be used instead of the default values.
These environment variables can be overridden by their associated command line arguments.

## Dependencies <a name="dependencies" />
Calls are made to the BETYdb `API` to extract experiment information.
If a suitable JSON file is available locally, it can be specified on the command line and bypass the BETYdb API call.

Calls are also made to the TERRA REF `BRAPI` interface to retrieve the experiment, plot, and cultivar information. 

## Database schema <a name="schema" />
The purpose of the script is to generate a database that can be used for file discovery.
This section outlines the views and underlying tables that are available.

Note that the database is restricted to the dates specified when it was built.

### View: unified
Presents a unified view of the data loaded into the database.

Genetic information is only included in this view if a cultivar gene map CSV file was provided.

| file_id | folder | filename | format | sensor | start_time | finish_time | gantry_x | gantry_y | gantry_z | plot_id | plot_name | season | plot_bb_min_lat | plot_bb_min_lon | plot_bb_max_lat | plot_bb_max_lon | cultivar_name | weather_timestamp | temperature | illuminance | precipitation | sun_direction | wind_speed | wind_direction | relative_humidity | <gene data> | 
|---------|--------|----------|--------|--------|------------|-------------|----------|----------|----------|---------|-----------|--------|------------------------|-----------------|-----------------|-----------------|---------------|-------------------|-------------|---------------------------|---------------|---------------|------------|----------------|-------------------|-------------|

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
* plot_id: :unique plot identifier
* plot_name: name of the plot
* season: the name of the season
* plot_bb_min_lat: the minimum latitude (Y) value of the plot's boundary
* plot_bb_min_lon: the minimum longitude (X) value of the plot's boundary
* plot_bb_max_lat: the maximum latitude (Y) value of the plot's boundary 
* plot_bb_max_lon: the maximum longitude (X) value of the plot's boundary
* cultivar_name: the name of the cultivar
* weather_timestamp: the timestamp of the weather capture
* temperature: the temperature in `degrees Celsius`
* illuminance: the brightness in `kilo Lux`
* precipitation: the precipitation in `mm/h`
* sun_direction: the angle of the sun in `degrees`
* wind_speed: the speed of the wind in `m/s`
* wind_direction: the wind direction in `degrees`
* relative_humidity: the relative humidity in `relative humidity percent`
* <gene data>: one or more columns of genetic data, when specified

### View: cultivar_files
This view is intended to map cultivars to specific files.

| plot_id | plot_name | season || plot_bb_min_lat | plot_bb_min_lon | plot_bb_max_lat | plot_bb_max_lon | file_id | folder | filename | format | sensor | start_time | finish_time | gantry_x | gantry_y | gantry_z | cultivar_name |
|---------|-----------|--------|------------------|-----------------|-----------------|-----------------|--------|----------|--------|--------|------------|-------------|----------|----------|----------|---------------|

* plot_id: :unique plot identifier
* plot_name: name of the plot
* season: the name of the season
* plot_bb_min_lat: the minimum latitude (Y) value of the plot's boundary
* plot_bb_min_lon: the minimum longitude (X) value of the plot's boundary
* plot_bb_max_lat: the maximum latitude (Y) value of the plot's boundary 
* plot_bb_max_lon: the maximum longitude (X) value of the plot's boundary
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
* cultivar_name: the name of the cultivar

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

### Table: gene_markers
This table is generated when a gene_markers_file CSV file is specified.
An `id` column is added to the table to assist in tracking the data.
The remaining columns are derived from the header information in the source CSV file.

| id | <gene info 1> | ... | <gene info n> |
|----|---------------|-----|----------------|

* id: the unique ID of each entry
* <gene info 1>: replaced with the name of the first column in the CSV file
* ...: additional columns from the CSV file (assuming there's more than one column)
* <gene info n>: replaced with the name of the last column in the CSV file (assuming there's more than one column)

### Table: cultivar_genes
This table is generated when a cultivar_gene_map_file CSV file is specified.
An `id` column is added to the table to assist in tracking the data.
The remaining columns are derived from the header information in the source CSV file.

| id | <cultivar gene info 1> | ... | <cultivar gene info n> |
|----|------------------------|-----|------------------------|

* id: the unique ID of each entry
* <cultivar gene info 1>: replaced with the name of the first column in the CSV file
* ...: additional columns from the CSV file (assuming there's more than one column)
* <cultivar gene info n>: replaced with the name of the last column in the CSV file (assuming there's more than one column)

