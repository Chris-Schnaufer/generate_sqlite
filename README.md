# Generates a SQLite database used to discover files
This script generates a SQLite database based upon the structure outlined at https://github.com/terraref/reference-data/issues/279

**NOTE**: running this script uses the Globus SDK and requires a web interface to obtain a Native App Authorization Code.
As part of obtaining the code, you will be asked for a label; you may want to reuse the same label every time you run this script.

Please be sure to read up on the assumptions this script makes (see below).

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
