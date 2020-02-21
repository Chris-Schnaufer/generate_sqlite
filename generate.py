#!/usr/bin/env python3
"""Generates a SQLite database for discovering files
"""
import argparse
import csv
from datetime import datetime, timedelta
import json
import logging
import os
import sqlite3
import tempfile
from typing import Callable
from typing import Optional
import webbrowser
import shutil
import re
import requests
from osgeo import ogr
import globus_sdk
from dateutil.parser import parse

GLOBUS_START_PATH = '/ua-mac'
GLOBUS_ENVIRONMENT_LOGGER_PATH = 'raw_data/EnvironmentLogger'
GLOBUS_ENDPOINT = 'Terraref'  # This is dependent upon the user; add a command line argument?
GLOBUS_CLIENT_ID = '80e3a80b-0e81-43b0-84df-125ce5ad6088'  # This script's ID registered with Globus
GLOBUS_LOCAL_ENDPOINT_ID = '3095856a-fd85-11e8-9345-0e3d676669f4'  # Find another way to get this info
GLOBUS_LOCAL_START_PATH = 'globus_data/sqlite'  # another user specific value

LOCAL_ROOT_PATH = '/Users/chris/'   # Works for me but not anyone else; needs to change
LOCAL_SAVE_PATH = os.path.join(LOCAL_ROOT_PATH, GLOBUS_LOCAL_START_PATH) # Needs another approach; user specific
LOCAL_STRIP_PATH = LOCAL_SAVE_PATH + '/'  # Needs another approach; user specific

BETYDB_ENV_URL = 'BETYDB_URL'
BETYDB_ENV_KEY = 'BETYDB_KEY'

BRAPI_URL = 'https://brapi.workbench.terraref.org/brapi/v1'

MAX_INSERT_BEFORE_COMMIT = 1000
PLOT_INCLUSION_FILTERS = {'city': 'Maricopa'}

TERRAREF_TIMESTAMP_REGEX = '[0-9]{4}-[0-9]{2}-[0-9]{2}__[0-9]{2}-[0-9]{2}-[0-9]{2}-[0-9]{1,3}'

# NOTE: SENSOR_MAPS global variable is defined after the mapping functions


def _download_one_file(client: globus_sdk.TransferClient, endpoint_id: str, file_path: str) -> Optional[str]:
    """Handles downloading a single file, if it's not on disk already
    Arguments:
        client: the Globus transfer client to use
        endpoint_id: the ID of the endpoint to access
        file_path: the Globus path to the file
    Return:
        Returns the local path to the file or None if it couldn't be retrieved
    """
    globus_save_path = os.path.join(LOCAL_SAVE_PATH, file_path)
    if not os.path.exists(globus_save_path):
        globus_remote_path = os.path.join(file_path)
        transfer_setup = globus_sdk.TransferData(client, endpoint_id, GLOBUS_LOCAL_ENDPOINT_ID, label="Get metadata", sync_level="checksum")
        transfer_setup.add_item(globus_remote_path, globus_save_path)
        transfer_request = client.submit_transfer(transfer_setup)
        task_result = client.task_wait(transfer_request['task_id'], timeout=600, polling_interval=5)
        if not task_result:
            return None
    return globus_save_path


def _map_rgb_file_to_metadata(client: globus_sdk.TransferClient, endpoint_id: str, file_directory: str, file_name: str) -> Optional[str]:
    """Performs mapping of rgb plot level file to associated JSON metadata file
    Arguments:
        client: the Globus transfer client to use
        endpoint_id: the ID of the endpoint to access
        file_directory: the directory of the file
        file_name: the name of the file to map
    Returns:
        The mapped filename or None if the name can't be mapped
    """
    # eg: rgb_geotiff_L1_ua-mac_2018-05-08__13-10-45-826_left.tif
    #  -> raw_data/stereoTop/2018-05-08/2018-05-08__13-10-45-826/3a45ac5f-67b5-47c6-a34d-89804269871c_metadata.json
    # Note the source file name contains the date string "2018-05-08__13-10-45-826"
    match = re.search(TERRAREF_TIMESTAMP_REGEX, file_name)
    if match:
        date = match[0].split('__')[0]
        folder_path = os.path.join(GLOBUS_START_PATH, 'raw_data/stereoTop', date, match[0])
        for one_entry in client.operation_ls(endpoint_id, path=folder_path):
            if one_entry['name'].endswith('metadata.json'):
                return os.path.join(folder_path, one_entry['name'])
    return None


def _map_las_file_to_metadata(client: globus_sdk.TransferClient, endpoint_id: str, file_directory: str, file_name: str) -> Optional[str]:
    """Performs mapping of las plot level file to associated JSON metadata file
    Arguments:
        client: the Globus transfer client to use
        endpoint_id: the ID of the endpoint to access
        file_directory: the directory of the file
        file_name: the name of the file to map
    Returns:
        The mapped filename or None if the name can't be mapped
    Exceptions:
        Raises RuntimeError if the DTM JSON file can't be found, or can't be downloaded or imported
    """
    # eg: file_directory: /ua-mac/Level_1_Plots/laser3d_las/2018-10-29/MAC Field Scanner Season 7 Range 54 Column 9/
    #     file_name: 3d_101118_sstart_fullfield d0aae7fe-e512-4fde-a434-b989fa93f4f9_merged.las

    # Get the *merged_dtm.json from Globus
    dtm = None
    for one_entry in client.operation_ls(endpoint_id, path=file_directory):
        if one_entry['name'].endswith('merged_dtm.json'):
            dtm_path = _download_one_file(client, endpoint_id, one_entry['name'])
            if not dtm_path:
                raise RuntimeError("Unable to retrieve LAS Merged DTM: %s" % one_entry['name'])
            with open(dtm_path, 'r') as in_file:
                dtm = json.load(in_file)
    if dtm is None:
        raise RuntimeError("Unable to find DTM JSON file associated with '%s'" % os.path.join(file_directory, file_name))

    # Get the first items that's a string
    timestamp = None
    date = None
    for key, value in dtm.items():
        if isinstance(value, str):
            file_name = os.path.basename(value)
            if file_name:
                match = re.search(TERRAREF_TIMESTAMP_REGEX, file_name)
                if match:
                    timestamp = match[0]
                    date = timestamp.split('__')[0]
                    break
    if timestamp is None or date is None:
        raise RuntimeError("Unable to get timestamp associated with file '%s'" % os.path.join(file_directory, file_name))

    # Get the path to the metadata JSON file
    folder_path = os.path.join(GLOBUS_START_PATH, 'raw_data/scanner3DTop', date, timestamp)
    for one_entry in client.operation_ls(endpoint_id, path=folder_path):
        if one_entry['name'].endswith('metadata.json'):
            return os.path.join(folder_path, one_entry['name'])

    return None


SENSOR_MAPS = {
    'RGB': {
        'file_paths': [
            {
                'path': 'Level_1_Plots/rgb_geotiff',
                'ext': ['tif']
            }
        ],
        'metadata_file_mapper': _map_rgb_file_to_metadata
    },
    'Lidar': {
        'file_paths': [
            {
                'path': 'Level_1_Plots/laser3d_las',
                'ext': ['las']
            }
        ],
        'metadata_file_mapper': _map_las_file_to_metadata
    }
}


def add_arguments(parser: argparse.ArgumentParser) -> None:
    """Adds command line arguments to the argument parser
    Arguments:
        parser: the instance to add arguments to
    """
#    parser.add_argument('sensor_paths',
#                        help='comma separated list of sensors and paths include in "<sensor>=<file ext>:<path>" format')
    parser.add_argument('sensors', help='comma separated list of sensors to include (one or more of: ' + ','.join(SENSOR_MAPS.keys()) + ')')
    parser.add_argument('dates', help='comma separated list of dates and ranges of dates (see below)')
    parser.add_argument('--BETYDB_URL', dest="betydb_url", help="the URL to the  BETYdb server to query")
    parser.add_argument('--BETYDB_KEY', dest="betydb_key", help="the BETYdb key to use when accessing the BETYdb server")
    parser.add_argument('--BRAPI_URL', dest="brapi_url", help="the URL to BRAPI server to use (default %s)" % BRAPI_URL)
    parser.add_argument('output_file', help="the output SQLite file")
    parser.add_argument('--debug', action="store_true", help="turns on debugging messages")
    parser.add_argument('--globus_endpoint',
                        help="override default remote Globus endpoint by name (default '%s')" % GLOBUS_ENDPOINT,
                        default=GLOBUS_ENDPOINT)
    parser.add_argument('--experiment_json', '-e', help="path to JSON file with experiment data from BETYdb")
    parser.add_argument('--cultivar_json', '-c', help="path to JSON file with cultivar data from BETYdb")
    parser.add_argument('--gene_marker_file', help='path to the gene marker CSV file')
    parser.add_argument('--gene_marker_file_key', type=int, default=0,
                        help='column index in gene marker file to use as a key (columns start at 0 - defaults to 0)')
    parser.add_argument('--gene_marker_file_ignore', type=int, help='the number of rows to ignore from the start of the gene marker file')
    parser.add_argument('--cultivar_gene_map_file', help='path to the CSV file mapping cultivars to gene markers')
    parser.add_argument('--cultivar_gene_file_key', type=int, default=0,
                        help='column index in cultivar gene file identifying cultivars (columns start at 0 - defaults to 0)')
    parser.add_argument('--cultivar_gene_map_file_ignore', type=int,
                        help='the number of rows to ignore from the start of the cultivar gene map file')

    parser.epilog = 'All specified dates need to be in "YYYY-MM-DD" format; date ranges are two dates separated by a '\
        'colon (":") and are inclusive.'


#def prepare_sensor_paths(sensor_paths_arg: str) -> tuple:
#    """Prepares the sensor and associated path pairs for processing
#    Arguments:
#        sensor_paths_arg: the command line parameter value
#    Return:
#        Returns a tuple containing tuple pairs of sensors with their associated paths and extensions (as a list of dict):
#        ((sensor1, [{'path': path1, 'ext': [ext1, ext2, ...]}, {'path': path2, 'ext': [ext n, ...]}, ...], (sensor2, ...), ...)
#    Exceptions:
#        RuntimeError is raised if a problem is found
#    """
#    all_sensor_paths = sensor_paths_arg.split(',')
#    if not all_sensor_paths:
#        raise RuntimeError("Sensor paths parameter is missing values")
#
#    def path_match(new: dict, cur: dict) -> bool:
#        """Internal to this function: checks if the paths can be considered a match
#        Arguments:
#            new: the new path to check
#            cur: the existing path to check against
#        Return:
#            True if the paths can be considered a match, False otherwise
#        """
#        return new['path'] == cur['path'] and (new['ext'][0] in cur['ext'] or '*' in cur['ext'])
#
#    def get_path_index(path: str, path_list: list) -> int:
#        """Returns the found index of the path in the list of possibilities
#        Arguments:
#            path: the path to find
#            path_list: the list of paths to search (each entry is a dict of 'path' and 'ext' list)
#        Return:
#            Returns the index of a match, or -1 if a match wasn't found
#        """
#        return next((idx for idx, values in enumerate(path_list) if values['path'] == path), -1)
#
#    sensors = []
#    paths = []
#    problems = False
#    for one_pair in all_sensor_paths:
#        if '=' not in one_pair:
#            logging.warning("Sensor path pair is invalid: '%s'", one_pair)
#            problems = True
#            continue
#
#        one_sensor, one_path = (val.strip() for val in one_pair.split('='))
#        if not one_sensor or not one_path:
#            logging.warning("Sensor path pair is only partially formed: '%s'", one_pair)
#            problems = True
#            continue
#
#        # Get the file extension value
#        if ':' in one_path:
#            cur_path, cur_ext = one_path.split(':')
#        else:
#            cur_path = one_path
#            cur_ext = None
#        if not cur_ext:
#            cur_ext = '*'
#
#        # Store the sensor and path information (with paths as a list off sensors)
#        sensor_path = {'path': cur_path, 'ext': [cur_ext]}
#        if one_sensor not in sensors:
#            sensors.append(one_sensor)
#            paths.append([sensor_path])
#        else:
#            # This sensor has more than one path associated with it
#            sensor_index = sensors.index(one_sensor)
#            found_path_match = [path_match(sensor_path, test_path) for test_path in paths[sensor_index]]
#            if True not in found_path_match:
#                path_idx = get_path_index(sensor_path['path'], paths[sensor_index])
#                if path_idx < 0:
#                    paths[sensor_index].append(sensor_path)
#                else:
#                    paths[sensor_index][path_idx]['ext'].extend(sensor_path['ext'])
#            else:
#                logging.debug("Duplicate path entry found: %s", str(one_pair))
#
#    if problems:
#        raise RuntimeError("Errors found while processing command line sensor paths. Please correct and try again")
#
#    return tuple((sensors[idx], paths[idx]) for idx in range(0, len(sensors)))


def prepare_sensors(sensors: str) -> tuple:
    """Prepares a list of sensors from a comma separated list of sensors
    Arguments:
        sensors: the comma separated list of sensors
    Return:
        A tuple of all valid sensors
    Exception:
        Raises RuntimeError is no valid sensors were found
    """
    sensor_list = []
    for one_sensor in sensors.split(','):
        cur_sensor = one_sensor.strip()
        if cur_sensor in SENSOR_MAPS:
            sensor_list.append(cur_sensor)
        else:
            logging.warning('Unknown sensor specified: %s', cur_sensor)

    if not sensor_list:
        raise RuntimeError("No know sensors were specified on command line")

    return tuple(sensor_list)


def validate_date(date: str) -> bool:
    """Confirms the date passed in is a valid date
    Arguments:
        date: the date string to confirm
    """
    try:
        valid = date == datetime.strptime(date, "%Y-%m-%d").strftime('%Y-%m-%d')
        if valid:
            # Parser throws a ValueError exception if the date's invalid
            parse(date)
        return valid
    except ValueError:
        pass

    return False


def generate_dates(start_date: str, last_date: str) -> list:
    """Generates the date strings in the date range
    Arguments:
        start_date: the expected first date in the range
        last_date: the expected last date to include in the range
    Return:
        Returns a tuple of the valid dates, with the earliest date first
    """
    one_date = parse(start_date)
    next_date = parse(last_date)

    if one_date < next_date:
        first, last = one_date, next_date
    else:
        first, last = next_date, one_date

    all_dates = []
    cur_date = first
    while cur_date <= last:
        all_dates.append(cur_date.strftime("%Y-%m-%d"))
        cur_date = cur_date + timedelta(days=1)

    return all_dates


def prepare_dates(dates_arg: str) -> tuple:
    """Prepares the dates command line parameter for processing
    Arguments:
        dates_arg: the command line parameter value
    Return:
        Returns an expanded list of dates to include
    Exceptions:
        RuntimeError is raised if a problem is found
    """
    all_dates = dates_arg.split(',')
    if not all_dates:
        raise RuntimeError("Dates parameter is missing values")

    dates = []
    problems = False
    for one_item in all_dates:
        # Check for a single date or a date range
        first_date = one_item
        last_date = one_item
        if ':' in one_item:
            first_date, last_date = one_item.split(':')

        # Determine if we have a single date or a range
        if first_date == last_date:
            cur_date = first_date.strip()
            if cur_date:
                if validate_date(cur_date):
                    dates.append(cur_date)
                else:
                    logging.warning("Invalid date specified: '%s'", cur_date)
                    problems = True
                    continue
        else:
            cur_start = first_date.strip()
            cur_last = last_date.strip()
            if not cur_start or not cur_last:
                logging.warning("Invalid date range specified: '%s'", one_item)
                problems = True
                continue
            if not validate_date(cur_start) or not validate_date(cur_last):
                logging.warning("Invalid dates specified in date range: '%s'", one_item)
                problems = True
                continue
            dates.extend(generate_dates(cur_start, cur_last))

    if problems:
        raise RuntimeError("Errors found while processing command line dates. Please correct and try again")

    return tuple(dates)


def get_betydb_url(betydb_url_arg: str) -> str:
    """Returns the BETYdb URL
    Arguments:
        betydb_url_arg: the command line argument for the BETYdb URL
    Return:
        Returns the found BETYdb URL
    """
    if betydb_url_arg and betydb_url_arg.strip():
        return betydb_url_arg

    env_url = os.environ.get(BETYDB_ENV_URL)
    if not env_url:
        logging.warning("BETYDB_URL environment variable has not been set")

    return env_url


def get_betydb_key(betydb_key_arg: str) -> str:
    """Returns the BETYdb key used to access the URL
    Arguments:
        betydb_key_arg: the command line argument for the BETYdb key
    Return:
        Returns the found BETYdb key
    """
    if betydb_key_arg and betydb_key_arg.strip():
        return betydb_key_arg

    env_key = os.environ.get(BETYDB_ENV_KEY)
    if not env_key:
        logging.warning("BETYDB_KEY environment variable has not been set")

    return env_key


def get_brapi_url(brapi_url_arg: str) -> str:
    """Returns the BRAPI URL to use when fetching data
    Arguments:
        brapi_url_arg: the command line argument for the BRAPI URL
    Return:
        Returns the BRAPI URL to use
    """
    if brapi_url_arg and brapi_url_arg.strip():
        return brapi_url_arg

    return BRAPI_URL


def make_timestamp_instance(timestamp_string: str) -> datetime:
    """Converts a string timestamp to a timestamp object
    Arguments:
        timestamp_string: the timestamp to convert (see Notes)
    Return:
        Returns a timestamp object representing the timestamp passed in
    Notes:
        Only accepts timestamp strings with the following format:
            "MM/DD/YYYY HH:MI:SS"
            "YYYY.MM.DD-HH:MI:SS"
    """
    if '.' in timestamp_string:
        return datetime.strptime(timestamp_string, '%Y.%m.%d-%H:%M:%S')

    return datetime.strptime(timestamp_string, '%m/%d/%Y %H:%M:%S')


def get_experiments_by_dates(dates: tuple, betydb_url: str, betydb_key: str, experiment_json_file: str = None) -> tuple:
    """Retrieves the experiments associated with dates
    Arguments:
        dates: the dates to fetch experiment information on
        betydb_url: the URL to the BETYdb instance to query
        betydb_key: the key to use in association with the BETYdb URL
        experiment_json_file: optional path to json file containing experiment data from BETYdb
    Return:
        A tuple containing the list of experiments matching the dates, a list of dates with their associated experiment
        ID, and a list of dates for which experiments were NOT found
    """
    found_experiments = []
    date_experiment_id = {}
    remaining_dates = dates

    # Get experiments JSON
    if not experiment_json_file or not os.path.exists(experiment_json_file):
        query_params = {'key': betydb_key, 'limit': 'none', 'associations_mode': 'full_info'}

        # Get the experiments and find matches
        url = os.path.join(betydb_url, 'api/v1/experiments')
        result = requests.get(url, params=query_params)
        result.raise_for_status()

        result_json = result.json()
    else:
        with open(experiment_json_file, "r") as in_file:
            result_json = json.load(in_file)
    if 'data' in result_json:
        experiments = result_json['data']
    else:
        raise RuntimeError("Invalid format of returned experiment JSON (missing 'data' key)")

    # Find the ones that match our dates
    for one_exp in experiments:
        exp_data = one_exp['experiment']
        # This is inefficient; it'd be better to keep the date ranges for comparison and not expand them
        exp_dates = generate_dates(exp_data['start_date'], exp_data['end_date'])
        date_matches = tuple(set(exp_dates).intersection(set(remaining_dates)))

        if date_matches:
            found_experiments.append(exp_data)
            remaining_dates = tuple(set(remaining_dates) - set(exp_dates))
            for one_date in date_matches:
                date_experiment_id[one_date] = exp_data['id']

    return found_experiments, date_experiment_id, remaining_dates


def get_cultivars_betydb(betydb_url: str, betydb_key: str, cultivar_json_file: str = None) -> list:
    """Retrieves all the cultivars from BETYdb
    Arguments:
        betydb_url: the URL to the BETYdb instance to query
        betydb_key: the key to use in association with the BETYdb URL
        cultivar_json_file: optional path to json file containing cultivar data from BETYdb
    Return:
        Returns the result of the query
    """
    if not cultivar_json_file or not os.path.exists(cultivar_json_file):
        query_params = {'key': betydb_key, 'limit': 'none', 'associations_mode': 'full_info'}

        # Get the cultivators
        url = os.path.join(betydb_url, 'api/v1/cultivars')
        result = requests.get(url, params=query_params)
        result.raise_for_status()

        result_json = result.json()
    else:
        with open(cultivar_json_file, 'r') as in_file:
            result_json = json.load(in_file)
    if 'data' in result_json:
        return result_json['data']

    raise RuntimeError("Invalid format of returned cultivar JSON (missing 'data' key)")


def get_cultivars_brapi(study_id: str, brapi_url: str) -> list:
    """Retrieves cultivar information from BRAPI on a per study basis
    Arguments:
        study_id: the ID of the study (experiment in BETYdb terms)
        brapi_url: the base BRAPI URL to use when making calls
    Returns:
        Returns the list of results containing the information on the study
    Notes:
        Will make calls until all pages of data are returned for the study
    """
    base_url = os.path.join(brapi_url, 'studies', str(study_id), 'layouts')
    params = {'page': -1}   # Start at -1 since we pre-increment before making a call
    studies_data = []

    # Loop through until we're done
    done = False
    while not done:
        # Making the call to get the data
        params['page'] += 1
        response = requests.get(base_url, params, verify=False)
        response.raise_for_status()

        # Getting and handling the response
        response_json = response.json()
        if not response_json:
            logging.warning("Received an empty JSON response from BRAPI studies. Stopping fetch of studies")
            done = True
            continue

        if 'result' not in response_json or 'data' not in response_json['result']:
            logging.warning("Unknown JSON format received from BRAPI studies request. Stopping fetch of studies")
            done = True
            continue
        if not isinstance(response_json['result']['data'], list):
            logging.warning("BRAPI studies request returned unexpected non-list data type result. Stopping fetch of studies")
            done = True
            continue

        # Merge the data or indicate we are done (due to an empty result)
        if response_json['result']['data']:
            studies_data.extend(response_json['result']['data'])
        else:
            done = True

    return studies_data


def match_cultivar_to_site_betydb(site_id: int, all_cultivars: list) -> Optional[tuple]:
    """Finds the cultivar that matches the site ID
    Arguments:
        site_id: the ID of the site of interest
        all_cultivars: the list of available cultivars
    Return:
        A tuple containing the found cultivar (dict) and the site trait (dict). None is returned if the site ID can't
        be matched
    """
    site_id_str = str(site_id)

    for one_cultivar in all_cultivars:
        if 'cultivar' in one_cultivar and 'traits' in one_cultivar['cultivar']:
            for one_trait in one_cultivar['cultivar']['traits']:
                if 'trait' in one_trait and 'site_id' in one_trait['trait']:
                    if one_trait['trait']['site_id'] == site_id_str:
                        # Return the found item
                        return one_cultivar, one_trait

    logging.warning("Didn't find a cultivar for site: %s", site_id_str)
    return None


def match_cultivar_to_site_brapi(site_id: int, all_cultivars: list) -> Optional[dict]:
    """Finds the cultivar that matches the site ID
    Arguments:
        site_id: the ID of the site of interest
        all_cultivars: the list of available cultivars
    Return:
        A tuple containing the found cultivar (dict) and the site trait (dict). None is returned if the site ID can't
        be matched
    """
    site_id_str = str(site_id)

    for one_cultivar in all_cultivars:
        if 'observationUnitDbId' in one_cultivar:
            if one_cultivar['observationUnitDbId'] == site_id_str:
                # Return the found item
                return one_cultivar

    logging.debug("Didn't find a cultivar for site: %s", site_id_str)
    return None


def get_bounds_from_wkt(wkt: str) -> tuple:
    """Returns the bounds represented by the WKT (Well Known Text) geometry representation
    Arguments:
        wkt: the well know text to return the bounds of
    Return:
        A tuple containing the minimum latitude (Y), minimum longitude (X), maximum latitude (Y), maximum longitude (X) of the
        geometry's bounding box
    Exceptions:
        Raises a RuntimeError if a problem is found
    """
    geometry = ogr.CreateGeometryFromWkt(wkt)
    if not geometry:
        raise RuntimeError("Unable to convert WKT to a working geometry: '%s'" % wkt)

    envelope = geometry.GetEnvelope()
    return envelope[2], envelope[0], envelope[3], envelope[1]


def get_save_experiments(dates: tuple, db_conn: sqlite3.Connection, betydb_url: str, betydb_key: str,
                         brapi_url: str, experiment_json_file: str = None, cultivar_json_file: str = None) -> Optional[tuple]:
    """Retrieves the experiments associated with the dates and saves them into the database
    Arguments:
        dates: the dates to fetch experiment information on
        db_conn: the database to write to
        betydb_url: the URL to the BETYdb instance to query
        betydb_key: the key to use in association with the BETYdb URL
        brapi_url: the BRAPI URL to fetch data from
        experiment_json_file: optional path to json file containing experiment data from BETYdb
        cultivar_json_file: optional path to json file containing cultivar data from BETYDB
    Return:
        A tuple consisting of the list of experiments saved to the SQLite database, a list of their associated cultivars,
        and a dictionary of dates with their associated experiment IDs
    Exceptions:
        A RuntimeError exception is raised when problems are found
    """
    # Get the experiments
    found_experiments, date_experiment_ids, remaining_dates = get_experiments_by_dates(dates, betydb_url, betydb_key,
                                                                                       experiment_json_file)

    # Report any left over dates outside of experiments
    if remaining_dates:
        logging.warning("Unable to find experiments for all dates and date ranges specified: %s", ','.join(remaining_dates))
    if not found_experiments:
        logging.error("No experiments were found for the requested dates")
        return None

    # Get the cultivars
    # all_cultivars = get_cultivars_betydb(betydb_url, betydb_key, cultivar_json_file)
    all_cultivars = {}
    for one_experiment in found_experiments:
        all_cultivars[one_experiment['id']] = get_cultivars_brapi(one_experiment['id'], brapi_url)
        logging.debug("Retrieved %s BRAPI cultivar entries for Experiment: %s", str(len(all_cultivars[one_experiment['id']])),
                      str(one_experiment['name']))

    # Create the experiments table
    exp_cursor = db_conn.cursor()
    exp_cursor.execute('''CREATE TABLE season_info
                          (id INTEGER, plot_name TEXT, season_id INTEGER, season TEXT, cultivar_id INTEGER, 
                          plot_bb_min_lat FLOAT, plot_bb_min_lon FLOAT, plot_bb_max_lat FLOAT, plot_bb_max_lon FLOAT)''')

    # Insert the data and commit every so often
    problem_found = False
    num_inserted = 0
    total_records = 0
    cultivars_matched = []
    for found_exp in found_experiments:
        for one_site in found_exp['sites']:
            # Check for any inclusion filters
            cur_site = one_site['site']
            if PLOT_INCLUSION_FILTERS:
                inclusion_match = True
                for key in PLOT_INCLUSION_FILTERS.keys():
                    if key not in cur_site:
                        inclusion_match = False
                        break
                    if cur_site[key] != PLOT_INCLUSION_FILTERS[key]:
                        inclusion_match = False
                        break
                if not inclusion_match:
                    logging.debug("Filtering out site '%s'", str(cur_site['id']))
                    continue

            # Find out cultivar
            cultivar_match = match_cultivar_to_site_brapi(cur_site['id'], all_cultivars[found_exp['id']])
            if not cultivar_match:
                logging.warning("Unable to find matching cultivar for site: '%s'", str(cur_site))
                problem_found = True
                continue

            # Add our cultivar in if we don't have it yet
            already_added = False
            for one_cultivar in cultivars_matched:
                if one_cultivar['germPlasmDbId'] == cultivar_match['germPlasmDbId']:
                    already_added = True
                    break
            if not already_added:
                cultivars_matched.append(cultivar_match)

            # Get our plot bounding points
            min_lat, min_lon, max_lat, max_lon = get_bounds_from_wkt(cur_site['geometry'])

            if 'sitename' in cur_site:
                site_name = cur_site['sitename']
            else:
                site_name = "unknown %s" % str(cur_site['id'])

            exp_cursor.execute("INSERT INTO season_info VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               [cur_site['id'], site_name, found_exp['id'], found_exp['name'], cultivar_match['germPlasmDbId'],
                                min_lat, min_lon, max_lat, max_lon])

            num_inserted += 1
            total_records += 1
            if num_inserted >= MAX_INSERT_BEFORE_COMMIT:
                db_conn.commit()
                num_inserted = 0

    # Create an index
    exp_cursor.execute("CREATE UNIQUE INDEX 'season_info_index' on 'season_info' ('id', 'cultivar_id' asc)")

    db_conn.commit()
    exp_cursor.close()

    # Handle problems
    if problem_found:
        raise RuntimeError("Problems found processing experiments - unable to continue")
    if total_records <= 0:
        logging.warning("No experiments records were written")

    logging.debug("Wrote %s experiments records", str(total_records))
    return found_experiments, cultivars_matched, date_experiment_ids


def save_cultivars(cultivars: list, db_conn: sqlite3.Connection) -> None:
    """Saves the cultivars to the database
    Arguments:
        cultivars: the list of cultivars to save
        db_conn: the database to write to
    """
    # Create the cultivars table
    cult_cursor = db_conn.cursor()
    cult_cursor.execute('''CREATE TABLE cultivars
                          (id INTEGER, name TEXT)''')

    # Write to the table
    num_inserted = 0
    total_records = 0
    for one_cultivar in cultivars:
        cult_cursor.execute("INSERT INTO cultivars VALUES(?, ?)", [one_cultivar['germPlasmDbId'], one_cultivar['germplasmName']])

        num_inserted += 1
        total_records += 1
        if num_inserted >= MAX_INSERT_BEFORE_COMMIT:
            db_conn.commit()
            num_inserted = 0

    # Create an index
    cult_cursor.execute("CREATE UNIQUE INDEX 'cultivars_index' on 'cultivars' ('id', 'name' asc)")

    db_conn.commit()
    cult_cursor.close()

    if total_records <= 0:
        logging.warning("No cultivar records were written")
    logging.debug("Wrote %s cultivar records", str(total_records))


def globus_get_authorizer() -> globus_sdk.RefreshTokenAuthorizer:
    """Returns Globus authorization information (requires user interaction)
    Return:
        The authorizer instance
    """
    auth_client = globus_sdk.NativeAppAuthClient(GLOBUS_CLIENT_ID)
    auth_client.oauth2_start_flow(refresh_tokens=True)

    authorize_url = auth_client.oauth2_get_authorize_url()
    print("Authorization URL: %s" % authorize_url)
    webbrowser.open(authorize_url)

    get_input = getattr(__builtins__, 'raw_input', input)
    auth_code = get_input('Enter the authorization code: ').strip()

    token_response = auth_client.oauth2_exchange_code_for_tokens(auth_code)
    transfer_info = token_response.by_resource_server['transfer.api.globus.org']

    return globus_sdk.RefreshTokenAuthorizer(transfer_info['refresh_token'], auth_client,
                                             access_token=transfer_info['access_token'],
                                             expires_at=transfer_info['expires_at_seconds'])


def globus_get_files_info(client: globus_sdk.TransferClient, endpoint_id: str, files_path: str,
                          extensions: list, metadata_file_mapper: Callable) -> Optional[list]:
    """Loads the files found on the path and returns their information
    Arguments:
        client: the Globus transfer client to use
        endpoint_id: the ID of the endpoint to access
        files_path: the path to load file information from
        extensions: a list of acceptable filename extensions (can be wildcard '*')
        metadata_file_mapper: function to map a file name to its metadata file
    Return:
        Returns a list of files associated with the file path
    """
    file_details = []
    json_file = None

    # Load all the files in the folder that are filtered in by extension, or are metadata JSON
    for one_entry in client.operation_ls(endpoint_id, path=files_path):
        file_format = os.path.splitext(one_entry['name'])[1]
        if file_format:
            file_format = file_format.lstrip('.')

        # Check for extension matching (we always keep metadata JSON files)
        match_found = one_entry['name'].endswith('_metadata.json')
        logging.debug("HACK: file info: '%s' '%s'  Match: %s  Exts: %s %s", files_path, one_entry['name'], str(match_found), str(extensions), str(file_format))
        for one_ext in extensions:
            if one_ext in ('*', file_format):
                match_found = True
                break

        if not match_found:
            logging.debug("Skipping over file due to non-matching extension: %s", one_entry['name'])
            continue

        # Prepare the file information
        file_info = {
            'directory': files_path,
            'filename': one_entry['name'],
            'format': file_format
        }
        file_details.append(file_info)

        if one_entry['name'].endswith('metadata.json'):
            json_file = one_entry['name']

    # If we don't have anything, return nothing
    if not file_details:
        return None

    # Fill in each file's json file entry
    missing_json_files = False
    for one_file in file_details:
        if one_file['filename'].endswith('_metadata.json'):
            continue

        if not json_file:
            if metadata_file_mapper:
                logging.debug("Calling metadata file mapper with: '%s' '%s'", one_file['directory'], one_file['filename'])
                json_file = metadata_file_mapper(client, endpoint_id, one_file['directory'], one_file['filename'])
            if not json_file:
                missing_json_files = True
                logging.info("Unable to find JSON file for file %s", os.path.join(one_file['directory'], one_file['filename']))
        if json_file:
            one_file['json_file'] = json_file

    if missing_json_files:
        raise RuntimeWarning("Missing metadata JSON files")

    return file_details


def globus_get_files_details(client: globus_sdk.TransferClient, endpoint_id: str, date_files_info: dict,
                             json_file_list: list) -> Optional[dict]:
    """Gets the details of the files in the list
    Arguments:
        client: the Globus transfer client to use
        endpoint_id: the ID of the endpoint to access
        date_files_info: list of file information
        json_file_list: the list of JSON files to fetch
    Return:
        Returns an updated list of file details
    """
    # Fetch metadata and pull information out of it
    file_transfers = {}
    for json_file in json_file_list:
        globus_save_path = os.path.join(LOCAL_SAVE_PATH, os.path.basename(json_file))
        if not os.path.exists(globus_save_path):
            globus_remote_path = json_file
            file_transfers[globus_remote_path] = globus_save_path
    if file_transfers:
        transfer_setup = globus_sdk.TransferData(client, endpoint_id, GLOBUS_LOCAL_ENDPOINT_ID, label="Get metadata", sync_level="checksum")
        for remote_path, save_path in file_transfers.items():
            transfer_setup.add_item(remote_path, save_path)
        transfer_request = client.submit_transfer(transfer_setup)
        task_result = client.task_wait(transfer_request['task_id'], timeout=600, polling_interval=5)
        if not task_result:
            raise RuntimeError("Unable to retrieve JSON metadata: %s" % ",".join(json_file_list))
    del file_transfers

    return_info = {}
    for one_date, file_list in date_files_info.items():
        return_info[one_date] = []
        for one_file in file_list:
            if 'json_file' not in one_file:
                logging.debug("   No loading details for file with no json: %s", one_file['filename'])
                return_info[one_date].append(one_file)
                continue

            variable_metadata = {}
            fixed_metadata = {}
            local_path = os.path.join(LOCAL_SAVE_PATH, os.path.basename(one_file['json_file']))
            logging.debug("Loading JSON file %s for file %s", local_path, one_file['filename'])
            with open(local_path, 'r') as in_file:
                metadata = json.load(in_file)
                if 'lemnatec_measurement_metadata' in metadata:
                    lmm = metadata['lemnatec_measurement_metadata']
                    for one_key in ['gantry_system_variable_metadata', 'sensor_variable_metadata']:
                        if one_key in lmm:
                            variable_metadata[one_key] = lmm[one_key]
                    for one_key in ['gantry_system_fixed_metadata', 'sensor_fixed_metadata']:
                        if one_key in lmm:
                            fixed_metadata[one_key] = lmm[one_key]

            pos_x, pos_y, pos_z, start_time = None, None, None, None
            if 'gantry_system_variable_metadata' in variable_metadata:
                gsvm = variable_metadata['gantry_system_variable_metadata']
                if 'position x [m]' in gsvm:
                    pos_x = gsvm['position x [m]']
                if 'position y [m]' in gsvm:
                    pos_y = gsvm['position y [m]']
                if 'position z [m]' in gsvm:
                    pos_z = gsvm['position z [m]']
                if 'time' in gsvm:
                    start_time = gsvm['time']

            # Update the file information
            more_details = {'local_json_file': local_path}
            if variable_metadata:
                more_details['variable_metadata'] = variable_metadata
            if fixed_metadata:
                more_details['fixed_metadata'] = fixed_metadata
            if pos_x:
                more_details['gantry_x'] = pos_x
            if pos_y:
                more_details['gantry_y'] = pos_y
            if pos_z:
                more_details['gantry_z'] = pos_z
            if start_time:
                more_details['start_time'] = start_time
                more_details['finish_time'] = start_time

            return_info[one_date].append({**more_details, **one_file})

    return return_info


#def local_get_files_details(files_path: str) -> Optional[list]:
#    """Loads the files found on the path and returns their information
#    Arguments:
#        files_path: the path to load file information from
#    """
#    file_details = []
#    json_file = None
#
#    for one_entry in os.listdir(files_path):
#        file_format = os.path.splitext(one_entry)[1]
#        if file_format:
#            file_format = file_format.lstrip('.')
#
#        if not files_path.startswith(LOCAL_STRIP_PATH):
#            raise RuntimeError("Expected file path to start with %s not %s" % (LOCAL_STRIP_PATH, files_path))
#
#        globus_path = os.path.join(GLOBUS_START_PATH, "raw_data", files_path[len(LOCAL_STRIP_PATH):])
#        file_info = {
#            'directory': globus_path,
#            'filename': one_entry,
#            'format': file_format
#        }
#        file_details.append(file_info)
#
#        if one_entry.endswith('metadata.json'):
#            json_file = one_entry
#
#    if not json_file:
#        if file_details:
#            raise RuntimeWarning("No metadata JSON file found in folder %s" % files_path)
#        return None
#
#    # Pull information out of metadata file
#    variable_metadata = {}
#    fixed_metadata = {}
#    with open(os.path.join(files_path, json_file), 'r') as in_file:
#        metadata = json.load(in_file)
#        if 'lemnatec_measurement_metadata' in metadata:
#            lmm = metadata['lemnatec_measurement_metadata']
#            for one_key in ['gantry_system_variable_metadata', 'sensor_variable_metadata']:
#                if one_key in lmm:
#                    variable_metadata[one_key] = lmm[one_key]
#            for one_key in ['gantry_system_fixed_metadata', 'sensor_fixed_metadata']:
#                if one_key in lmm:
#                    fixed_metadata[one_key] = lmm[one_key]
#
#    pos_x, pos_y, pos_z, start_time = None, None, None, None
#    if 'gantry_system_variable_metadata' in variable_metadata:
#        gsvm = variable_metadata['gantry_system_variable_metadata']
#        if 'position x [m]' in gsvm:
#            pos_x = gsvm['position x [m]']
#        if 'position y [m]' in gsvm:
#            pos_y = gsvm['position y [m]']
#        if 'position z [m]' in gsvm:
#            pos_z = gsvm['position z [m]']
#        if 'time' in gsvm:
#            start_time = gsvm['time']
#
#    # Update the file information
#    more_details = {}
#    if variable_metadata:
#        more_details['variable_metadata'] = variable_metadata
#    if fixed_metadata:
#        more_details['fixed_metadata'] = fixed_metadata
#    if pos_x:
#        more_details['gantry_x'] = pos_x
#    if pos_y:
#        more_details['gantry_y'] = pos_y
#    if pos_z:
#        more_details['gantry_z'] = pos_z
#    if start_time:
#        more_details['start_time'] = start_time
#        more_details['finish_time'] = start_time
#
#    for idx, values in enumerate(file_details):
#        file_details[idx] = {**more_details, **values}
#
#    return file_details


def globus_get_files(client: globus_sdk.TransferClient, endpoint_id: str, sensor_path: str, extensions: list, date_experiment_ids: dict,
                     metadata_file_mapper: Callable) -> dict:
    """Returns a list of files on the endpoint path that match the dates provided
    Arguments:
        client: the Globus transfer client to use
        endpoint_id: the ID of the endpoint to access
        sensor_path: the sensor specific path
        extensions: a list of acceptable filename extensions (can be wildcard '*')
        date_experiment_ids: dates with their associated experiment ID
        metadata_file_mapper: function to map a file name to its metadata file
    Return:
        Returns a dictionary with dates as keys, each associated with a list of informational dict's on the files found
    """
    found_files = {}
    working_file_set = {}
    download_file_list = []
    base_path = os.path.join('/-', GLOBUS_START_PATH, sensor_path)
    for one_date in date_experiment_ids.keys():
        cur_path = os.path.join(base_path, one_date)
        logging.debug("Globus path: %s", cur_path)
        path_contents = None
        try:
            path_contents = client.operation_ls(endpoint_id, path=cur_path)
        except globus_sdk.exc.TransferAPIError:
            logging.error("Continuing after TransferAPIError Exception caught for: '%s'", cur_path)
            continue

        for one_entry in path_contents:
            if one_entry['type'] == 'dir':
                sub_path = os.path.join(cur_path, one_entry['name'])
                logging.debug("Globus remote file path: %s", sub_path)
                cur_files = globus_get_files_info(client, endpoint_id, sub_path, extensions, metadata_file_mapper)
                if cur_files:
                    logging.debug("Found %s files for sub path: %s with extensions %s", str(len(cur_files)), sub_path, str(extensions))
                    if one_date not in working_file_set:
                        working_file_set[one_date] = cur_files
                    else:
                        working_file_set[one_date].extend(cur_files)

                    for one_file in cur_files:
                        if 'json_file' in one_file:
                            if one_file['json_file'] not in download_file_list:
                                download_file_list.append(os.path.join(one_file['directory'], one_file['json_file']))
                else:
                    logging.debug("Found 0 files for sub path: %s", sub_path)

            # Only download files when we have a group of them
            if len(download_file_list) >= 10:
                logging.info("Have 100 files to download - getting file details")
                new_details = globus_get_files_details(client, endpoint_id, working_file_set, download_file_list)
                for cur_date in new_details:
                    if cur_date not in found_files:
                        found_files[cur_date] = new_details[cur_date]
                    else:
                        found_files[cur_date].extend(new_details[cur_date])
                working_file_set = {}
                download_file_list = []

    if len(download_file_list) > 0:
        logging.info("Have %s remaining files to download - getting file details", str(len(download_file_list)))
        new_details = globus_get_files_details(client, endpoint_id, working_file_set, download_file_list)
        for cur_date in new_details:
            if cur_date not in found_files:
                found_files[cur_date] = new_details[cur_date]
            else:
                found_files[cur_date].extend(new_details[cur_date])

    return found_files


def local_get_files(file_paths: list, date_experiment_ids: dict) -> dict:
    """Returns a list of files on the endpoint path that match the dates provided
    Arguments:
        file_paths: the list of file paths and associated extensions
        date_experiment_ids: dates with their associated experiment ID
    Return:
        Returns a dictionary with dates as keys, each associated with a list of informational dict's on the files found
    """
    raise RuntimeError("local_get_files is not implemented at this time - needs updating")
#    found_files = {}
#    base_path = sensor_path
#    found_local_folders = 0
#    for one_date in date_experiment_ids.keys():
#        cur_path = os.path.join(base_path, one_date)
#        logging.debug("Local path: %s", cur_path)
#        path_contents = os.listdir(cur_path)
#        for one_entry in path_contents:
#            sub_path = os.path.join(cur_path, one_entry)
#            if os.path.isdir(sub_path):
#                found_local_folders += 1
#                cur_files = local_get_files_details(sub_path)
#                if cur_files:
#                    if one_date not in found_files:
#                        found_files[one_date] = cur_files
#                    else:
#                        found_files[one_date].extend(cur_files)
#                else:
#                    logging.debug("Found 0 files for sub path: %s", sub_path)
#
#    logging.debug("Found %s local folders", str(found_local_folders))
#    return found_files


def map_file_to_plot_id(file_path: str, season_id: str, seasons: list) -> str:
    """Find the plot that is associated with the file
    Arguments:
        file_path: the path to the file
        season_id: the ID of the season associated with the file
        seasons: the list of seasons
    Return:
        Returns the found plot ID
    Exceptions:
        Raises RuntimeError if the plot ID isn't found
    """
    found_plot_id = None
    file_parts = file_path.split('/')
    for one_season in seasons:
        if 'id' not in one_season or 'sites' not in one_season or not one_season['id'] == season_id:
            continue
        for one_site in one_season['sites']:
            if 'site' not in one_site or 'sitename' not in one_site['site']:
                continue
            if one_site['site']['sitename'] in file_parts:
                found_plot_id = one_site['site']['id']
                break

    if found_plot_id is None:
        raise RuntimeError("Unable to find plot ID for file %s" % file_path)
    return found_plot_id


def globus_get_save_files(globus_authorizer: globus_sdk.RefreshTokenAuthorizer, remote_endpoint: str, sensors: tuple,
                          seasons: list, date_season_ids: dict, db_conn: sqlite3.Connection) -> dict:
    """Fetches file information associated with the sensors and dates from Globus and updates the database
    Arguments:
        globus_authorizer: the Globus authorization instance
        remote_endpoint: the remote endpoint to access
        sensors: a tuple of sensors to work on
        seasons: the list of seasons
        date_season_ids: dates with their associated season ID
        db_conn: the database to write to
    Return:
        Returns a dictionary of file IDs, and their associated start and finish timestamps as a tuple
    """
    files_timestamp = {}

    # Prepare to fetch file information from Globus
    trans_client = globus_sdk.TransferClient(authorizer=globus_authorizer)

    # Find the remote ID
    endpoint_id = None
    for endpoint in trans_client.endpoint_search(filter_scope='shared-with-me'):
        if 'display_name' in endpoint and endpoint['display_name'] == remote_endpoint:
            endpoint_id = endpoint['id']
            break
        if 'canonical_name' in endpoint and endpoint['canonical_name'] == remote_endpoint:
            endpoint_id = endpoint['id']
            break
    if not endpoint_id:
        raise RuntimeError("Unable to find remote endpoint: %s" % remote_endpoint)

    # Create the table for file information
    file_cursor = db_conn.cursor()
    file_cursor.execute('''CREATE TABLE files
                          (id INTEGER, folder TEXT, filename TEXT, format TEXT, sensor TEXT, start_time TEXT, finish_time TEXT,
                           gantry_x FLOAT, gantry_y FLOAT, gantry_z FLOAT, plot_id INTEGER, season_id INTEGER)''')

    # Loop through each sensor and dates and get the associated file information
    num_inserted = 0
    total_records = 0
    file_id = 1
    try:
        for one_sensor in sensors:
            sensor = one_sensor
            paths = SENSOR_MAPS[one_sensor]['file_paths']
            for one_path in paths:
                if SENSOR_MAPS[one_sensor]['metadata_file_mapper']:
                    mfm = SENSOR_MAPS[one_sensor]['metadata_file_mapper']
                else:
                    mfm = None
                files = None
                files = globus_get_files(trans_client, endpoint_id, one_path['path'], one_path['ext'], date_season_ids, mfm)
                if not files:
                    logging.warning("Unable to find files for dates for sensor %s", sensor)
                    continue

                for one_date in files.keys():
                    date_files = files[one_date]
                    season_id = date_season_ids[one_date]
                    for one_file in date_files:
                        plot_id = map_file_to_plot_id(os.path.join(one_file['directory'], one_file['filename']), season_id, seasons)
                        file_cursor.execute('INSERT INTO files VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                                            [file_id, one_file['directory'], one_file['filename'], one_file['format'],
                                             sensor, one_file['start_time'], one_file['finish_time'], one_file['gantry_x'],
                                             one_file['gantry_y'], one_file['gantry_z'], plot_id, season_id])

                        files_timestamp[file_id] = (make_timestamp_instance(one_file['start_time']),
                                                    make_timestamp_instance(one_file['finish_time']))

                        file_id += 1
                        num_inserted += 1
                        total_records += 1
                        if num_inserted >= MAX_INSERT_BEFORE_COMMIT:
                            db_conn.commit()
                            num_inserted = 0

    except Exception as ex:
        logging.error("Exception caught in globus_get_save_files: %s", str(ex))
        if logging.getLogger().level == logging.DEBUG:
            logging.exception(ex)
        raise ex

    # Create the indexes
    file_cursor.execute("CREATE UNIQUE INDEX 'files_index' on 'files' ('id', 'plot_id' ASC)")

    db_conn.commit()
    file_cursor.close()

    if total_records <= 0:
        logging.warning("No file records were written")
    logging.debug("Wrote %s file records", str(total_records))

    return files_timestamp


#def local_get_save_files(sensor_paths: tuple, date_experiment_ids: dict, db_conn: sqlite3.Connection) -> dict:
#    """Locally fetches file information associated with the sensors and dates and updates the database
#    Arguments:
#        sensor_paths: a tuple of sensors and a dict of their associated paths and filename extensions
#        date_experiment_ids: dates with their associated experiment ID
#        db_conn: the database to write to
#    Return:
#        Returns a dictionary of file IDs, and their associated start and finish timestamps as a tuple
#    """
#    files_timestamp = {}
#
#    # Create the table for file information
#    file_cursor = db_conn.cursor()
#    file_cursor.execute('''CREATE TABLE files
#                          (id INTEGER, folder TEXT, filename TEXT, format TEXT, sensor TEXT, start_time TEXT, finish_time TEXT,
#                           gantry_x FLOAT, gantry_y FLOAT, gantry_z FLOAT, season_id INTEGER)''')
#
#    # Loop through each sensor and dates and get the associated file information
#    num_inserted = 0
#    total_records = 0
#    file_id = 1
#    for one_sensor_path in sensor_paths:
#        sensor = one_sensor_path[0]
#        paths = one_sensor_path[1]
#        files = local_get_files(paths, date_experiment_ids)
#        if not files:
#            logging.warning("Unable to find files for dates for sensor %s", sensor)
#            continue
#
#        for one_date in files.keys():
#            date_files = files[one_date]
#            experiment_id = date_experiment_ids[one_date]
#            for one_file in date_files:
#                file_cursor.execute('INSERT INTO files VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
#                                    [file_id, one_file['directory'], one_file['filename'], one_file['format'], sensor,
#                                     one_file['start_time'], one_file['finish_time'], one_file['gantry_x'],
#                                     one_file['gantry_y'], one_file['gantry_z'], experiment_id])
#
#                files_timestamp[file_id] = (make_timestamp_instance(one_file['start_time']),
#                                            make_timestamp_instance(one_file['finish_time']))
#
#                file_id += 1
#                num_inserted += 1
#                total_records += 1
#                if num_inserted >= MAX_INSERT_BEFORE_COMMIT:
#                    db_conn.commit()
#                    num_inserted = 0
#    db_conn.commit()
#    file_cursor.close()
#
#    if total_records <= 0:
#        logging.warning("No file records were written")
#    logging.debug("Wrote %s file records", str(total_records))
#
#    return files_timestamp


def globus_get_all_weather(client: globus_sdk.TransferClient, endpoint_id: str, dates: list) -> dict:
    """Returns a dictionary of all the weather found for the dates provided
    Arguments:
        client: the Globus transfer client to use
        endpoint_id: the ID of the endpoint to access
        dates: the list of dates to get
    Return:
        Returns a dictionary with dates as keys, each associated with a list of informational dict's on the weather for those dates
    """
    found_weather = {}
    base_path = os.path.join('/-', GLOBUS_START_PATH, GLOBUS_ENVIRONMENT_LOGGER_PATH)
    transfer_setup = globus_sdk.TransferData(client, endpoint_id, GLOBUS_LOCAL_ENDPOINT_ID,
                                             label="Get weather", sync_level="checksum")

    # Setup for getting files that aren't local
    dates_files = {}
    file_transfer_needed = False
    for one_date in dates:
        cur_path = os.path.join(base_path, one_date)
        logging.debug("Globus path: %s", cur_path)
        path_contents = client.operation_ls(endpoint_id, path=cur_path)
        dates_files[one_date] = []
        for one_entry in path_contents:
            if one_entry['type'] == 'file':
                json_path = os.path.join(cur_path, one_entry['name'])
                logging.debug("Globus remote file path: %s", json_path)
                globus_save_path = os.path.join(LOCAL_SAVE_PATH, os.path.basename(json_path))
                dates_files[one_date].append(globus_save_path)
                if not os.path.exists(globus_save_path):
                    globus_remote_path = json_path
                    transfer_setup.add_item(globus_remote_path, globus_save_path)
                    file_transfer_needed = True

    # Fetch files if necessary
    if file_transfer_needed:
        transfer_request = client.submit_transfer(transfer_setup)
        task_result = client.task_wait(transfer_request['task_id'], timeout=600, polling_interval=5)
        if not task_result:
            raise RuntimeError("Unable to retrieve weather files from Globus")

    # Loop through and load all the data
    problems_found = False
    for one_date, date_file_list in dates_files.items():
        if date_file_list:
            found_weather[one_date] = []
            logging.debug("Loading %s weather files for date %s", len(date_file_list), one_date)
            for one_file in date_file_list:
                with open(one_file, 'r') as in_file:
                    weather = json.load(in_file)
                    if 'environment_sensor_readings' in weather:
                        for one_reading in weather['environment_sensor_readings']:
                            weather_info = {'timestamp': one_reading['timestamp']}
                            for one_sensor, sensor_readings in one_reading['weather_station'].items():
                                weather_info[one_sensor] = sensor_readings['value']
                            found_weather[one_date].append(weather_info)
                    else:
                        logging.error("Unknown JSON file format for weather file '%s'", one_file)
                        problems_found = True
        else:
            logging.debug("Found no files to load for date %s", one_date)

    if problems_found:
        raise RuntimeError("Unable to complete loading weather data due to previous problems")

    return found_weather


def get_save_weather(globus_authorizer: globus_sdk.RefreshTokenAuthorizer, remote_endpoint: str, date_experiment_ids: dict,
                     db_conn: sqlite3.Connection) -> dict:
    """Retrieves  and  saves weather  data
    Arguments:
        globus_authorizer: the Globus authorization instance
        remote_endpoint: the remote endpoint to access
        date_experiment_ids: dates with their associated experiment ID
        db_conn: the database to write to
    Return:
        Returns a dict of the weather ID and its associated timestamp
    """
    weather_timestamps = {}

    # Prepare to fetch weather information from Globus
    trans_client = globus_sdk.TransferClient(authorizer=globus_authorizer)

    # Find the remote ID
    endpoint_id = None
    for endpoint in trans_client.endpoint_search(filter_scope='shared-with-me'):
        if 'display_name' in endpoint and endpoint['display_name'] == remote_endpoint:
            endpoint_id = endpoint['id']
            break
        if 'canonical_name' in endpoint and endpoint['canonical_name'] == remote_endpoint:
            endpoint_id = endpoint['id']
            break
    if not endpoint_id:
        raise RuntimeError("Unable to find remote endpoint: %s" % remote_endpoint)

    # Create the table for file information
    weather_cursor = db_conn.cursor()
    weather_cursor.execute('''CREATE TABLE weather
                           (id INTEGER, timestamp TEXT, temperature FLOAT, illuminance FLOAT, precipitation FLOAT, sun_direction FLOAT,
                           wind_speed FLOAT, wind_direction FLOAT, relative_humidity FLOAT)''')

    # Loop through each sensor and dates and get the associated file information
    num_inserted = 0
    total_records = 0
    problems_found = 0
    weather_id = 1
    # Load all the data to be found and check for missing dates (aka: missing data) below
    all_weather = globus_get_all_weather(trans_client, endpoint_id, list(date_experiment_ids.keys()))
    for one_date in date_experiment_ids:
        if one_date not in all_weather:
            logging.warning("Unable to find weather data for date %s", one_date)
            problems_found = True
            continue

        for one_weather in all_weather[one_date]:
            weather_cursor.execute('INSERT INTO weather VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)',
                                   [weather_id, one_weather['timestamp'], one_weather['temperature'], one_weather['brightness'],
                                    one_weather['precipitation'], one_weather['sunDirection'], one_weather['windVelocity'],
                                    one_weather['windDirection'], one_weather['relHumidity']])

            weather_timestamps[weather_id] = make_timestamp_instance(one_weather['timestamp'])

            weather_id += 1
            num_inserted += 1
            total_records += 1
            if num_inserted >= MAX_INSERT_BEFORE_COMMIT:
                db_conn.commit()
                num_inserted = 0

    # Create the index
    weather_cursor.execute("CREATE UNIQUE INDEX 'weather_index' ON 'weather' ('id' ASC)")

    db_conn.commit()
    weather_cursor.close()

    if problems_found:
        raise RuntimeError("Unable to retrieve weather data for all dates")

    if total_records <= 0:
        logging.warning("No weather records were written")

    logging.debug("Wrote %s weather records", str(total_records))

    return weather_timestamps


def get_ordered_weather_ids_timestamps(weather_timestamps: dict) -> tuple:
    """Returns a tuple containing the ordered list of weather IDs and their associated timestamps
    Arguments:
        weather_timestamps: the dictionary of weather IDs and their timestamps
    Return:
        A tuple containing an ordered tuple of weather IDs and and ordered tuple of timestamps
    """
    ids = list(weather_timestamps.keys())
    tss = list(weather_timestamps.values())

    ids.sort()
    tss.sort()

    return tuple(ids), tuple(tss)


def find_file_weather_ids(start_ts: datetime, finish_ts: datetime, ordered_weather_ids: tuple, ordered_weather_timestamps: tuple) -> tuple:
    """Finds the minimum and maximum weather timestamps associated with the files start and finish timestamps
    Arguments:
        start_ts: the starting timestamp to look for
        finish_ts: the finishing timestamp to look for
        ordered_weather_ids: the ordered list of weather IDs
        ordered_weather_timestamps: the ordered list of timestamps
    Return:
        A tuple containing the ID of the starting and ending weather timestamps that encompass the file's timestamps
    Notes:
        Assumes the ascending numerical order of weather IDs are directly related to the ascending temporal order of
        the weather timestamp (in other words a larger ID value occurs later than any of the smaller ID values)
    """
    assert len(ordered_weather_ids) == len(ordered_weather_timestamps)

    def b_search(search_timestamp: datetime, ordered_timestamps: tuple) -> tuple:
        """Find the nearest min and max timestamp indexes for the specified search timestamp
        Arguments:
            search_timestamp: the timestamp to look for
            ordered_timestamps: the ordered list of timestamps to search
        Return:
            A tuple containing the min and max indexes encompassing the timestamp
        Notes:
            If a timestamp index can't be found, None is returned in the tuple
        """
        min_index, max_index = None, None

        first_idx = 0
        last_idx = len(ordered_timestamps) - 1

        # Simple cases first (empty tuple, one element tuple)
        if last_idx < first_idx:
            return min_index, max_index
        if first_idx == last_idx:
            if ordered_timestamps[first_idx] == search_timestamp:
                min_index, max_index = first_idx, last_idx
            elif ordered_timestamps[first_idx] < search_timestamp:
                min_index = first_idx
            else:
                max_index = last_idx
            return min_index, max_index

        # Check the cases the loop doesn't handle
        if ordered_timestamps[last_idx] == search_timestamp:
            return last_idx, last_idx
        if ordered_timestamps[last_idx] < search_timestamp:
            return last_idx, None

        # Perform search
        while True:
            mid_idx = int((first_idx + last_idx) / 2)
            if ordered_timestamps[mid_idx] == search_timestamp:
                min_index, max_index = mid_idx, mid_idx
                break
            if ordered_timestamps[mid_idx] < search_timestamp:
                first_idx = mid_idx
            else:
                last_idx = mid_idx
            if last_idx - first_idx <= 1:
                if ordered_timestamps[first_idx] < search_timestamp:
                    min_index = first_idx
                if ordered_timestamps[last_idx] > search_timestamp:
                    max_index = last_idx
                break

        return min_index, max_index

    min_start_index, max_start_index = b_search(start_ts, ordered_weather_timestamps)
    min_finish_index, max_finish_index = b_search(finish_ts, ordered_weather_timestamps)
    if None in (min_start_index, max_start_index, min_finish_index, max_finish_index):
        raise RuntimeError("Unable to find weather associated with file timestamps: %s %s" % (start_ts, finish_ts))
    if min_start_index > min_finish_index:
        raise RuntimeError("Something went horribly wrong finding weather associated with file timestamps: %s %s" % (start_ts, finish_ts))

    start_index = min_start_index
    finish_index = max_finish_index
    # Return the closest weather (comment out the next few lines to keep the bracketing weather)
    if abs((start_ts - ordered_weather_timestamps[min_start_index]).total_seconds()) > \
            abs((start_ts - ordered_weather_timestamps[max_start_index]).total_seconds()):
        start_index = max_start_index
    if abs((finish_ts - ordered_weather_timestamps[min_finish_index]).total_seconds()) < \
            abs((finish_ts - ordered_weather_timestamps[max_finish_index]).total_seconds()):
        finish_index = min_finish_index

    return ordered_weather_ids[start_index], ordered_weather_ids[finish_index]


def create_weather_files_table(weather_timestamps: dict, files_timestamps: dict, db_conn: sqlite3.Connection) -> None:
    """Creates a mapping table between the weather and files
    Arguments:
        weather_timestamps: a dictionary of the weather IDs and their timestamp
        files_timestamps: a dictionary of the file IDs and their starting and finishing timestamps
        db_conn: the database to write to
    """
    # Create the table for file information
    wf_cursor = db_conn.cursor()
    wf_cursor.execute('''CREATE TABLE weather_file_map
                           (id INTEGER, file_id INTEGER, min_weather_id INTEGER, max_weather_id INTEGER)''')

    # Loop through each sensor and dates and get the associated file information
    num_inserted = 0
    total_records = 0
    problems_found = 0
    wf_id = 1

    ordered_weather_ids, ordered_weather_timestamps = get_ordered_weather_ids_timestamps(weather_timestamps)
    logging.info("Looking up %s files for their associated weather", str(len(files_timestamps)))
    for file_id, file_start_finish_ts in files_timestamps.items():
        min_weather_id, max_weather_id = find_file_weather_ids(file_start_finish_ts[0], file_start_finish_ts[1],
                                                               ordered_weather_ids, ordered_weather_timestamps)
        wf_cursor.execute('INSERT INTO weather_file_map VALUES(?, ?, ?, ?)', [wf_id, file_id, min_weather_id, max_weather_id])
        wf_id += 1
        num_inserted += 1
        total_records += 1
        if num_inserted >= MAX_INSERT_BEFORE_COMMIT:
            db_conn.commit()
            num_inserted = 0

    # Create the index
    wf_cursor.execute("CREATE UNIQUE INDEX 'weather_file_map_index' ON 'weather_file_map' ('id' ASC)")
    wf_cursor.execute("CREATE INDEX 'weather_file_map_lookup_index' ON 'weather_file_map' ('min_weather_id', 'max_weather_id' ASC)")

    db_conn.commit()
    wf_cursor.close()

    if problems_found:
        raise RuntimeError("Unable to retrieve weather data for all dates")

    if total_records <= 0:
        logging.warning("No weather records were written")

    logging.debug("Wrote %s weather files mapping records", str(total_records))


def save_gene_markers(gene_marker_file: str, key_column_index: int, file_row_ignore: int, db_conn: sqlite3.Connection) -> dict:
    """Saves the gene marker file into the database
    Arguments:
        gene_marker_file: path to the gene marker file to import
        key_column_index: the index of the column to provide key values
        file_row_ignore: number of rows to ignore at the start of the file
        db_conn: the database to write to
    Return:
        Returns a dictionary of row IDs and the key value
    """
    if not key_column_index:
        key_index = 0
    else:
        key_index = int(key_column_index)
    if not file_row_ignore:
        skip_count = 0
    else:
        skip_count = int(file_row_ignore)

    gene_cursor = db_conn.cursor()

    id_key_map = {}
    created_table = False
    column_order = None
    insert_sql = None
    rows_inserted = 0
    with open(gene_marker_file, 'r') as in_file:
        # Skip over the rows as requested
        if skip_count:
            logging.info('Skipping %s rows at start of gene marker file: %s', str(skip_count), gene_marker_file)
        while skip_count > 0:
            skipped_line = in_file.readline()
            logging.debug("Skipping line: %s", skipped_line)
            skip_count -= 1

        # Process the rest of the file
        reader = csv.DictReader(in_file)
        row_id = 1
        for row in reader:
            # Create the table the first time through
            if not created_table:
                column_order = tuple(row.keys())
                if key_index >= len(column_order):
                    raise RuntimeError('Gene mapping key column index value (%s) is greater than the number of columns: %s' %
                                       (str(key_index), str(len(column_order))))
                column_names = tuple([column.replace(' ', '_').replace('.', '_').lower() for column in column_order])
                logging.info('Creating gene_markers table with columns: %s', str(column_names))
                create_sql = 'CREATE TABLE gene_markers (%s)' % ('id INTEGER, ' + ' TEXT, '.join(column_names) + ' TEXT')
                logging.debug('Create gene_markers SQL: %s', create_sql)
                gene_cursor.execute(create_sql)
                insert_sql = 'INSERT INTO gene_markers(id, ' + ','.join(column_names) + ') VALUES(' + \
                                   ','.join(['?' for _ in range(0, len(column_names) + 1)]) + ')'
                logging.debug('Insert gene_markers SQL: %s', insert_sql)
                created_table = True

            # Add the row
            insert_values = [row_id]
            for one_column in column_order:
                insert_values.append(row[one_column])
            gene_cursor.execute(insert_sql, insert_values)
            id_key_map[row_id] = row[column_order[key_index]]
            rows_inserted += 1
            row_id += 1

    # Create the index
    gene_cursor.execute("CREATE UNIQUE INDEX 'gene_markers_index' ON 'gene_markers' ('id' ASC)")

    db_conn.commit()
    gene_cursor.close()

    if not created_table:
        raise RuntimeError("Empty gene marker file specified")
    logging.info("Inserted %s rows into gene marker table", str(rows_inserted))

    return id_key_map


def save_cultivar_genes(cultivar_gene_file: str, key_column_index: int, file_row_ignore: int, db_conn: sqlite3.Connection) -> tuple:
    """Saves the cultivar to genes file into the database
    Arguments:
        cultivar_gene_file: path to the cultivar gene file to import
        key_column_index: the index of the column to provide key values
        file_row_ignore: number of rows to ignore at the start of the file
        db_conn: the database to write to
    Return:
        Returns the a tuple containing the column name of the cultivar field, and a list of table columns from the file
    """
    if not key_column_index:
        key_index = 0
    else:
        key_index = int(key_column_index)
    if not file_row_ignore:
        skip_count = 0
    else:
        skip_count = int(file_row_ignore)

    cg_cursor = db_conn.cursor()

    cultivar_column_name = None
    created_table = False
    column_order = None
    column_names = None
    insert_sql = None
    rows_inserted = 0
    with open(cultivar_gene_file, 'r') as in_file:
        # Skip over the rows as requested
        if skip_count:
            logging.info('Skipping %s rows at start of cultivar_gene file: %s', str(skip_count), cultivar_gene_file)
        while skip_count > 0:
            skipped_line = in_file.readline()
            logging.debug("Skipping line: %s", skipped_line)
            skip_count -= 1

        # Process the rest of the file
        reader = csv.DictReader(in_file)
        row_id = 1
        for row in reader:
            # Create the table the first time through
            if not created_table:
                column_order = tuple(row.keys())
                if key_index >= len(column_order):
                    raise RuntimeError('Cultivar gene key column index value (%s) is greater than the number of columns: %s' %
                                       (str(key_index), str(len(column_order))))
                column_names = tuple([column.replace(' ', '_').replace('.', '_').lower() for column in column_order])
                cultivar_column_name = column_names[key_index]
                logging.debug("Cultivar column name for cultivar_genes table: %s", cultivar_column_name)
                logging.info('Creating cultivar_genes table with columns: %s', str(column_names))
                create_sql = 'CREATE TABLE cultivar_genes (%s)' %\
                             ('id INTEGER, ' + column_names[0] + ' TEXT, ' + ' INTEGER, '.join(column_names[1:]) + ' INTEGER')
                logging.debug('Create cultivar_genes SQL: %s', create_sql)
                cg_cursor.execute(create_sql)
                insert_sql = 'INSERT INTO cultivar_genes(id, ' + ','.join(column_names) + ') VALUES(' + \
                                   ','.join(['?' for _ in range(0, len(column_names) + 1)]) + ')'
                logging.debug('Insert cultivar_genes SQL: %s', insert_sql)
                created_table = True

            # Add the row
            insert_values = [row_id]
            for one_column in column_order:
                int_match = re.search('^[-+]?\\d+$', row[one_column])
                if row[one_column] == 'No WGS':
                    insert_values.append(-1)
                elif row[one_column] == 'NA':
                    insert_values.append(-2)
                elif int_match is not None:
                    insert_values.append(int(row[one_column]))
                else:
                    insert_values.append(row[one_column])
            cg_cursor.execute(insert_sql, insert_values)
            rows_inserted += 1
            row_id += 1

    # Create the index
    cg_cursor.execute("CREATE UNIQUE INDEX 'cultivar_genes_index' ON 'cultivar_genes' ('id','" + cultivar_column_name + "' ASC)")

    db_conn.commit()
    cg_cursor.close()

    if not created_table:
        raise RuntimeError("Empty cultivar genes file specified")
    logging.info("Inserted %s rows into cultivar genes table", str(rows_inserted))

    return cultivar_column_name, column_names


def create_db_views(db_conn: sqlite3.Connection, cultivar_genes_cultivar_column_name: str,
                    cultivar_genes_all_column_names: list) -> None:
    """Adds views to the database
    Arguments:
        db_conn: the database to write to
        cultivar_genes_cultivar_column_name: the column name in the cultivar_genes table that contains the cultivars
        cultivar_genes_all_column_names: the list of all column names in the cultivar_genes table
    """
    view_cursor = db_conn.cursor()

    # cultivar, plot, season, sensor, date/daterange
    # CREATE TABLE season_info
    #                      (id INTEGER, plot_name TEXT, season_id INTEGER, season TEXT, cultivar_id INTEGER,
    #                      plot_bb_min_lat FLOAT, plot_bb_min_lon FLOAT, plot_bb_max_lat FLOAT, plot_bb_max_lon FLOAT)
    # CREATE TABLE files (id, folder TEXT, filename TEXT, format TEXT, sensor TEXT, start_time TEXT, finish_time TEXT,
    #                            gantry_x FLOAT, gantry_y FLOAT, gantry_z FLOAT, plot_id INTEGER, season_id INTEGER)
    # CREATE TABLE cultivars (id INTEGER, name TEXT)
    # CREATE TABLE weather (id, timestamp TEXT, temperature FLOAT, illuminance FLOAT, precipitation FLOAT,
    #                            sun_direction FLOAT, wind_speed FLOAT, wind_direction FLOAT, relative_humidity FLOAT
    view_cursor.execute('''CREATE VIEW cultivar_files AS select e.id as plot_id, e.plot_name as plot_name, e.season as season,
                        e.plot_bb_min_lat as plot_bb_min_lat, e.plot_bb_min_lon as plot_bb_min_lon,
                        e.plot_bb_max_lat as plot_bb_max_lat, e.plot_bb_max_lon as plot_bb_max_lon,
                        f.id as file_id, f.folder as folder, f.filename as filename, f.format as format, f.sensor as sensor,
                        f.start_time as start_time, f.finish_time as finish_time, f.gantry_x as gantry_x, f.gantry_y as gantry_y,
                        f.gantry_z as gantry_z, c.name as cultivar_name
                        from season_info as e left join files as f on e.id = f.plot_id 
                            left join cultivars as c on e.cultivar_id = c.id''')

    # CREATE TABLE weather_files
    #                            (id INTEGER, file_id INTEGER, min_weather_id INTEGER, max_weather_id INTEGER)
    #view_cursor.execute('''CREATE VIEW weather_files AS select * from (select w.timestamp as timestamp, w.temperature as temperature,
    #                    w.illuminance as illuminance, w.precipitation as precipitation, w.sun_direction as sun_direction,
    #                    w.wind_speed as wind_speed, w.wind_direction as wind_direction, w.relative_humidity as relative_humidity,
    #                    f.id as file_id, f.folder as folder, f.filename as filename, f.format as format, f.sensor as sensor,
    #                    f.start_time as start_time, f.finish_time as finish_time, f.gantry_x as gantry_x, f.gantry_y as gantry_y,
    #                    f.gantry_z as gantry_z
    #                    from weather as w left join weather_file_map as wf on w.id >= wf.min_weather_id and w.id <= wf.max_weather_id
    #                        left join files as f on wf.file_id = f.id) a where not a.file_id is NULL''')
    view_cursor.execute('''CREATE VIEW weather_files AS select * from (select w.timestamp as timestamp, w.temperature as temperature,
                        w.illuminance as illuminance, w.precipitation as precipitation, w.sun_direction as sun_direction,
                        w.wind_speed as wind_speed, w.wind_direction as wind_direction, w.relative_humidity as relative_humidity, 
                        f.id as file_id, f.folder as folder, f.filename as filename, f.format as format, f.sensor as sensor,
                        f.start_time as start_time, f.finish_time as finish_time, f.gantry_x as gantry_x, f.gantry_y as gantry_y,
                        f.gantry_z as gantry_z
                        from weather as w left join weather_file_map as wf on w.id = wf.min_weather_id
                            left join files as f on wf.file_id = f.id) a where not a.file_id is NULL''')

    #CREATE TABLE cultivar_genes (%s)' %\
    #                        ('id INTEGER, ' + column_names[0] + ' TEXT, ' + ' INTEGER, '.join(column_names[1:]) + ' INTEGER')
    # Create a format-able string for optional cultivar genetic information
    view_template = '''CREATE VIEW unified as select f.id as file_id, f.folder as folder, f.filename as filename,
                    f.format as format, f.sensor as sensor, f.start_time as start_time, f.finish_time as finish_time,
                    f.gantry_x as gantry_x, f.gantry_y as gantry_y, f.gantry_z as gantry_z,
                    e.id as plot_id, e.plot_name as plot_name, e.season as season,
                    e.plot_bb_min_lat as plot_bb_min_lat, e.plot_bb_min_lon as plot_bb_min_lon,
                    e.plot_bb_max_lat as plot_bb_max_lat, e.plot_bb_max_lon as plot_bb_max_lon,
                    c.name as cultivar_name,
                    %s
                    w.timestamp as weather_timestamp, w.temperature as temperature,
                    w.illuminance as illuminance, w.precipitation as precipitation, w.sun_direction as sun_direction,
                    w.wind_speed as wind_speed, w.wind_direction as wind_direction, w.relative_humidity as relative_humidity
                    from files f left join season_info as e on f.plot_id = e.id
                        left join cultivars as c on e.cultivar_id = c.id
                        %s
                        left join weather_files as w on f.id = w.file_id'''

    if cultivar_genes_cultivar_column_name:
        join_columns = ['cg.' + one_name for one_name in cultivar_genes_all_column_names
                        if one_name not in ['id', cultivar_genes_cultivar_column_name]]
        view_sql = view_template % (','.join(join_columns) + ', ', 'left join cultivar_genes as cg on c.name = cg.' +
                                    cultivar_genes_cultivar_column_name)
    else:
        view_sql = view_template % ('', '')
    logging.debug('Unified view SQL: %s', view_sql)
    view_cursor.execute(view_sql)

    view_cursor.close()


def generate() -> None:
    """Performs all the steps needed to generate the SQLite database
    Exceptions:
        RuntimeError exceptions are raised when something goes wrong
    """
    parser = argparse.ArgumentParser(description="Generate SQLite database for file discovery")
    add_arguments(parser)
    args = parser.parse_args()

    # Check for debugging
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    logging.debug("Command line args: %s", str(args))

    # Break apart any command line arguments that may be multi-part
#    sensor_paths = prepare_sensor_paths(args.sensor_paths)
    sensors = prepare_sensors(args.sensors)
    dates = prepare_dates(args.dates)
    logging.info("Specified sensors: %s", str(sensors))
    logging.info("Specified dates: %s", str(dates))

    # Get other values we'll need
    betydb_url = get_betydb_url(args.betydb_url)
    betydb_key = get_betydb_key(args.betydb_key)
    brapi_url = get_brapi_url(args.brapi_url)

    # Get our temporary file name
    _, working_filename = tempfile.mkstemp()
    sql_db = sqlite3.connect(working_filename)

    try:
        # Get the Globus authorization
        authorizer = globus_get_authorizer()

        # Generate the experiments table
        experiments, cultivars, date_experiment_ids = get_save_experiments(dates, sql_db, betydb_url, betydb_key, brapi_url,
                                                                           args.experiment_json, args.cultivar_json)

        # Generating the cultivars table
        save_cultivars(cultivars, sql_db)

        # Create the files table
        files_timestamps = globus_get_save_files(authorizer, args.globus_endpoint, sensors, experiments, date_experiment_ids, sql_db)
        #files_timestamps = local_get_save_files(sensor_paths, date_experiment_ids, sql_db)

        # Create the weather table
        weather_timestamps = get_save_weather(authorizer, args.globus_endpoint, date_experiment_ids, sql_db)

        # Create supporting tables
        create_weather_files_table(weather_timestamps, files_timestamps, sql_db)

        # Add gene marker information
        gene_markers_map = None
        cultivar_column_name = None
        cultivar_genes_column_names = None
        if args.gene_marker_file:
            gene_markers_map = save_gene_markers(args.gene_marker_file, args.gene_marker_file_key, args.gene_marker_file_ignore, sql_db)
        if args.cultivar_gene_map_file:
            cultivar_column_name, cultivar_genes_column_names = save_cultivar_genes(args.cultivar_gene_map_file, args.cultivar_gene_file_key,
                                                                                    args.cultivar_gene_map_file_ignore, sql_db)

        # Create the views
        create_db_views(sql_db, cultivar_column_name, cultivar_genes_column_names)

        shutil.move(working_filename, args.output_file)
        sql_db.close()
        sql_db = None
    finally:
        if sql_db:
            sql_db.close()
        del sql_db
        if os.path.exists(working_filename):
            os.unlink(working_filename)


if __name__ == "__main__":
    generate()
