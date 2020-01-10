#!/usr/bin/env python3
"""Generates a SQLite database for discovering files
"""
import _asyncio
import argparse
import asyncio
from datetime import datetime, timedelta
import json
import logging
import os
import sqlite3
import tempfile
from typing import Optional
import webbrowser
import shutil
import requests
from osgeo import ogr
import globus_sdk
from dateutil.parser import parse

RUN_SYNCHRONOUSLY = True

GLOBUS_START_PATH = '/ua-mac'
GLOBUS_ENDPOINT = 'Terraref'
GLOBUS_CLIENT_ID = '80e3a80b-0e81-43b0-84df-125ce5ad6088'
GLOBUS_LOCAL_ENDPOINT_ID = '3095856a-fd85-11e8-9345-0e3d676669f4'
GLOBUS_LOCAL_START_PATH = 'globus_data'

BETYDB_ENV_URL = 'BETYDB_URL'
BETYDB_ENV_KEY = 'BETYDB_KEY'

BRAPI_URL = 'https://brapi.workbench.terraref.org/brapi/v1'

MAX_INSERT_BEFORE_COMMIT = 1000
PLOT_INCLUSION_FILTERS = {'city': 'Maricopa'}


def add_arguments(parser: argparse.ArgumentParser) -> None:
    """Adds command line arguments to the argument parser
    Arguments:
        parser: the instance to add arguments to
    """
    parser.add_argument('sensor_paths',
                        help='comma separated list of sensors and paths include in "<sensor>=<path>" format')
    parser.add_argument('dates', help='command separated list of dates and ranges of dates (see below)')
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

    parser.epilog = 'All specified dates need to be in "YYYY-MM-DD" format; date ranges are two dates separated by a '\
        'colon (":") and are inclusive.'


def prepare_sensor_paths(sensor_paths_arg: str) -> tuple:
    """Prepares the sensor and associated path pairs for processing
    Arguments:
        sensor_paths_arg: the command line parameter value
    Return:
        Returns a tuple containing tuple pairs of sensors with their associated paths (as a list):
        ((sensor1, [path 1, path 2, ...]), (sensor2, [path n, path n+1, ...]), ...)
    Exceptions:
        RuntimeError is raised if a problem is found
    """
    all_sensor_paths = sensor_paths_arg.split(',')
    if not all_sensor_paths:
        raise RuntimeError("Sensor paths parameter is missing values")

    sensors = []
    paths = []
    problems = False
    for one_pair in all_sensor_paths:
        if '=' not in one_pair:
            logging.warning("Sensor path pair is invalid: '%s'", one_pair)
            problems = True
            continue

        one_sensor, one_path = (val.strip() for val in one_pair.split('='))
        if not one_sensor or not one_path:
            logging.warning("Sensor path pair is only partially formed: '%s'", one_pair)
            problems = True
            continue

        # Store the sensor and path (with paths as a list)
        if one_sensor not in sensors:
            sensors.append(one_sensor)
            paths.append([one_path])
        else:
            # This sensor has more than one path associated with it
            path_index = sensors.index(one_sensor)
            if one_path not in paths[path_index]:
                paths[path_index].append(one_path)

    if problems:
        raise RuntimeError("Errors found while processing command line sensor paths. Please correct and try again")

    return tuple((sensors[idx], paths[idx]) for idx in range(0, len(sensors)))


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
    Exceptions:
        RuntimeError is raised if a problem occurs or a BETYdb URL can't be found
    """
    if betydb_url_arg and betydb_url_arg.strip():
        return betydb_url_arg

    env_url = os.environ.get(BETYDB_ENV_URL)
    if not env_url:
        raise RuntimeError("BETYDB_URL environment variable has not been set")

    return env_url


def get_betydb_key(betydb_key_arg: str) -> str:
    """Returns the BETYdb key used to access the URL
    Arguments:
        betydb_key_arg: the command line argument for the BETYdb key
    Return:
        Returns the found BETYdb key
    Exceptions:
        RuntimeError is raised if a problem occurs or a BETYdb key can't be found
    """
    if betydb_key_arg and betydb_key_arg.strip():
        return betydb_key_arg

    env_key = os.environ.get(BETYDB_ENV_KEY)
    if not env_key:
        raise RuntimeError("BETYDB_KEY environment variable has not been set")

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

    logging.debug("Didn't find a cultivar for site: %s", site_id_str)
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
    exp_cursor.execute('''CREATE TABLE experimental_info
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

            exp_cursor.execute("INSERT INTO experimental_info VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                               [cur_site['id'], site_name, found_exp['id'], found_exp['name'], cultivar_match['germPlasmDbId'],
                                min_lat, min_lon, max_lat, max_lon])

            num_inserted += 1
            total_records += 1
            if num_inserted >= MAX_INSERT_BEFORE_COMMIT:
                db_conn.commit()
                num_inserted = 0

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


def globus_get_files_details(client: globus_sdk.TransferClient, endpoint_id: str, files_path: str) -> Optional[list]:
    """Loads the files found on the path and returns their information
    Arguments:
        client: the Globus transfer client to use
        endpoint_id: the ID of the endpoint to access
        files_path: the path to load file information from
    """
    file_details = []
    json_file = None

    for one_entry in client.operation_ls(endpoint_id, path=files_path):
        file_format = os.path.splitext(one_entry['name'])[1]
        if file_format:
            file_format = file_format.lstrip('.')

        file_info = {
            'directory': files_path,
            'filename': one_entry['name'],
            'format': file_format
        }
        file_details.append(file_info)

        if one_entry['name'].endswith('metadata.json'):
            json_file = one_entry['name']

    if not json_file:
        if file_details:
            raise RuntimeWarning("No metadata JSON file found in folder %s" % files_path)
        return None

    # Fetch metadata and pull information out of it
    globus_save_path = os.path.join('/Users/chris/', GLOBUS_LOCAL_START_PATH, os.path.basename(json_file))
    if not os.path.exists(globus_save_path):
        globus_remote_path = os.path.join(files_path, json_file)
        transfer_setup = globus_sdk.TransferData(client, endpoint_id, GLOBUS_LOCAL_ENDPOINT_ID,
                                                 label="Get metadata", sync_level="checksum")
        transfer_setup.add_item(globus_remote_path, globus_save_path)
        transfer_request = client.submit_transfer(transfer_setup)
        task_result = client.task_wait(transfer_request['task_id'], timeout=600, polling_interval=5)

        if not task_result:
            raise RuntimeError("Unable to retrieve JSON metadata: %s" % json_file)

    variable_metadata = {}
    fixed_metadata = {}
    with open(globus_save_path, 'r') as in_file:
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
    more_details = {}
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

    for idx, values in enumerate(file_details):
        file_details[idx] = {**more_details, **values}

    return file_details


def local_get_files_details(files_path: str) -> Optional[list]:
    """Loads the files found on the path and returns their information
    Arguments:
        files_path: the path to load file information from
    """
    file_details = []
    json_file = None

    for one_entry in os.listdir(files_path):
        file_format = os.path.splitext(one_entry)[1]
        if file_format:
            file_format = file_format.lstrip('.')

        file_info = {
            'directory': files_path,
            'filename': one_entry,
            'format': file_format
        }
        file_details.append(file_info)

        if one_entry.endswith('metadata.json'):
            json_file = one_entry

    if not json_file:
        if file_details:
            raise RuntimeWarning("No metadata JSON file found in folder %s" % files_path)
        return None

    # Pull information out of metadata file
    variable_metadata = {}
    fixed_metadata = {}
    with open(os.path.join(files_path, json_file), 'r') as in_file:
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
    more_details = {}
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

    for idx, values in enumerate(file_details):
        file_details[idx] = {**more_details, **values}

    return file_details


async def _globus_file_transfer(client, endpoint_id, remote_path: str, local_save_path: str) -> str:
    """Used to asynchronously fetch a file from Globus
    Arguments:
        client: the Globus transfer client to use
        endpoint_id: the ID of the endpoint to access
        remote_path: path to file in Globus endpoint
        local_save_path: path to save file to
    Return:
        Returns the local path once the file is transferred
    Exceptions:
        Raises RuntimeError if the transfer task failed
    """
    transfer_setup = globus_sdk.TransferData(client, endpoint_id, GLOBUS_LOCAL_ENDPOINT_ID,
                                             label="Get metadata", sync_level="checksum")
    transfer_setup.add_item(remote_path, local_save_path)
    transfer_request = client.submit_transfer(transfer_setup)
    task_result = client.task_wait(transfer_request['task_id'], timeout=600, polling_interval=5)

    if not task_result:
        raise RuntimeError("Unable to retrieve JSON metadata: %s" % os.path.basename(local_save_path))

    return local_save_path


def globus_get_files_details_async(client: globus_sdk.TransferClient, endpoint_id: str, files_path: str) -> Optional[_asyncio.Task]:
    """Loads the files found on the path as a task
    Arguments:
        client: the Globus transfer client to use
        endpoint_id: the ID of the endpoint to access
        files_path: the path to load file information from
    """
    file_details = []
    json_file = None

    for one_entry in client.operation_ls(endpoint_id, path=files_path):
        file_format = os.path.splitext(one_entry['name'])[1]
        if file_format:
            file_format = file_format.lstrip('.')

        file_info = {
            'directory': files_path,
            'filename': one_entry['name'],
            'format': file_format
        }
        file_details.append(file_info)

        if one_entry['name'].endswith('metadata.json'):
            json_file = one_entry['name']

    if not json_file:
        if file_details:
            raise RuntimeWarning("No metadata JSON file found in folder %s" % files_path)
        return None

    # Fetch metadata and pull information out of it
    globus_save_path = os.path.join('/Users/chris/', GLOBUS_LOCAL_START_PATH, os.path.basename(json_file))
    if not os.path.exists(globus_save_path):
        globus_remote_path = os.path.join(files_path, json_file)
        file_fetch_func = lambda: _globus_file_transfer(client, endpoint_id, globus_remote_path, globus_save_path)
    else:
        file_fetch_func = lambda: globus_save_path

    variable_metadata = {}
    fixed_metadata = {}
    with open(os.path.join(globus_save_path), 'r') as in_file:
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
    more_details = {}
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

    for idx, values in enumerate(file_details):
        file_details[idx] = {**more_details, **values}

    return file_details


def globus_get_files(client: globus_sdk.TransferClient, endpoint_id: str, sensor_path: str, date_experiment_ids: dict) -> dict:
    """Returns a list of files on the endpoint path that match the dates provided
    Arguments:
        client: the Globus transfer client to use
        endpoint_id: the ID of the endpoint to access
        sensor_path: the sensor specific path
        date_experiment_ids: dates with their associated experiment ID
    Return:
        Returns a dictionary with dates as keys, each associated with a list of informational dict's on the files found
    """
    pending_tasks = []
    found_files = {}
    base_path = os.path.join('/-', GLOBUS_START_PATH, sensor_path)
    for one_date in date_experiment_ids.keys():
        cur_path = os.path.join(base_path, one_date)
        logging.debug("Globus path: %s", cur_path)
        path_contents = client.operation_ls(endpoint_id, path=cur_path)
        for one_entry in path_contents:
            if one_entry['type'] == 'dir':
                sub_path = os.path.join(cur_path, one_entry['name'])
                logging.debug("Globus remote file path: %s", sub_path)
                if RUN_SYNCHRONOUSLY:
                    cur_files = globus_get_files_details(client, endpoint_id, sub_path)
                    if cur_files:
                        logging.debug("Found %s files for sub path: %s", str(len(cur_files)), sub_path)
                        if one_date not in found_files:
                            found_files[one_date] = cur_files
                        else:
                            found_files[one_date].extend(cur_files)
                    else:
                        logging.debug("Found 0 files for sub path: %s", sub_path)
                else:
                    raise RuntimeError("Asynchronous fetching from Globus is not supported at this time")
                    # pending_tasks.append(asyncio.create_task(globus_get_files_details_async(client, endpoint_id, sub_path)))

    return found_files


def local_get_files(sensor_path: str, date_experiment_ids: dict) -> dict:
    """Returns a list of files on the endpoint path that match the dates provided
    Arguments:
        sensor_path: the sensor specific path
        date_experiment_ids: dates with their associated experiment ID
    Return:
        Returns a dictionary with dates as keys, each associated with a list of informational dict's on the files found
    """
    found_files = {}
    base_path = sensor_path
    for one_date in date_experiment_ids.keys():
        cur_path = os.path.join(base_path, one_date)
        logging.debug("Local path: %s", cur_path)
        path_contents = os.listdir(cur_path)
        for one_entry in path_contents:
            sub_path = os.path.join(cur_path, one_entry)
            if os.path.isdir(sub_path):
                logging.debug("Local folder path: %s", sub_path)
                cur_files = local_get_files_details(sub_path)
                if cur_files:
                    if one_date not in found_files:
                        found_files[one_date] = cur_files
                    else:
                        found_files[one_date].extend(cur_files)
                else:
                    logging.debug("Found 0 files for sub path: %s", sub_path)

    return found_files


def globus_get_save_files(remote_endpoint: str, sensor_paths: tuple, date_experiment_ids: dict, db_conn: sqlite3.Connection) -> None:
    """Fetches file information associated with the sensors and dates from Globus and updates the database
    Arguments:
        remote_endpoint: the remote endpoint to access
        sensor_paths: a tuple of sensors and their associated paths
        date_experiment_ids: dates with their associated experiment ID
        db_conn: the database to write to
    """
    # Prepare to fetch file information from Globus
    authorizer = globus_get_authorizer()
    trans_client = globus_sdk.TransferClient(authorizer=authorizer)

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
                          (path TEXT, filename TEXT, format TEXT, sensor TEXT, start_time TEXT, finish_time TEXT,
                           gantry_x FLOAT, gantry_y FLOAT, gantry_z FLOAT, experiment_id INTEGER)''')

    # Loop through each sensor and dates and get the associated file information
    num_inserted = 0
    total_records = 0
    for one_sensor_path in sensor_paths:
        sensor = one_sensor_path[0]
        paths = one_sensor_path[1]
        for one_path in paths:
            files = globus_get_files(trans_client, endpoint_id, one_path, date_experiment_ids)
            if not files:
                logging.warning("Unable to find files for dates for sensor %s", sensor)
                continue

            for one_date in files.keys():
                date_files = files[one_date]
                experiment_ids = date_experiment_ids[one_date]
                for one_exp_id in experiment_ids:
                    for one_file in date_files:
                        file_cursor.execute('INSERT INTO files VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                                            [one_file['directory'], one_file['filename'], one_file['format'],
                                             one_file['start_time'], one_file['finish_time'], one_file['gantry_x'],
                                             one_file['gantry_y'], one_file['gantry_z'], one_exp_id])
                        num_inserted += 1
                        total_records += 1
                        if num_inserted >= MAX_INSERT_BEFORE_COMMIT:
                            db_conn.commit()
                            num_inserted = 0
    db_conn.commit()
    file_cursor.close()

    if total_records <= 0:
        logging.warning("No file records were written")
    logging.debug("Wrote %s file records", str(total_records))


def local_get_save_files(sensor_paths: tuple, date_experiment_ids: dict, db_conn: sqlite3.Connection) -> None:
    """Locally fetches file information associated with the sensors and dates and updates the database
    Arguments:
        sensor_paths: a tuple of sensors and their associated paths
        date_experiment_ids: dates with their associated experiment ID
        db_conn: the database to write to
    """
    # Create the table for file information
    file_cursor = db_conn.cursor()
    file_cursor.execute('''CREATE TABLE files
                          (path TEXT, filename TEXT, format TEXT, sensor TEXT, start_time TEXT, finish_time TEXT,
                           gantry_x FLOAT, gantry_y FLOAT, gantry_z FLOAT, season_id INTEGER)''')

    # Loop through each sensor and dates and get the associated file information
    num_inserted = 0
    total_records = 0
    for one_sensor_path in sensor_paths:
        sensor = one_sensor_path[0]
        paths = one_sensor_path[1]
        for one_path in paths:
            files = local_get_files(one_path, date_experiment_ids)
            if not files:
                logging.warning("Unable to find files for dates for sensor %s", sensor)
                continue

            for one_date in files.keys():
                date_files = files[one_date]
                logging.debug("Date files count: %s %s", len(date_files), type(date_files))
                experiment_id = date_experiment_ids[one_date]
                for one_file in date_files:
                    file_cursor.execute('INSERT INTO files VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                                        [one_file['directory'], one_file['filename'], one_file['format'], sensor,
                                         one_file['start_time'], one_file['finish_time'], one_file['gantry_x'],
                                         one_file['gantry_y'], one_file['gantry_z'], experiment_id])
                    num_inserted += 1
                    total_records += 1
                    if num_inserted >= MAX_INSERT_BEFORE_COMMIT:
                        db_conn.commit()
                        num_inserted = 0
    db_conn.commit()
    file_cursor.close()

    if total_records <= 0:
        logging.warning("No file records were written")
    logging.debug("Wrote %s file records", str(total_records))


def create_db_views(db_conn: sqlite3.Connection) -> None:
    """Adds views to the database
    Arguments:
        db_conn: the database to write to
    """
    view_cursor = db_conn.cursor()

    # cultivar, plot, season, sensor, date/daterange
    # CREATE TABLE experimental_info
    #                      (id INTEGER, plot_name TEXT, season_id INTEGER, season TEXT, cultivar_id INTEGER,
    #                      plot_bb_min_lat FLOAT, plot_bb_min_lon FLOAT, plot_bb_max_lat FLOAT, plot_bb_max_lon FLOAT)
    # CREATE TABLE files (path TEXT, filename TEXT, format TEXT, sensor TEXT, start_time TEXT, finish_time TEXT,
    #                            gantry_x FLOAT, gantry_y FLOAT, gantry_z FLOAT, season_id INTEGER)
    # CREATE TABLE cultivars (id INTEGER, name TEXT)
    view_cursor.execute('''CREATE VIEW files_view AS select e.id as plot_id, e.plot_name as plot_name, e.season as season,
                        f.path as folder, f.filename as filename, f.format as format, f.sensor as sensor, f.start_time as start_time,
                        f.finish_time as finish_time, f.gantry_x as gantry_x, f.gantry_y as gantry_y, f.gantry_z as gantry_z,
                        c.name as cultivar_name
                        from experimental_info as e left join files as f on e.season_id = f.season_id 
                            left join cultivars as c on e.cultivar_id = c.id''')

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
    sensor_paths = prepare_sensor_paths(args.sensor_paths)
    dates = prepare_dates(args.dates)

    # Get other values we'll need
    betydb_url = get_betydb_url(args.betydb_url)
    betydb_key = get_betydb_key(args.betydb_key)
    brapi_url = get_brapi_url(args.brapi_url)

    # Get our temporary file name
    _, working_filename = tempfile.mkstemp()
    sql_db = sqlite3.connect(working_filename)

    try:
        # Generate the experiments table
        experiments, cultivars, date_experiment_ids = get_save_experiments(dates, sql_db, betydb_url, betydb_key, brapi_url,
                                                                           args.experiment_json, args.cultivar_json)

        # Generating the cultivars table
        save_cultivars(cultivars, sql_db)

        # Create the files table
        # globus_get_save_files(args.globus_endpoint, sensor_paths, date_experiment_ids, sql_db)
        local_get_save_files(sensor_paths, date_experiment_ids, sql_db)

        # Create the views
        create_db_views(sql_db)

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
