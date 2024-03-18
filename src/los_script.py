# from libraries.sftp_export import *
import logging
import os
import re
import subprocess
import sys
import unittest
import urllib
import xml.etree.ElementTree as et
import shutil
import zipfile
# from _typeshed import SupportsDunderLT, SupportsDunderGT
from typing import Any

import pandas
import pandas as pd

import paramiko
import requests
import toml


class Manager:
    """
    A manager class that coordinates the uploading of tagged results to an SFTP server.
    """

    def __init__(self, path_toml: str):
        self.__verify_and_load_toml(path_toml)
        self.__broker = BrokerRequestResultManager()
        self.__sftp = SftpFileManager()
        self.__xml = StatusXmlManager()

    def __flatten_dict(self, d, parent_key='', sep='.'):
        items = []
        for k, v in d.items():
            new_key = f'{parent_key}{sep}{k}' if parent_key else k
            if isinstance(v, dict):
                items.extend(self.__flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def __verify_and_load_toml(self, path_toml: str):
        """
        This method verifies the TOML file path, loads the configuration, flattens it into a dictionary,
        and sets the environment variables based on the loaded configuration.
        """
        required_keys = {'BROKER.URL', 'BROKER.API_KEY', 'REQUESTS.TAG', 'SFTP.HOST', 'SFTP.USERNAME',
                         'SFTP.PASSWORD', 'SFTP.TIMEOUT', 'SFTP.FOLDERNAME', 'MISC.WORKING_DIR'}
        if not os.path.isfile(path_toml):
            raise SystemExit('invalid TOML file path')
        with open(path_toml, encoding='utf-8') as file:
            dict_config = toml.load(file)
        flattened_config = self.__flatten_dict(dict_config)
        loaded_keys = set(flattened_config.keys())
        if required_keys.issubset(loaded_keys):
            for key in loaded_keys:
                os.environ[key] = flattened_config.get(key)
        else:
            missing_keys = required_keys - loaded_keys
            raise SystemExit(f'following keys are missing in config file: {missing_keys}')


class SftpFileManager:
    """
    A class for managing file operations with an SFTP server.
    """

    def __init__(self, config_file="config.toml"):
        config = toml.load(config_file)

        self.__sftp_host = os.environ['SFTP.HOST']
        self.__sftp_username = os.environ['SFTP.USERNAME']
        self.__sftp_password = os.environ['SFTP.PASSWORD']
        self.__sftp_timeout = int(os.environ['SFTP.TIMEOUT'])
        self.__sftp_foldername = os.environ['SFTP.FOLDERNAME']
        self.__connection = self.__connect_to_sftp()

    def __connect_to_sftp(self) -> paramiko.sftp_client.SFTPClient:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.__sftp_host, username=self.__sftp_username, password=self.__sftp_password,
                    timeout=self.__sftp_timeout)
        return ssh.open_sftp()

    def upload_file(self, path_file: str):
        """
        Upload a file to the SFTP server and overwrite if it already exists on the server.
        """
        logging.info('Sending %s to sftp server', path_file)
        filename = os.path.basename(path_file)
        self.__connection.put(path_file, f"{self.__sftp_foldername}/{filename}")

    def list_files(self):
        """
        List all files in the SFTP server's specified folder.
        """
        logging.info('Listing files on sftp server')
        files = self.__connection.listdir(f"{self.__sftp_foldername}")
        return files

    def delete_file(self, filename: str):
        logging.info('Deleting %s from sftp server', filename)
        try:
            self.__connection.remove(f"{self.__sftp_foldername}/{filename}")
        except FileNotFoundError:
            logging.info('%s could not be found', filename)


class BrokerRequestResultManager:
    """
    A class for managing request results from the AKTIN Broker. The AKTIN Broker is the data source from where our data is beeing imported.
    """
    __timeout = 10

    def __init__(self, config_file="config.toml"):
        config = toml.load(config_file)

        self.__broker_url = config['broker']['url']
        self.__admin_api_key = config['broker']['api_key']
        self.__tag_requests = config['requests']['tag']
        self.__working_dir = os.getcwd()  #TODO check if working dir should be stated in toml or read automatically like here
        self.__check_broker_server_availability()

    def __check_broker_server_availability(self):
        url = self.__append_to_broker_url('broker', 'status')
        try:
            response = requests.head(url, timeout=self.__timeout)
            response.raise_for_status()
        except requests.exceptions.Timeout:
            raise SystemExit('Connection to AKTIN Broker timed out')
        except requests.exceptions.HTTPError as err:
            raise SystemExit(f'HTTP error occurred: {err}')
        except requests.exceptions.RequestException as err:
            raise SystemExit(f'An ambiguous error occurred: {err}')

    def __append_to_broker_url(self, *items: str) -> str:
        url = self.__broker_url
        for item in items:
            url = f'{url}/{item}'
        return url

    def __create_basic_header(self, mediatype: str = 'application/xml') -> dict:
        """
        HTTP header for requests to AKTIN Broker. Includes the authorization, connection, and accepted media type.
        """
        return {'Authorization': ' '.join(['Bearer', self.__admin_api_key]), 'Connection': 'keep-alive',
                'Accept': mediatype}

    def download_request_result_to_working_dir(self, id_request: str) -> str:
        """
        Retrieve the request results from the AKTIN Broker for a specific request ID.
        To download request results from AKTIN broker, they have to be exported first as a temporarily
        downloadable file with an uuid.
        """
        logging.info('Downloading results of %s', id_request)
        id_export = self.__export_request_result(id_request)
        zip_file_path = self.__download_exported_result_to_working_dir(id_export, id_request)
        return zip_file_path

    def __export_request_result(self, id_request: str) -> str:
        """
        Export the request results as a temporarily downloadable file with a unique ID.
        """
        url = self.__append_to_broker_url('broker', 'export', 'request-bundle', id_request)
        response = requests.post(url, headers=self.__create_basic_header('text/plain'), timeout=self.__timeout)
        response.raise_for_status()
        return response.text

    def __download_exported_result_to_working_dir(self, id_export: str, id_request: str) -> str:
        """
        Download the exported request results as a ZIP file inside the folder WORKING_DIR.
        Returns the path to the downloaded ZIP file.
        """
        url = self.__append_to_broker_url('broker', 'download', id_export)
        response = requests.get(url, headers=self.__create_basic_header(), timeout=self.__timeout)
        response.raise_for_status()
        zip_file_path = os.path.join(self.__working_dir, "resources", f'{id_request}_result.zip')
        with open(zip_file_path, 'wb') as zip_file:
            zip_file.write(response.content)
        return zip_file_path

    def get_request_ids_with_tag(self, tag: str) -> list:
        logging.info('Checking for requests with tag %s', tag)
        url = self.__append_to_broker_url('broker', 'request', 'filtered')
        url = '?'.join([url, urllib.parse.urlencode(
            {'type': 'application/vnd.aktin.query.request+xml', 'predicate': "//tag='%s'" % tag})])
        response = requests.get(url, headers=self.__create_basic_header(), timeout=self.__timeout)
        response.raise_for_status()
        list_request_id = [element.get('id') for element in et.fromstring(response.content)]
        logging.info('%d requests found', len(list_request_id))
        return list_request_id


class BrokerRequestIDManager:
    """
    A class to manage requests IDs.
    """
    __timeout = 10

    def __init__(self, config_file="config.toml"):
        config = toml.load(config_file)

        self.__broker_url = config['broker']['url']
        self.__admin_api_key = config['broker']['api_key']
        self.__tag_requests = config['requests']['tag']
        self.__working_dir = config['misc']['working_dir']
        self.__check_broker_server_availability()

    def __check_broker_server_availability(self):
        url = self.__append_to_broker_url('broker', 'status')
        try:
            response = requests.head(url, timeout=self.__timeout)
            response.raise_for_status()
        except requests.exceptions.Timeout:
            raise SystemExit('Connection to AKTIN Broker timed out')
        except requests.exceptions.HTTPError as err:
            raise SystemExit(f'HTTP error occurred: {err}')
        except requests.exceptions.RequestException as err:
            raise SystemExit(f'An ambiguous error occurred: {err}')

    def __append_to_broker_url(self, *items: str) -> str:
        url = self.__broker_url
        for item in items:
            url = f'{url}/{item}'
        return url

    def __create_basic_header(self, mediatype: str = 'application/xml') -> dict:
        """
        HTTP header for requests to AKTIN Broker. Includes the authorization, connection, and accepted media type.
        """
        return {'Authorization': ' '.join(['Bearer', self.__admin_api_key]), 'Connection': 'keep-alive',
                'Accept': mediatype}

    def request_highest_id_by_tag_from_broker(self, tag='pandemieradar'):
        """
        Requests the highest ID for a given tag from AKTIN Broker. Highest ID = latest entry
        :return: id of the last result
        """
        url = self.__append_to_broker_url('broker', 'request', 'filtered')
        url = '?'.join([url, urllib.parse.urlencode(
            {'type': 'application/vnd.aktin.query.request+xml', 'predicate': "//tag='%s'" % tag})])
        response = requests.get(url, headers=self.__create_basic_header(), timeout=self.__timeout)
        response.raise_for_status()

        # TODO merge the two iterations to one
        # TODO return type should be int
        list_request_id = [element.get('id') for element in et.fromstring(response.content)]
        if len(list_request_id) < 1:
            raise Exception(f"no element with tag:\"{tag}\" was found!")
        return max(list_request_id)


def set_path_variable(path_toml: str) -> None:
    """
    This Method sets the path variable in an Windows environment. This is necessary for executing the Rscript for
    Length of stay.
    :return:
    """
    config = toml.load(path_toml)
    # Specify the directory containing Rscript.exe
    r_bin_dir = config['rscript']['r_directory']

    # Get the current value of the PATH environment variable
    current_path = os.environ.get('PATH', '')

    # Append the R bin directory to the PATH, separating it with the appropriate separator
    new_path = f"{current_path};{r_bin_dir}" if current_path else r_bin_dir

    # Update the PATH environment variable
    os.environ['PATH'] = new_path


def execute_given_rscript(broker_result_zip_path: str, rscript_path: str, path_toml: str) -> str:
    set_path_variable(path_toml)
    # result = subprocess.call(['Rscript', rscript_path, broker_result_zip_path])
    output = subprocess.check_output(['Rscript', rscript_path, broker_result_zip_path])

    # Decode the output to string if necessary
    output_string = output.decode("utf-8")
    result_path = output_string.split('\"')[-2]

    return result_path


def delete_contents_of_dir(_dir) -> None:
    try:
        # Use shutil.rmtree to remove all files and subdirectories within the directory
        shutil.rmtree(_dir)
        # Recreate the directory if needed
        os.mkdir(_dir)
        print(f"Contents of '{_dir}' have been deleted.")
    except Exception as e:
        print(f"An error occurred: {e}")


def main(path_toml: str, request_tag: str = "LOS") -> None:
    config = toml.load(path_toml)
    work_directory = config['misc']['temp_dir']
    # construct path to length of stay calculating r script
    r_script_path = config['rscript']['script_path']
    # construct path to resource folder where the broker results are stored
    broker_result_path = os.path.join(work_directory, 'resources')
    # delete resources folder as preparation for new results
    delete_contents_of_dir(broker_result_path)

    # create instances of the broker request id manager and the broker request result manager
    zip_file_path = download_latest_data_export_from_broker_by_tag(path_toml, request_tag)
    r_result_path = execute_given_rscript(zip_file_path, r_script_path, path_toml)

    clean_and_upload_to_sftp_server(r_result_path)


def download_latest_data_export_from_broker_by_tag(path_toml, request_tag):
    broker_id_manager = BrokerRequestIDManager(path_toml)
    broker_manager = BrokerRequestResultManager(path_toml)
    request_id = broker_id_manager.request_highest_id_by_tag_from_broker(request_tag)
    zip_file_path = broker_manager.download_request_result_to_working_dir(request_id)
    return zip_file_path


def clean_and_upload_to_sftp_server(r_result_path) -> None:
    sftp_manager = SftpFileManager()
    sftp_files = sftp_manager.list_files()
    for sftp_file in sftp_files:
        sftp_manager.delete_file(sftp_file)
    sftp_manager.upload_file(r_result_path)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        raise SystemExit('path to config TOML is missing!')
    path_toml = sys.argv[1]
    conf = toml.load(path_toml)
    request_tag = conf['requests']['tag']
    main(path_toml, request_tag)




