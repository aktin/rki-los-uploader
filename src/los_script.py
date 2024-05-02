# -*- coding: utf-8 -*-
"""
Created on 22.03.2024
@AUTHOR: Wiliam Hoy (whoy@ukaachen.de)
@VERSION=1.0
"""

#
#      Copyright (c) 2024 Wiliam Hoy
#
#      This program is free software: you can redistribute it and/or modify
#      it under the terms of the GNU Affero General Public License as
#      published by the Free Software Foundation, either version 3 of the
#      License, or (at your option) any later version.
#
#      This program is distributed in the hope that it will be useful,
#      but WITHOUT ANY WARRANTY; without even the implied warranty of
#      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#      GNU Affero General Public License for more details.
#
#      You should have received a copy of the GNU Affero General Public License
#      along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
#

import logging
import os
import re
import subprocess
import sys
import urllib
import xml.etree.ElementTree as et
import shutil

import paramiko
import requests
import toml
from requests import Response


class Manager:
    """
    A manager class that coordinates the uploading of tagged results to an SFTP server.
    """

    def __init__(self, path_toml: str):
        self.__verify_and_load_toml(path_toml)

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
                         'SFTP.PASSWORD', 'SFTP.TIMEOUT', 'SFTP.FOLDERNAME', 'MISC.WORKING_DIR',
                         'MISC.TEMP_ZIP_DIR', 'RSCRIPT.SCRIPT_PATH'}
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
    def __init__(self):
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

    def __init__(self):
        self.__broker_url = os.environ['BROKER.URL']
        self.__admin_api_key = os.environ['BROKER.API_KEY']
        self.__tag_requests = os.environ['REQUESTS.TAG']
        self.__working_dir = os.environ['MISC.WORKING_DIR']
        self.__temp_dir = os.environ['MISC.TEMP_ZIP_DIR']
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

    def download_latest_broker_result_by_set_tag(self) -> str:
        """
        Creates a zip archive from a broker result by using the id of the last tagged result and requesting it.
        :return: path to the resulting zip archive
        """
        id_request = self.__get_id_of_latest_request_by_set_tag()
        uuid = self.__export_request_result(id_request)
        logging.info('Downloading results of %s', id_request)
        result_stream = self.__download_exported_result(uuid)
        logging.info('Download finished!')
        zip_file_path = self.__store_broker_response_as_zip(result_stream, id_request)
        return zip_file_path

    def __export_request_result(self, id_request: str) -> str:
        """
        Returns a UUID from response from AKTIN
        """
        url = self.__append_to_broker_url('broker', 'export', 'request-bundle', id_request)
        response = requests.post(url, headers=self.__create_basic_header('text/plain'), timeout=self.__timeout)
        response.raise_for_status()
        uuid = response.text
        return uuid

    def __download_exported_result(self, uuid: str) -> Response:
        """
        Download the exported request results as a ZIP file inside the folder WORKING_DIR.
        Returns the path to the downloaded ZIP file.
        """
        url = self.__append_to_broker_url('broker', 'download', uuid)
        broker_result_stream = requests.get(url, headers=self.__create_basic_header(), timeout=self.__timeout)
        broker_result_stream.raise_for_status()
        return broker_result_stream

    def __store_broker_response_as_zip(self, broker_result_stream, id_request: str):
        """
        Takes a stream containing a broker result and extracts it to a zip archive in the specified temporary
        directory self.__temp_dir
        """
        # id request als input raus und name aus dem header nehmen
        zip_file_path = os.path.join(self.__temp_dir, f'{id_request}_result.zip')  # suche id in stream
        with open(zip_file_path, 'wb') as zip_file:
            zip_file.write(broker_result_stream.content)
        return zip_file_path

    def __get_id_of_latest_request_by_set_tag(self):
        """
        Requests the highest ID for a set tag from AKTIN Broker. Highest ID = latest entry
        :return: id of the last result
        """
        url = self.__append_to_broker_url('broker', 'request', 'filtered')
        url = '?'.join([url, urllib.parse.urlencode(
            {'type': 'application/vnd.aktin.query.request+xml', 'predicate': "//tag='%s'" % self.__tag_requests})])
        response = requests.get(url, headers=self.__create_basic_header(), timeout=self.__timeout)
        response.raise_for_status()

        list_request_id = [element.get('id') for element in et.fromstring(response.content)]
        if len(list_request_id) < 1:
            raise Exception(f"no element with tag:\"{self.__tag_requests}\" was found!")
        return max(list_request_id)


class DirectoryManager:

    def __init__(self):
        """
        :param test_directory: True if this class is used in a testing environment and needs to store the files in this environment
        """
        self._temp_work_path = os.environ['MISC.TEMP_ZIP_DIR']
        self._broker_result_directory = self._temp_work_path+"/broker_result"

    def create_temp_directory(self):
        if not os.path.exists(self._temp_work_path):
            os.makedirs(self._temp_work_path)
        if not os.path.exists(self._broker_result_directory):
            os.makedirs(self._broker_result_directory)

    def get_temp_directory(self):
        return self._temp_work_path

    def get_broker_result_directory(self):
        return self._broker_result_directory

    def cleanup(self):
        try:
            # Use shutil.rmtree to remove all files and subdirectories within the directory
            shutil.rmtree(self._temp_work_path)
            logging.info(f"Contents of '{self._temp_work_path}' have been deleted.")
        except Exception as e:
            logging.error(e)


class LosScriptManager:
    """
    This class manages helper methods for managing the execution and flow of the Rscript. It starts the main method
    flow, executes the rscript and clears a directory in wich temporary files are stored.
    """

    def __init__(self, path_toml: str):
        self.__manager = Manager(path_toml)
        self.__broker_manager = BrokerRequestResultManager()
        # self.__sftp_manager = SftpFileManager()
        self._r_script_path = os.environ['RSCRIPT.SCRIPT_PATH']
        self.__temp_result_path = os.environ['MISC.TEMP_ZIP_DIR']

    def main(self) -> None:
        dir_manager = DirectoryManager()
        dir_manager.create_temp_directory()
        zip_file_path = self.__broker_manager.download_latest_broker_result_by_set_tag()
        r_result_path = self.execute_given_rscript(zip_file_path)
        self.clean_and_upload_to_sftp_server(r_result_path)
        dir_manager.cleanup()

    def execute_given_rscript(self, zip_file_path: str):
        output = subprocess.check_output(['Rscript', self._r_script_path, zip_file_path])
        logging.info("R Script finished successfully")
        # Decode the output to string if necessary
        output_string = output.decode("utf-8")
        # search output for regex "timeframe_path:"
        result_path = re.search('timeframe_path:.*\"', output_string)[0].split(':')[1]
        return result_path

    def clean_and_upload_to_sftp_server(self, r_result_path) -> None:
        sftp_manager = self.__sftp_manager
        sftp_files = sftp_manager.list_files()
        for sftp_file in sftp_files:
            sftp_manager.delete_file(sftp_file)
        sftp_manager.upload_file(r_result_path)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        raise SystemExit('path to config TOML is missing!')

    toml_path = sys.argv[1]
    losmanager = LosScriptManager(toml_path)
    losmanager.main()
