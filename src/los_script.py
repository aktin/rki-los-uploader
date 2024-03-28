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
from re import Match
import paramiko
import requests
import toml


class StatusXmlManager:
    """
    A class for managing operations on an XML status file.
    """

    def __init__(self):
        self.path_status_xml = os.path.join(os.environ['MISC.WORKING_DIR'], 'status.xml')
        if not os.path.isfile(self.path_status_xml):
            self.__init_status_xml()
        self.__format_date = '%Y-%m-%d %H:%M:%S'
        self.__element_tree = et.parse(self.path_status_xml)

    def __init_status_xml(self):
        """
        Creates a new 'status.xml' file in the working directory with an empty <status> tag.
        """
        root = et.Element('status')
        self.__element_tree = et.ElementTree(root)
        self.__save_current_status_xml_as_file()

    def __save_current_status_xml_as_file(self):
        self.__element_tree.write(self.path_status_xml, encoding='utf-8')

    def get_element_by_id(self, id_request: str) -> et.Element:
        root = self.__element_tree.getroot()
        for request_status in root.iter('request-status'):
            id_element = request_status.find('id')
            if id_element is not None and id_element.text == id_request:
                return request_status
        return None

    def update_or_add_element(self, id_request: str, completion: str):
        root = self.__element_tree.getroot()
        for request_status in root.findall('request-status'):
            id_element = request_status.find('id')
            if id_element is not None and id_element.text == id_request:
                request_status.find('completion').text = completion
                self.__add_or_update_date_tag_in_element(request_status, 'last-update')
                break
        else:
            new_request_status = et.SubElement(root, 'request-status')
            et.SubElement(new_request_status, 'id').text = id_request
            et.SubElement(new_request_status, 'completion').text = completion
            et.SubElement(new_request_status, 'uploaded').text = datetime.utcnow().strftime(self.__format_date)
        self.__save_current_status_xml_as_file()

    def __add_or_update_date_tag_in_element(self, parent: et.Element, name_tag: str) -> None:
        child = parent.find(name_tag)
        if child is None:
            et.SubElement(parent, name_tag).text = datetime.utcnow().strftime(self.__format_date)
        else:
            child.text = datetime.utcnow().strftime(self.__format_date)

    def add_delete_tag_to_element(self, id_request: str):
        parent = self.get_element_by_id(id_request)
        self.__add_or_update_date_tag_in_element(parent, 'deleted')
        self.__save_current_status_xml_as_file()

    def get_request_completion_as_dict(self) -> dict:
        """
        Extract the request ID and completion from each element in the status XML.
        Returns them as a dictionary.
        """
        root = self.__element_tree.getroot()
        list_ids = [element.text for element in root.findall('.//id')]
        list_completion = [element.text for element in root.findall('.//completion')]
        return dict(zip(list_ids, list_completion))

    def compare_request_completion_between_broker_and_sftp(self, dict_broker: dict, dict_xml: dict) -> (set, set, set):
        set_new = set(dict_broker.keys()).difference(set(dict_xml.keys()))
        set_update = self.__get_requests_to_update(dict_broker, dict_xml)
        set_delete = self.__get_requests_to_delete(dict_broker, dict_xml)
        logging.info(
            f"{len(set_new)} new requests, {len(set_update)} requests to update, {len(set_delete)} requests to delete")
        return set_new, set_update, set_delete

    def __get_requests_to_update(self, dict_broker: dict, dict_xml: dict) -> set:
        """
        A request has to be updated on sftp server if its completion rate changed
        """
        set_update = set(dict_broker.keys()).intersection(set(dict_xml.keys()))
        for key in set_update.copy():
            if dict_broker.get(key) == dict_xml.get(key):
                set_update.remove(key)
            if self.__is_request_tagged_as_deleted(key):
                set_update.remove(key)
        return set_update

    def __get_requests_to_delete(self, dict_broker: dict, dict_xml: dict) -> set:
        """
        A request with the tag "deleted" is already deleted on sftp server
        """
        set_delete = set(dict_xml.keys()).difference(set(dict_broker.keys()))
        for key in set_delete.copy():
            if self.__is_request_tagged_as_deleted(key):
                set_delete.remove(key)
        return set_delete

    def __is_request_tagged_as_deleted(self, id_request: str) -> bool:
        parent = self.get_element_by_id(id_request)
        child = parent.find('deleted')
        return child is not None


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
                         'SFTP.PASSWORD', 'SFTP.TIMEOUT', 'SFTP.FOLDERNAME', 'MISC.WORKING_DIR', 'MISC.TEMP_DIR',
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

    def get_broker(self):
        return self.__broker


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
        zip_file_path = os.path.join(os.environ['MISC.TEMP_ZIP_DIR'], f'{id_request}_result.zip')
        with open(zip_file_path, 'wb') as zip_file:
            zip_file.write(response.content)
        return zip_file_path

    def request_highest_id_by_tag_from_broker(self, tag):
        """
        Requests the highest ID for a given tag from AKTIN Broker. Highest ID = latest entry
        :return: id of the last result
        """
        url = self.__append_to_broker_url('broker', 'request', 'filtered')
        url = '?'.join([url, urllib.parse.urlencode(
            {'type': 'application/vnd.aktin.query.request+xml', 'predicate': "//tag='%s'" % tag})])
        response = requests.get(url, headers=self.__create_basic_header(), timeout=self.__timeout)
        response.raise_for_status()

        list_request_id = [element.get('id') for element in et.fromstring(response.content)]
        if len(list_request_id) < 1:
            raise Exception(f"no element with tag:\"{tag}\" was found!")
        return max(list_request_id)

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


class LosScriptManager:
    """
    This class manages helper methods for managing the execution and flow of the Rscript. It starts the main method
    flow, executes the rscript and clears a directory in wich temporary files are stored.
    """

    def main(self, path_toml: str) -> None:
        manager = Manager(path_toml)
        r_script_path = os.environ['RSCRIPT.SCRIPT_PATH']
        request_tag = os.environ['REQUESTS.TAG']

        # construct path to resource folder where the broker results are stored
        broker_result_path = os.environ['MISC.TEMP_ZIP_DIR']
        # create instances of the broker request id manager and the broker request result manager

        zip_file_path = self.download_latest_data_export_from_broker_by_tag(request_tag, manager)
        r_result_path = self.execute_given_rscript(zip_file_path, r_script_path)

        manager.__sftp.clean_and_upload_to_sftp_server(r_result_path, manager)
        # delete resources folder as preparation for new results
        self.delete_contents_of_dir(broker_result_path)

    def execute_given_rscript(self, broker_result_zip_path: str, rscript_path: str) -> Match[str] | None:
        os.chmod(rscript_path, 0o755)

        output = subprocess.check_output(['Rscript', rscript_path, broker_result_zip_path])
        logging.info("R Script finished successfully")
        # Decode the output to string if necessary
        output_string = output.decode("utf-8")
        # search output for regex "timeframe_path:"
        result_path = re.search('timeframe_path:.*\"', output_string)[0].split(':')[1]
        return result_path

    def delete_contents_of_dir(self, path) -> None:
        try:
            # Use shutil.rmtree to remove all files and subdirectories within the directory
            shutil.rmtree(path)
            # Recreate the directory if needed
            os.mkdir(path)
            logging.info(f"Contents of '{path}' have been deleted.")
        except Exception as e:
            logging.error(e)

    def download_latest_data_export_from_broker_by_tag(self, request_tag, manager: Manager):
        """This method manages the download of the latest broker data export, by using the existing methods from
        BrokerRequestResultManager and managing them"""
        broker_manager = manager.get_broker()
        request_id = broker_manager.request_highest_id_by_tag_from_broker(request_tag)
        zip_file_path = broker_manager.download_request_result_to_working_dir(request_id)
        return zip_file_path

    def clean_and_upload_to_sftp_server(self, r_result_path, manager: Manager) -> None:
        sftp_manager = manager.__sftp
        sftp_files = sftp_manager.list_files()
        for sftp_file in sftp_files:
            sftp_manager.delete_file(sftp_file)
        sftp_manager.upload_file(r_result_path)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        raise SystemExit('path to config TOML is missing!')

    toml_path = sys.argv[1]
    losmanager = LosScriptManager()
    losmanager.main(toml_path)
