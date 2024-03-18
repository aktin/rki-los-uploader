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


def set_path_variable() -> None:
    """
    This Method sets the path variable in an Windows environment. This is necessary for executing the Rscript for
    Length of stay.
    :return:
    """
    # Specify the directory containing Rscript.exe
    r_bin_dir = "C:/Program Files/R/R-4.3.1/bin"  # TODO this path has to be changed for each installation, maybe console input

    # Get the current value of the PATH environment variable
    current_path = os.environ.get('PATH', '')

    # Append the R bin directory to the PATH, separating it with the appropriate separator
    new_path = f"{current_path};{r_bin_dir}" if current_path else r_bin_dir

    # Update the PATH environment variable
    os.environ['PATH'] = new_path


def execute_given_rscript(broker_result_zip_path: str, rscript_path: str) -> str:
    set_path_variable()
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
    r_result_path = execute_rscript(zip_file_path, r_script_path)

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
    main(path_toml, "LOS")



class TestLOSCalculation(unittest.TestCase):
    """
    Tests the R script if it returns the assumed result
    """
    def setUp(self, path_toml: str = "config.toml"):
        config = toml.load(path_toml)
        self.work_directory = config['misc']['temp_dir']
        self.r_script_path = os.path.join(self.work_directory, "LOSCalculator.R")
        self.zip_file_path = os.path.join(self.work_directory, 'resources\\unittest_result.zip')

    def test_single_clinic(self):
        test_data = [("aufnahme_ts	entlassung_ts	triage_ts	a_encounter_num	a_encounter_ide	a_billing_ide\n"
                     "2023-07-28T21:55:36Z	2023-07-28T23:02:49Z	2023-07-28T21:58:08Z	4	4	4\n"
                     "2023-07-28T22:21:09Z	2023-07-28T23:37:27Z	2023-07-28T22:21:49Z	5	5	5\n"
                     "2023-07-28T23:46:09Z	2023-07-29T00:55:15Z	2023-07-28T23:47:20Z	6	6	6")]
        self.__pack_zip__(test_data)

        execute_rscript(self.zip_file_path, self.r_script_path)

        with open("resources\\broker_result\\timeframe.csv", "r") as file:
            result = file.read()
            result_data = result.replace('\"', '').split("\n")
            # for each element in result_data split it by the delimiter ","
            result_data = [element.split(",") for element in result_data]
            # create pandas dataframe with results from R script
            results = pd.DataFrame(result_data[1:-1], columns=result_data[0])
            #check if the dataframe has the expected values
            expected_data = [["date", "ed_count", "visit_mean", "los_mean", "los_reference", "los_difference", "change"],
                    ["2023-W30", "1", "3", "69.41", "193.54", "-124.12", "Abnahme"]]

            expected = pd.DataFrame(expected_data[1:], columns=expected_data[0])
            self.assertEqual(results.to_string(), expected.to_string())

    def test_multiple_clinics(self):
        test_data = [("aufnahme_ts	entlassung_ts	triage_ts	a_encounter_num	a_encounter_ide	a_billing_ide\n"
                      "2023-07-28T21:55:36Z	2023-07-28T23:02:49Z	2023-07-28T21:58:08Z	4	4	4\n"
                      "2023-07-28T22:21:09Z	2023-07-28T23:37:27Z	2023-07-28T22:21:49Z	5	5	5\n"
                      "2023-07-28T23:46:09Z	2023-07-29T00:55:15Z	2023-07-28T23:47:20Z	6	6	6")]
        self.__pack_zip__(test_data)

        execute_rscript(self.zip_file_path, self.r_script_path)

        with open("resources\\broker_result\\timeframe.csv", "r") as file:
            result = file.read()
            result_data = result.replace('\"', '').split("\n")
            # for each element in result_data split it by the delimiter ","
            result_data = [element.split(",") for element in result_data]
            # create pandas dataframe with results from R script
            results = pd.DataFrame(result_data[1:-1], columns=result_data[0])
            # check if the dataframe has the expected values
            expected_data = [
                ["date", "ed_count", "visit_mean", "los_mean", "los_reference", "los_difference", "change"],
                ["2023-W30", "1", "3", "69.41", "193.54", "-124.12", "Abnahme"]]

            expected = pd.DataFrame(expected_data[1:], columns=expected_data[0])
            self.assertEqual(results.to_string(), expected.to_string())



    def __pack_zip__(self, contents: list[str]):
        """
        This method creates a zip file from the given content-array and saves it in the resources folder in the way the broker
        request would structure it.
        :param contents:
        :return:
        """

        for i in range(len(contents)):
            #create a directory "unittest_result" with a subdirectory "i_result"
            i_result_path = "resources\\unittest_result\\"+str(i)+"_result"
            os.makedirs(i_result_path, exist_ok=True)

            #create a file "case_data.txt" with the content
            with open(i_result_path+"\\case_data.txt", "w") as file:
                file.write(contents[i])

            # convert 8_result to a zip file
            with zipfile.ZipFile(i_result_path+".zip", "w") as zip_file:
                for root, dirs, files in os.walk(i_result_path):
                    for file in files:
                        zip_file.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), i_result_path))

            # delete the directory "8_result"
            shutil.rmtree(i_result_path)

        # convert unittest_result to a zip file
        with zipfile.ZipFile("resources\\unittest_result.zip", "w") as zip_file:
            for root, dirs, files in os.walk("resources\\unittest_result"):
                for file in files:
                    zip_file.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), "resources\\unittest_result"))

        # delete the directory "unittest_result"
        shutil.rmtree("resources\\unittest_result")

