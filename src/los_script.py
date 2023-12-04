# from libraries.sftp_export import *
import logging
import os
import re
import sys
import urllib
import xml.etree.ElementTree as et
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding

import paramiko
import requests
import toml


class Manager:
    """
    A manager class that coordinates the uploading of tagged results to an SFTP server.
    """

    def __init__(self, path_toml: str):
        self.__verify_and_load_toml(path_toml)
        # self.__broker = BrokerRequestResultManager()
        # self.__sftp = SftpFileManager()
        # self.__xml = StatusXmlManager()

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
                         'SFTP.PASSWORD', 'SFTP.TIMEOUT', 'SFTP.FOLDERNAME', 'SECURITY.PATH_ENCRYPTION_KEY',
                         'MISC.WORKING_DIR'}
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
        self.encryptor = OpenSSLFileEncryption
        self.__connection = self.__connect_to_sftp()

    # def __init_encryptor(self, path_file, path_enc_file):
    #     with open(self.__path_key_encryption, 'rb') as key:
    #         command = f"openssl enc -aes-256-cbc -salt -in {path_file} -out {path_enc_file} -k {key}"
    #         subprocess.run(command, shell=True)

    def __connect_to_sftp(self) -> paramiko.sftp_client.SFTPClient:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(self.__sftp_host, username=self.__sftp_username, password=self.__sftp_password,
                    timeout=self.__sftp_timeout)
        return ssh.open_sftp()

    def upload_request_result(self, response: requests.models.Response):
        """
        Upload the content of the response from `BrokerRequestResultManager.get_request_result()` to the SFTP server.
        Extracts the filename from the response headers.
        Prior to uploading, stores the file temporarily in the current local folder and encrypts it using Fernet.
        """
        filename = self.__extract_filename_from_broker_response(response)
        tmp_path_enc_file = self.encryptor.get_enc_file_path(filename)
        try:
            self.upload_file(tmp_path_enc_file)
        finally:
            if os.path.isfile(tmp_path_enc_file):
                os.remove(tmp_path_enc_file)

    @staticmethod
    def __extract_filename_from_broker_response(response: requests.models.Response) -> str:
        return re.search('filename=\"(.*)\"', response.headers['Content-Disposition']).group(1)

    # def __encrypt_file(self, file: bytes) -> bytes:
    #     return self.encryptor.encrypt(file)

    def upload_file(self, path_file: str):
        """
        Upload a file to the SFTP server and overwrite if it already exists on the server.
        """
        logging.info('Sending %s to sftp server', path_file)
        filename = os.path.basename(path_file)
        self.__connection.put(path_file, f"{self.__sftp_foldername}/{filename}")

    def delete_request_result(self, id_request: str):
        name_zip = self.__create_results_file_name(id_request)
        self.__delete_file(name_zip)

    @staticmethod
    def __create_results_file_name(id_request: str) -> str:
        """
        Create the file name for the request result based on the AKTIN Broker naming convention.
        """
        return ''.join(['export_', id_request, '.zip'])

    def __delete_file(self, filename: str):
        logging.info('Deleting %s from sftp server', filename)
        try:
            self.__connection.remove(f"{self.__sftp_foldername}/{filename}")
        except FileNotFoundError:
            logging.info('%s could not be found', filename)


class BrokerRequestResultManager:
    """
    A class for managing request results from the AKTIN Broker.
    """
    __timeout = 10

    def __init__(self):
        self.__broker_url = os.environ['BROKER.URL']
        self.__admin_api_key = os.environ['BROKER.API_KEY']
        self.__tag_requests = os.environ['REQUESTS.TAG']
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

    def get_request_result(self, id_request: str) -> requests.models.Response:
        """
        Retrieve the request results from the AKTIN Broker for a specific request ID.
        To download request results from AKTIN broker, they have to be exported first as a temporarily downloadable file with an uuid.
        """
        logging.info('Downloading results of %s', id_request)
        id_export = self.__export_request_result(id_request)
        response = self.__download_exported_result(id_export)
        return response

    def __export_request_result(self, id_request: str) -> str:
        """
        Export the request results as a temporarily downloadable file with a unique ID.
        """
        url = self.__append_to_broker_url('broker', 'export', 'request-bundle', id_request)
        response = requests.post(url, headers=self.__create_basic_header('text/plain'), timeout=self.__timeout)
        response.raise_for_status()
        return response.text

    def __download_exported_result(self, id_export: str) -> requests.models.Response:
        url = self.__append_to_broker_url('broker', 'download', id_export)
        response = requests.get(url, headers=self.__create_basic_header(), timeout=self.__timeout)
        response.raise_for_status()
        return response

    def get_tagged_requests_completion_as_dict(self) -> dict:
        """
        Get the completion status of requests tagged with a specific tag.
        """
        list_requests = self.__get_request_ids_with_tag(self.__tag_requests)
        dict_broker = {}
        for id_request in list_requests:
            completion = self.__get_request_result_completion(id_request)
            dict_broker[id_request] = str(completion)
        return dict_broker

    def __get_request_ids_with_tag(self, tag: str) -> list:
        logging.info('Checking for requests with tag %s', tag)
        url = self.__append_to_broker_url('broker', 'request', 'filtered')
        url = '?'.join([url, urllib.parse.urlencode(
            {'type': 'application/vnd.aktin.query.request+xml', 'predicate': "//tag='%s'" % tag})])
        response = requests.get(url, headers=self.__create_basic_header(), timeout=self.__timeout)
        response.raise_for_status()
        list_request_id = [element.get('id') for element in et.fromstring(response.content)]
        logging.info('%d requests found', len(list_request_id))
        return list_request_id

    def __get_request_result_completion(self, id_request: str) -> float:
        """
        Get the completion status of a given broker request.
        Computes the result completion by counting connected nodes and the number of nodes that completed the request.
        Returns the completion percentage (rounded to 2 decimal places) or 0.0 if no nodes found.
        """
        url = self.__append_to_broker_url('broker', 'request', id_request, 'status')
        response = requests.get(url, headers=self.__create_basic_header(), timeout=self.__timeout)
        root = et.fromstring(response.content)
        num_nodes = len(root.findall('.//{http://aktin.org/ns/exchange}node'))
        num_completed = len(root.findall('.//{http://aktin.org/ns/exchange}completed'))
        return round(num_completed / num_nodes, 2) if num_nodes else 0.0


class OpenSSLFileEncryption:
    def __init__(self):
        self.__path_key_encryption = b'os.environ["SECURITY.PATH_ENCRYPTION_KEY"]'
        self.__working_dir = os.environ['MISC.WORKING_DIR']

    # def __encrypt_file(self, path_in, path_out):
    #     with open(self.__path_key_encryption, 'rb') as key:
    #         command = f"openssl enc -aes-256-cbc -salt -in {path_in} -out {path_out} -k {key}"
    #         subprocess.run(command, shell=True)

    def __encrypt_file(self, path_in, path_out):
        with open(path_in, 'rb') as input_file:
            plaintext = input_file.read()
            ciphertext = self.__xor_encrypt(plaintext, self.__path_key_encryption)

            with open(path_out, 'wb') as output_file:
                # output_file.write(iv)
                output_file.write(ciphertext)

    @staticmethod
    def __xor_encrypt(plaintext, key):
        iv = os.urandom(16)
        cipher = Cipher(algorithms.AES(key), modes.CFB(iv), backend=default_backend())
        padder = padding.PKCS7(algorithms.AES.block_size).padder()
        padded_plaintext = padder.update(plaintext) + padder.finalize()
        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(padded_plaintext) + encryptor.finalize()

        return iv + ciphertext

    def __get_file_path(self, filename: str) -> (str, str):
        enc_filename = filename + "_enc"
        tmp_path_file = os.path.join(self.__working_dir, filename)
        tmp_path_enc_file = os.path.join(self.__working_dir, enc_filename)
        return tmp_path_file, tmp_path_enc_file

    def get_enc_file_path(self, filename: str) -> str:
        path_in, path_out = self.__get_file_path(filename)
        self.__encrypt_file(path_in, path_out)
        os.remove(path_in)
        return path_out


def main(path_toml: str):
    try:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s',
                            handlers=[logging.StreamHandler()])
        manager = Manager(path_toml)
        manager.upload_tagged_results_to_sftp()
    except Exception as e:
        logging.exception(e)
    finally:
        [logging.root.removeHandler(handler) for handler in logging.root.handlers[:]]
        logging.shutdown()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        raise SystemExit('path to config TOML is missing!')
    main(sys.argv[1])
