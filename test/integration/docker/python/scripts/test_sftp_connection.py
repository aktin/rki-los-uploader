import logging
import os
import tempfile
import unittest

import paramiko


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


class SftpUnittest(unittest.TestCase):
    def setUp(self):
        self.__sftp_manager = SftpFileManager()

    def test_connection(self):
        self.assertTrue(self.__sftp_manager.list_files().__contains__("rki"))

    def test_upload(self):
        filename = 'upload_test_file.txt'
        with open(filename, 'w') as file:
            self.__sftp_manager.upload_file(filename)
            self.assertTrue(self.__sftp_manager.list_files().__contains__("upload_test_file"))
            self.assertFalse(self.__sftp_manager.list_files().__contains__("This should not exist in the sftp server!"))

