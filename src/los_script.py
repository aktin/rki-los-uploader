# -*- coding: utf-8 -*-
"""
Created on 22.03.2024
@AUTHOR: Wiliam Hoy (whoy@ukaachen.de), Alexander Kombeiz (akombeiz@ukaachen.de)
@VERSION=1.1
"""

#
#      Copyright (c) 2025 AKTIN
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

import datetime
import logging
import os
import re
import shutil
import subprocess
import sys
import urllib
import xml.etree.ElementTree as et
from pathlib import Path

import paramiko
import requests
import toml
from requests import Response


class ConfigurationManager:
  """
  Handles loading and validating TOML configuration files into environment variables.
  """
  __required_keys = {
    'BROKER.URL', 'BROKER.API_KEY',
    'REQUESTS.TAG',
    'SFTP.HOST', 'SFTP.PORT', 'SFTP.USERNAME', 'SFTP.PASSWORD', 'SFTP.TIMEOUT', 'SFTP.FOLDERNAME',
    'RSCRIPT.SCRIPT_PATH', 'RSCRIPT.LOS_MAX', 'RSCRIPT.ERROR_MAX'
  }

  def __init__(self, path_toml: Path):
    self.__verify_and_load_toml(path_toml)

  def __verify_and_load_toml(self, path_toml: Path):
    """
    This method verifies the TOML file path, loads the configuration, flattens it into a dictionary,
    and sets the environment variables based on the loaded configuration.
    """
    path_toml = Path(path_toml).resolve()
    logging.info('Loading %s as environment vars', path_toml)
    self.__verify_file_exists(path_toml)
    config = self.__load_toml_file(path_toml)
    flattened_config = self.__flatten_dict(config)
    self.__validate_and_set_env_vars(flattened_config)

  def __verify_file_exists(self, path: Path):
    if not path.exists() or not path.is_file():
      raise SystemExit('Invalid TOML file path')

  def __load_toml_file(self, path: Path) -> dict:
    with path.open(encoding='utf-8') as file:
      return toml.load(file)

  def __flatten_dict(self, d, parent_key='', sep='.') -> dict:
    items = []
    for k, v in d.items():
      new_key = f'{parent_key}{sep}{k}' if parent_key else k
      if isinstance(v, dict):
        items.extend(self.__flatten_dict(v, new_key, sep=sep).items())
      else:
        items.append((new_key, v))
    return dict(items)

  def __validate_and_set_env_vars(self, config: dict):
    loaded_keys = set(config.keys())
    missing_keys = self.__required_keys - loaded_keys
    if missing_keys:
      raise SystemExit(f'Missing keys in config file: {missing_keys}')
    for key, value in config.items():
      os.environ[key] = str(value)


class SftpFileManager:
  """
  A class for managing file operations with an SFTP server.
  """

  def __init__(self):
    self.__sftp_host = os.environ['SFTP.HOST']
    self.__sftp_port = int(os.environ['SFTP.PORT'])
    self.__sftp_username = os.environ['SFTP.USERNAME']
    self.__sftp_password = os.environ['SFTP.PASSWORD']
    self.__sftp_timeout = int(os.environ['SFTP.TIMEOUT'])
    self.__sftp_foldername = Path(os.environ['SFTP.FOLDERNAME'])
    self.__connection = self.__connect_to_sftp()

  def __connect_to_sftp(self) -> paramiko.sftp_client.SFTPClient:
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(self.__sftp_host,
                port=self.__sftp_port,
                username=self.__sftp_username,
                password=self.__sftp_password,
                timeout=self.__sftp_timeout,
                allow_agent=False,
                look_for_keys=False)
    return ssh.open_sftp()

  def upload_file(self, path_file: Path):
    """
    Upload a file to the SFTP server and overwrite if it already exists on the server.
    """
    path_file = Path(path_file).resolve()
    if not path_file.exists():
      raise FileNotFoundError(f"File {path_file} does not exist.")
    logging.info('Sending %s to SFTP server', path_file)
    self.__connection.put(str(path_file), str(self.__sftp_foldername / path_file.name))

  def list_files(self) -> list:
    logging.info('Listing files from SFTP server')
    return self.__connection.listdir(str(self.__sftp_foldername))

  def delete_file(self, filename: str):
    logging.info('Deleting %s from SFTP server', filename)
    try:
      self.__connection.remove(str(self.__sftp_foldername / filename))
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
    self.__requests_tag = os.environ['REQUESTS.TAG']
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
    return "/".join([self.__broker_url] + list(items))

  def __create_basic_header(self, mediatype: str = 'application/xml') -> dict:
    return {'Authorization': f'Bearer {self.__admin_api_key}', 'Connection': 'keep-alive', 'Accept': mediatype}

  def download_latest_broker_result_by_set_tag(self, zip_target_path: Path = None, requests_tag: str = None) -> Path:
    """
    Creates a zip archive from a broker result by using the id of the last tagged result and requesting it.
    :return: Path to the resulting zip archive.
    """
    id_request = str(self.__get_id_of_latest_request_by_set_tag(requests_tag))
    uuid = self.__export_request_result(id_request)
    result_stream = self.__download_exported_result(uuid)
    zip_file_path = self.__store_broker_response_as_zip(result_stream, id_request, zip_target_path)
    return zip_file_path

  def __get_id_of_latest_request_by_set_tag(self, requests_tag: str = None) -> int:
    """
    Asks the broker for the highest request ID of a set tag.
    """
    requests_tag = requests_tag or self.__requests_tag
    url = self.__append_to_broker_url('broker', 'request', 'filtered')
    url = '?'.join(
        [url, urllib.parse.urlencode({'type': 'application/vnd.aktin.query.request+xml', 'predicate': "//tag='%s'" % requests_tag})])
    response = requests.get(url, headers=self.__create_basic_header(), timeout=self.__timeout)
    response.raise_for_status()
    list_request_id = [int(element.get('id')) for element in et.fromstring(response.content)]
    if not list_request_id:
      logging.warning("No requests with tag: %s were found!" % self.__requests_tag)
      sys.exit(0)
    logging.info('%d requests found (Highest Id: %d)', len(list_request_id), max(list_request_id))
    return max(list_request_id)

  def __export_request_result(self, id_request: str) -> str:
    """
    Tells the broker to aggregate request results into a temporarily downloadable file.
    """
    logging.info('Exporting results of %s', id_request)
    url = self.__append_to_broker_url('broker', 'export', 'request-bundle', id_request)
    response = requests.post(url, headers=self.__create_basic_header('text/plain'), timeout=self.__timeout)
    response.raise_for_status()
    return response.text

  def __download_exported_result(self, uuid: str) -> Response:
    logging.info('Downloading results of %s', uuid)
    url = self.__append_to_broker_url('broker', 'download', uuid)
    response = requests.get(url, headers=self.__create_basic_header(), timeout=self.__timeout)
    response.raise_for_status()
    return response

  def __store_broker_response_as_zip(self, response: Response, id_request: str, target_path: Path = None) -> Path:
    """
    Saves broker response content as a zip archive in the specified or script directory.
    """
    target_path = target_path or Path(__file__).resolve().parent
    zip_file_path = target_path / f'result{id_request}.zip'
    logging.info("Writing results to %s", zip_file_path)
    with zip_file_path.open('wb') as zip_file:
      zip_file.write(response.content)
    return zip_file_path


class LosScriptManager:
  """
  Manages execution of R script for length of stay calculations.
  """

  def __init__(self):
    self.__rscript_path = Path(os.environ['RSCRIPT.SCRIPT_PATH']).resolve()
    self.__los_max = os.environ['RSCRIPT.LOS_MAX']
    self.__error_max = os.environ['RSCRIPT.ERROR_MAX']

  def execute_rscript(self, zip_file_path: Path, start_cw: str, end_cw: str) -> Path:
    """
    Executes R script with provided zip file and calendar weeks, returns path to results.
    """
    zip_file_path = Path(zip_file_path).resolve()
    cmd = ['Rscript', self.__rscript_path.as_posix(), zip_file_path.as_posix(), start_cw, end_cw, self.__los_max, self.__error_max]
    logging.info(f"Running command: {' '.join(cmd)}")
    output = subprocess.run(cmd, capture_output=True, text=True)
    if output.returncode != 0:
      raise RuntimeError(f"R script failed: {output.stderr}")
    logging.info("Rscript finished successfully")
    return self.__extract_result_path(output.stdout)

  def __extract_result_path(self, output: str) -> Path:
    """
    Extracts output file path from R script stdout.
    """
    match = re.search(r'timeframe_path:(.+?)(?:$|\n)', output)
    if not match:
      raise ValueError("Could not find result path in R script output")
    result_path = Path(match.group(1).strip().strip('"')).resolve()
    return result_path


class LosResultFileManager:
  """
  Manages result files for LOS calculations, including renaming and cleanup.
  """

  def rename_result_file_to_standardized_form(self, result_file_path: Path) -> Path:
    """
    Renames the result file to the standardized form:
    LOS_<year of CW-3>_W<CW-3>_to_<CurrentYear>-W<CurrentCW>_<CurrentDate>-<CurrentTime>
    """
    result_file_path = result_file_path.resolve()
    if not result_file_path.exists():
      raise FileNotFoundError(f'File {result_file_path} does not exist.')
    now = datetime.datetime.now()
    current_year, current_week, _ = now.isocalendar()
    adjusted_year, adjusted_week = self.calculate_cw_minus_three(current_year, current_week)
    timestamp = now.strftime('%Y%m%d-%H%M%S')
    new_filename = f'LOS_{adjusted_year}-W{adjusted_week:02d}_to_{current_year}-W{current_week:02d}_{timestamp}'
    new_file_path = result_file_path.with_name(new_filename + result_file_path.suffix)
    result_file_path.rename(new_file_path)
    return new_file_path

  def calculate_cw_minus_three(self, year: int, week: int) -> tuple[int, int]:
    if week > 3:
      return year, week - 3
    else:
      # Need to get last year's total weeks
      last_year = year - 1
      last_year_weeks = datetime.date(last_year, 12, 28).isocalendar()[1]
      return last_year, last_year_weeks - (3 - week)

  def clear_rscript_data(self, result_file_path: Path):
    result_dir = result_file_path.resolve().parent
    if not result_dir.exists():
      raise FileNotFoundError(f"Directory {result_dir} does not exist.")
    shutil.rmtree(result_dir)


class LosProcessor:
  def __init__(self, config_path: str):
    config_path = Path(config_path).resolve()
    self.__config_manager = ConfigurationManager(config_path)
    self.__broker_manager = BrokerRequestResultManager()
    self.__sftp_manager = SftpFileManager()
    self.__los_script = LosScriptManager()
    self.__result_manager = LosResultFileManager()

  def process(self):
    try:
      now = datetime.datetime.now()
      current_year, current_week, _ = now.isocalendar()
      _, adjusted_week = self.__result_manager.calculate_cw_minus_three(current_year, current_week)
      zip_path = self.__broker_manager.download_latest_broker_result_by_set_tag()
      result_path = self.__los_script.execute_rscript(zip_path, str(current_week), str(adjusted_week))
      renamed_path = self.__result_manager.rename_result_file_to_standardized_form(result_path)
      self.__clean_and_upload_sftp(renamed_path)
      self.__result_manager.clear_rscript_data(renamed_path)
    except Exception as e:
      logging.error(f"Error during LOS processing: {e}")
      raise

  def __clean_and_upload_sftp(self, file_path: Path):
    files = self.__sftp_manager.list_files()
    for file in files:
      self.__sftp_manager.delete_file(file)
    self.__sftp_manager.upload_file(file_path)


def main():
  logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
  if len(sys.argv) < 2:
    raise SystemExit('Path to config TOML is missing!')
  processor = LosProcessor(sys.argv[1])
  processor.process()


if __name__ == '__main__':
  main()
