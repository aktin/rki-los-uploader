# -*- coding: utf-8 -*-
"""
@AUTHOR: Wiliam Hoy (whoy@ukaachen.de), Alexander Kombeiz (akombeiz@ukaachen.de)
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
import zipfile
from pathlib import Path

import paramiko
import requests
import toml
from requests import Response


class ConfigurationManager:
  """Manages TOML configuration loading and environment variable setup.

  This class validates and loads configuration from a TOML file into environment
  variables for use by other components. It ensures all required configuration
  keys are present and properly formatted.

  Attributes:
      __required_keys (set): Set of configuration keys that must be present
  """

  __required_keys = {
    'BROKER.URL', 'BROKER.API_KEY',
    'REQUESTS.TAG',
    'SFTP.HOST', 'SFTP.PORT', 'SFTP.USERNAME', 'SFTP.PASSWORD', 'SFTP.TIMEOUT', 'SFTP.FOLDER',
    'RSCRIPT.LOS_SCRIPT_PATH', 'RSCRIPT.LOS_MAX', 'RSCRIPT.ERROR_MAX'
  }

  def __init__(self, path_toml: Path):
    self.__verify_and_load_toml(path_toml)

  def __verify_and_load_toml(self, path_toml: Path):
    path_toml = Path(path_toml).resolve()
    logging.info("Loading configuration from %s", path_toml)
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

  def __flatten_dict(self, d: dict, parent_key: str = '', sep: str = '.') -> dict:
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
  """Manages SFTP server file operations.

  Handles uploading, listing and deleting files on a configured SFTP server.
  Uses environment variables for connection settings.
  """

  def __init__(self):
    self.__sftp_host = os.environ['SFTP.HOST']
    self.__sftp_port = int(os.environ['SFTP.PORT'])
    self.__sftp_username = os.environ['SFTP.USERNAME']
    self.__sftp_password = os.environ['SFTP.PASSWORD']
    self.__sftp_timeout = int(os.environ['SFTP.TIMEOUT'])
    self.__sftp_folder = Path(os.environ['SFTP.FOLDER'])
    self.__connection = self.__connect_to_sftp()

  def __connect_to_sftp(self) -> paramiko.sftp_client.SFTPClient:
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(
        self.__sftp_host,
        port=self.__sftp_port,
        username=self.__sftp_username,
        password=self.__sftp_password,
        timeout=self.__sftp_timeout,
        allow_agent=False,
        look_for_keys=False
    )
    return ssh.open_sftp()

  def upload_file(self, path_file: Path):
    path_file = Path(path_file).resolve()
    logging.info('Uploading %s to SFTP server', path_file)
    if not path_file.exists():
      raise FileNotFoundError(f"File {path_file} does not exist.")
    self.__connection.put(str(path_file), str(self.__sftp_folder / path_file.name))

  def list_files(self) -> list:
    return self.__connection.listdir(str(self.__sftp_folder))

  def delete_file(self, filename: str):
    logging.info('Deleting %s from SFTP server', filename)
    try:
      self.__connection.remove(str(self.__sftp_folder / filename))
    except FileNotFoundError:
      logging.warning("File not found on SFTP server")


class BrokerRequestResultManager:
  """Manages interactions with AKTIN Broker API. The AKTIN Broker is the data source from where our data is being imported.

  Handles downloading and processing of hospital data from the AKTIN Broker.
  Uses environment variables for connection settings.
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
      logging.info("Broker server is reachable")
    except requests.exceptions.Timeout:
      raise SystemExit('Connection to AKTIN Broker timed out')
    except requests.exceptions.HTTPError as err:
      raise SystemExit(f'HTTP error occurred: {err}')
    except requests.exceptions.RequestException as err:
      raise SystemExit(f'An ambiguous error occurred: {err}')

  def __append_to_broker_url(self, *items: str) -> str:
    return "/".join([self.__broker_url] + list(items))

  def __create_basic_header(self, mediatype: str = 'application/xml') -> dict:
    return {
      'Authorization': f'Bearer {self.__admin_api_key}',
      'Connection': 'keep-alive',
      'Accept': mediatype
    }

  def download_latest_broker_result_by_set_tag(self, zip_target_path: Path = None, requests_tag: str = None) -> Path:
    logging.info("Fetching requests with tag=%s", requests_tag)
    id_request = str(self.__get_id_of_latest_request_by_set_tag(requests_tag))
    uuid = self.__export_request_result(id_request)
    result_stream = self.__download_exported_result(uuid)
    zip_file_path = self.__store_broker_response_as_zip(result_stream, id_request, zip_target_path)
    return zip_file_path

  def __get_id_of_latest_request_by_set_tag(self, requests_tag: str = None) -> int:
    requests_tag = requests_tag or self.__requests_tag
    url = self.__append_to_broker_url('broker', 'request', 'filtered')
    url = '?'.join([url, urllib.parse.urlencode({'type': 'application/vnd.aktin.query.request+xml', 'predicate': f"//tag='{requests_tag}'"})])
    response = requests.get(url, headers=self.__create_basic_header(), timeout=self.__timeout)
    response.raise_for_status()
    list_request_id = [int(element.get('id')) for element in et.fromstring(response.content)]
    if not list_request_id:
      logging.warning("No requests found")
      sys.exit(0)
    max_id = max(list_request_id)
    logging.info("Found requests count=%d max_id=%d", len(list_request_id), max_id)
    return max_id

  def __export_request_result(self, id_request: str) -> str:
    logging.info("Exporting broker results request_id=%s", id_request)
    url = self.__append_to_broker_url('broker', 'export', 'request-bundle', id_request)
    response = requests.post(url, headers=self.__create_basic_header('text/plain'), timeout=self.__timeout)
    response.raise_for_status()
    return response.text

  def __download_exported_result(self, uuid: str) -> Response:
    logging.info("Downloading broker results uuid=%s", uuid)
    url = self.__append_to_broker_url('broker', 'download', uuid)
    response = requests.get(url, headers=self.__create_basic_header(), timeout=self.__timeout)
    response.raise_for_status()
    return response

  def __store_broker_response_as_zip(self, response: Response, id_request: str, target_path: Path = None) -> Path:
    target_path = target_path or Path(__file__).resolve().parent
    zip_file_path = target_path / f'result{id_request}.zip'
    logging.info("Writing broker results to file path=%s", zip_file_path)
    with zip_file_path.open('wb') as zip_file:
      zip_file.write(response.content)
    return zip_file_path


class LosScriptManager:
  """Manages R script execution for length of stay calculations.

  Handles running the R script with appropriate parameters and processing
  its output. Uses environment variables for R script settings.
  """

  def __init__(self):
    self.__los_script_path = Path(os.environ['RSCRIPT.LOS_SCRIPT_PATH']).resolve()
    self.__los_max = os.environ['RSCRIPT.LOS_MAX']
    self.__error_max = os.environ['RSCRIPT.ERROR_MAX']

  def execute_rscript(self, zip_file_path: Path, start_cw: str, end_cw: str) -> Path:
    zip_file_path = Path(zip_file_path).resolve()
    cmd = ['Rscript', self.__los_script_path.as_posix(), zip_file_path.as_posix(), start_cw, end_cw, self.__los_max, self.__error_max]
    logging.info("Executing R script command='%s'", ' '.join(cmd))
    output = subprocess.run(cmd, capture_output=True, text=True)
    if output.returncode != 0:
      raise RuntimeError(f"R script failed: {output.stderr}")
    logging.info("R script execution completed successfully")
    return self.__extract_result_path(output.stdout)

  def __extract_result_path(self, output: str) -> Path:
    match = re.search(r'timeframe_path:(.+?)(?:$|\n)', output)
    if not match:
      raise ValueError("Could not find result path in script output")
    result_path = Path(match.group(1).strip().strip('"')).resolve()
    return result_path


class LosResultFileManager:
  """Manages LOS calculation result files.

  Handles renaming, zipping and cleanup of result files according to standardized format.
  Can create zip archives with standardized folder structure for result files.
  """

  def rename_result_file_to_standardized_form(self, file_path: Path) -> Path:
    file_path = file_path.resolve()
    logging.info("Standardizing result filename path=%s", file_path)
    if not file_path.exists():
      raise FileNotFoundError(f'File {file_path} does not exist.')
    now = datetime.datetime.now()
    current_year, current_week, _ = now.isocalendar()
    adjusted_year, adjusted_week = self.calculate_cw_minus_three(current_year, current_week)
    timestamp = now.strftime('%Y%m%d-%H%M%S')
    new_filename = f'LOS_{adjusted_year}-W{adjusted_week:02d}_to_{current_year}-W{current_week:02d}_{timestamp}'
    new_file_path = file_path.with_name(new_filename + file_path.suffix)
    file_path.rename(new_file_path)
    return new_file_path

  def calculate_cw_minus_three(self, year: int, week: int) -> tuple[int, int]:
    if week > 3:
      return year, week - 3
    else:
      last_year = year - 1
      last_year_weeks = datetime.date(last_year, 12, 28).isocalendar()[1]
      return last_year, last_year_weeks - (3 - week)

  def zip_result_file(self, file_path: Path) -> Path:
    file_path = file_path.resolve()
    logging.info("Creating ZIP archive for result file path=%s", file_path)
    if not file_path.exists():
      raise FileNotFoundError(f'File {file_path} does not exist.')
    folder_name = file_path.stem
    folder_path = file_path.parent / folder_name
    folder_path.mkdir(exist_ok=True)
    shutil.copy2(file_path, folder_path / file_path.name)
    zip_path = file_path.with_suffix('.zip')
    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
      for file in folder_path.rglob('*'):
        zf.write(file, file.relative_to(folder_path.parent))
    shutil.rmtree(folder_path)
    return zip_path

  def clear_rscript_data(self, result_file_path: Path):
    result_dir = result_file_path.resolve().parent
    logging.info("Cleaning R script data directory path=%s", result_dir)
    if not result_dir.exists():
      raise FileNotFoundError(f"Directory {result_dir} does not exist.")
    shutil.rmtree(result_dir)


class LosProcessor:
  """Main pipeline processor for Length of Stay calculations.

  Coordinates the end-to-end process of:
  1. Loading configuration
  2. Downloading broker data
  3. Running R script analysis
  4. Zipping result file
  5. Uploading results to SFTP
  """

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
      raw_data_zip = self.__broker_manager.download_latest_broker_result_by_set_tag()
      processed_data = self.__los_script.execute_rscript(raw_data_zip, str(current_week), str(adjusted_week))
      renamed_data = self.__result_manager.rename_result_file_to_standardized_form(processed_data)
      zipped_data = self.__result_manager.zip_result_file(renamed_data)
      self.__clean_and_upload_sftp(zipped_data)
      self.__result_manager.clear_rscript_data(zipped_data)
    except Exception as e:
      logging.error(f"Error during LOS processing: {e}", exc_info=True)
      raise

  def __clean_and_upload_sftp(self, file_path: Path):
    files = self.__sftp_manager.list_files()
    for file in files:
      self.__sftp_manager.delete_file(file)
    self.__sftp_manager.upload_file(file_path)


def main():
  logging.basicConfig(
      level=logging.INFO,
      format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
  )
  if len(sys.argv) < 2:
    raise SystemExit('Path to config TOML is missing!')
  processor = LosProcessor(sys.argv[1])
  processor.process()


if __name__ == '__main__':
  main()
