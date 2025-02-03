# -*- coding: utf-8 -*-
"""
@AUTHOR: Alexander Kombeiz (akombeiz@ukaachen.de)
"""

#
#  Copyright (c) 2025 AKTIN
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU Affero General Public License as
#  published by the Free Software Foundation, either version 3 of the
#  License, or (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU Affero General Public License for more details.
#
#  You should have received a copy of the GNU Affero General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

import os
from pathlib import Path

import pytest

from src.los_script import ConfigurationManager


@pytest.fixture
def valid_toml_content() -> str:
  return """
[BROKER]
URL = "test-url"
API_KEY = "test-key"

[REQUESTS]
TAG = "test-tag"

[SFTP]
HOST = "test-host"
PORT = "22"
USERNAME = "test-user"
PASSWORD = "test-pass"
TIMEOUT = "30"
FOLDER = "test-folder"

[RSCRIPT]
LOS_SCRIPT_PATH = "/path/to/script"
LOS_MAX = "30"
ERROR_MAX = "0.05"
CLINIC_NUMS="1-5,7,9-10"
"""


@pytest.fixture
def invalid_toml_content() -> str:
  return """
[BROKER]
URL = "test-url"
API_KEY = "test-key"
"""


@pytest.fixture
def config_paths(valid_toml_content, invalid_toml_content, tmp_path) -> dict:
  valid_path = tmp_path / "valid.toml"
  invalid_path = tmp_path / "invalid.toml"
  valid_path.write_text(valid_toml_content)
  invalid_path.write_text(invalid_toml_content)
  return {'valid': valid_path, 'invalid': invalid_path}


@pytest.fixture(autouse=True)
def cleanup_env():
  """
  Clean up environment variables after each test
  """
  yield
  env_prefixes = ['BROKER', 'REQUESTS', 'SFTP', 'MISC', 'RSCRIPT']
  keys_to_remove = [key for key in os.environ if any(key.startswith(prefix) for prefix in env_prefixes)]
  for key in keys_to_remove:
    del os.environ[key]


def test_valid_config_loads_successfully(config_paths):
  ConfigurationManager(config_paths['valid'])
  assert os.environ['BROKER.URL'] == 'test-url'
  assert os.environ['SFTP.HOST'] == 'test-host'
  assert os.environ['RSCRIPT.CLINIC_NUMS'] == '1,2,3,4,5,7,9,10'


def test_missing_file_raises_error():
  with pytest.raises(SystemExit, match='Invalid TOML file path'):
    ConfigurationManager(Path('nonexistent.toml'))


def test_missing_required_keys_raises_error(config_paths):
  with pytest.raises(SystemExit, match='Missing keys in config file'):
    ConfigurationManager(config_paths['invalid'])
