# -*- coding: utf-8 -*-
"""
@AUTHOR: Alexander Kombeiz (akombeiz@ukaachen.de)
@VERSION=1.1
"""

import os
from pathlib import Path

import pytest

from src.los_script import ConfigurationManager


@pytest.fixture(scope="module")
def config_paths():
  base_path = Path(__file__).parent.parent / 'resources'
  return {
    'valid': str(base_path / 'test.toml'),
    'invalid': str(base_path / 'invalid.toml')
  }


@pytest.fixture(autouse=True)
def cleanup_env():
  """
  Clean up environment variables after each test
  """
  yield
  env_prefixes = ['BROKER', 'REQUESTS', 'SFTP', 'MISC', 'RSCRIPT']
  keys_to_remove = [
    key for key in os.environ
    if any(key.startswith(prefix) for prefix in env_prefixes)
  ]
  for key in keys_to_remove:
    del os.environ[key]


def test_valid_config_loads_successfully(config_paths):
  ConfigurationManager(config_paths['valid'])
  assert os.environ['BROKER.URL'] == 'test-url'
  assert os.environ['SFTP.HOST'] == 'test-host'


def test_missing_file_raises_error():
  with pytest.raises(SystemExit, match='invalid TOML file path'):
    ConfigurationManager('nonexistent.toml')


def test_missing_required_keys_raises_error(config_paths):
  with pytest.raises(SystemExit, match='following keys are missing'):
    ConfigurationManager(config_paths['invalid'])
