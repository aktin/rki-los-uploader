# -*- coding: utf-8 -*-
"""
Created on 06.12.24
@AUTHOR: Alexander Kombeiz (akombeiz@ukaachen.de)
@VERSION=1.0
"""

import os
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.los_script import ConfigurationManager


class TestConfigurationManager:

  CONFIG_PATH = os.path.join(Path(__file__).parent.parent, 'resources', 'test.toml')
  INVALID_PATH = os.path.join(Path(__file__).parent.parent, 'resources', 'invalid.toml')

  def test_valid_config_loads_successfully(self):
    ConfigurationManager(self.CONFIG_PATH)
    assert os.environ['BROKER.URL'] == 'test-url'
    assert os.environ['SFTP.HOST'] == 'test-host'
    assert os.environ['RSCRIPT.SCRIPT_PATH'] == '/path/to/script'

  def test_missing_file_raises_error(self):
    with pytest.raises(SystemExit, match='invalid TOML file path'):
      ConfigurationManager('nonexistent.toml')

  def test_missing_required_keys_raises_error(self):
    with pytest.raises(SystemExit, match='following keys are missing'):
      ConfigurationManager(self.INVALID_PATH)

  @pytest.fixture(autouse=True)
  def cleanup_env(self):
    """Clean up environment variables after each test"""
    yield
    keys_to_remove = [
      key for key in os.environ.keys()
      if any(prefix in key for prefix in
             ['BROKER', 'REQUESTS', 'SFTP', 'MISC', 'RSCRIPT'])
    ]
    for key in keys_to_remove:
      del os.environ[key]
