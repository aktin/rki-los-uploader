# -*- coding: utf-8 -*-
"""
@AUTHOR: Alexander Kombeiz (akombeiz@ukaachen.de)
"""

import datetime
import zipfile
from pathlib import Path
from unittest.mock import patch

import pytest

from src.los_script import LosResultFileManager


@pytest.fixture
def result_manager():
  return LosResultFileManager()


@pytest.fixture
def test_file(tmp_path: Path) -> Path:
  test_dir = tmp_path / "result"
  test_dir.mkdir()
  test_file = test_dir / "result1.csv"
  test_file.touch()
  return test_file


@patch("datetime.datetime", wraps=datetime.datetime)
def test_rename_result_file_to_standardized_form(mock_datetime, result_manager, test_file):
  # Set a fixed datetime (January 1, 2025, CW=1)
  fixed_datetime = datetime.datetime(2025, 1, 1, 12, 30, 45)
  mock_datetime.now.return_value = fixed_datetime
  new_file_path = result_manager.rename_result_file_to_standardized_form(test_file)
  expected_filename = "LOS_2024-W50_to_2025-W01_20250101-123045.csv"
  assert new_file_path.name == expected_filename
  assert new_file_path.exists()


def test_clear_rscript_data(result_manager, test_file):
  parent_dir = test_file.parent
  assert parent_dir.exists()
  result_manager.clear_rscript_data(test_file)
  assert not parent_dir.exists()


def test_zip_result_file(result_manager, test_file):
  zip_path = result_manager.zip_result_file(test_file)
  assert zip_path.exists()
  assert zip_path.suffix == '.zip'

  with zipfile.ZipFile(zip_path, 'r') as zf:
    file_list = zf.namelist()
    expected_path = f"{test_file.stem}/{test_file.name}"
    assert expected_path in file_list
