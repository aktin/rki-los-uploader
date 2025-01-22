# -*- coding: utf-8 -*-
"""
Created on 06.12.24
@AUTHOR: Wiliam Hoy (whoy@ukaachen.de), Alexander Kombeiz (akombeiz@ukaachen.de)
@VERSION=1.2
"""

import os
import sys
import zipfile
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.los_script import LosScriptManager


@pytest.fixture(scope="function", autouse=True)
def setup_env(tmp_path):
  os.environ['RSCRIPT.SCRIPT_PATH'] = str(Path(__file__).parent.parent.parent / 'src/resources/LOSCalculator.R')
  os.environ['RSCRIPT.LOS_MAX'] = "410"
  os.environ['RSCRIPT.ERROR_MAX'] = "25"
  yield
  # Cleanup after tests
  os.environ.pop('RSCRIPT.SCRIPT_PATH', None)
  os.environ.pop('RSCRIPT.LOS_MAX', None)
  os.environ.pop('RSCRIPT.ERROR_MAX', None)


@pytest.fixture(scope="function")
def los_manager() -> LosScriptManager:
  return LosScriptManager()


@pytest.fixture(scope="function")
# test files are automatically created in /tmp/
def test_zip_path(tmp_path) -> Path:
  return tmp_path / "test.zip"


@pytest.fixture(scope="function")
def start_end_cw() -> tuple[str, str]:
  return "29", "34"

def test_single_clinic(los_manager: LosScriptManager, test_zip_path: Path, start_end_cw: tuple[str, str]):
  test_data = [__get_standard_test_data()]
  expected = __get_standard_expected_data("1")
  assert __compare_r_result_to_expected(los_manager, test_zip_path, start_end_cw, test_data, expected)


def test_multiple_clinics(los_manager: LosScriptManager, test_zip_path: Path, start_end_cw: tuple[str, str]):
  test_data = [__get_standard_test_data(), __get_standard_test_data()]
  expected = __get_standard_expected_data("2")
  assert __compare_r_result_to_expected(los_manager, test_zip_path, start_end_cw, test_data, expected)


def test_missing_values_in_aufnahme_ts(los_manager: LosScriptManager, test_zip_path: Path, start_end_cw: tuple[str, str]):
  test_data = ["aufnahme_ts\tentlassung_ts\ttriage_ts\ta_encounter_num\ta_encounter_ide\ta_billing_ide\n"
               "2023-07-28T21:55:36Z\t2023-07-28T23:02:49Z\t\t4\t4\t4\n"
               "2023-07-28T22:21:09Z\t2023-07-28T23:37:27Z\t2023-07-28T22:21:49Z\t5\t5\t5\n"
               "2023-07-28T23:46:09Z\t2023-07-29T00:55:15Z\t2023-07-28T23:47:20Z\t6\t6\t6"]
  expected = __get_standard_expected_data("1")
  assert __compare_r_result_to_expected(los_manager, test_zip_path, start_end_cw, test_data, expected)


def test_completely_missing_values_in_aufnahme_ts(los_manager: LosScriptManager, test_zip_path: Path, start_end_cw: tuple[str, str]):
  test_data = [
    "aufnahme_ts\tentlassung_ts\ttriage_ts\ta_encounter_num\ta_encounter_ide\ta_billing_ide\n"
    "\t2023-07-28T23:02:49Z\t2023-07-28T21:55:36Z\t4\t4\t4\n"
    "\t2023-07-28T23:37:27Z\t2023-07-28T22:21:09Z\t5\t5\t5\n"
    "\t2023-07-29T00:55:15Z\t2023-07-28T23:46:09Z\t6\t6\t6"]
  expected = __get_standard_expected_data("1")
  assert __compare_r_result_to_expected(los_manager, test_zip_path, start_end_cw, test_data, expected)


def test_no_column_aufnahme_ts(los_manager: LosScriptManager, test_zip_path: Path, start_end_cw: tuple[str, str]):
  test_data = [
    "entlassung_ts\ttriage_ts\ta_encounter_num\ta_encounter_ide\ta_billing_ide\n"
    "2023-07-28T23:02:49Z\t2023-07-28T21:55:36Z\t4\t4\t4\n"
    "2023-07-28T23:37:27Z\t2023-07-28T22:21:09Z\t5\t5\t5\n"
    "2023-07-29T00:55:15Z\t2023-07-28T23:46:09Z\t6\t6\t6"]
  expected = __get_standard_expected_data("1")
  assert __compare_r_result_to_expected(los_manager, test_zip_path, start_end_cw, test_data, expected)


def test_no_column_entlassung_ts(los_manager: LosScriptManager, test_zip_path: Path, start_end_cw: tuple[str, str]):
  test_data = [("triage_ts\ta_encounter_num\ta_encounter_ide\ta_billing_ide\n"
                "2023-07-28T21:55:36Z\t4\t4\t4\n"
                "2023-07-28T22:21:09Z\t5\t5\t5\n"
                "2023-07-28T23:46:09Z\t6\t6\t6")]
  expected = [["message"], ["Error: No Data found in case_data files!"]]
  assert __compare_r_result_to_expected(los_manager, test_zip_path, start_end_cw, test_data, expected)

def test_turn_of_the_year(los_manager: LosScriptManager, test_zip_path: Path, start_end_cw: tuple[str, str]):
  test_data = [("aufnahme_ts\tentlassung_ts\ttriage_ts\ta_encounter_num\ta_encounter_ide\ta_billing_ide\n"
          "2023-12-31T21:55:36Z\t2023-12-31T23:02:49Z\t2023-12-31T21:58:08Z\t4\t4\t4\n"
          "2023-12-31T22:21:09Z\t2023-12-31T23:37:27Z\t2023-12-31T22:21:49Z\t5\t5\t5\n"
          "2023-12-31T23:46:09Z\t2024-01-01T00:55:15Z\t2023-12-31T23:47:20Z\t6\t6\t6")]
  expected = [["date", "ed_count", "visit_mean", "los_mean", "los_reference", "los_difference", "change"],
          ["2023-W52", "1", "3", "70.87", "193.54", "-122.66", "Abnahme"]]
  assert __compare_r_result_to_expected(los_manager, test_zip_path, ("50", "03"), test_data, expected)


def __get_standard_test_data() -> str:
  return ("aufnahme_ts\tentlassung_ts\ttriage_ts\ta_encounter_num\ta_encounter_ide\ta_billing_ide\n"
          "2023-07-28T21:55:36Z\t2023-07-28T23:02:49Z\t2023-07-28T21:58:08Z\t4\t4\t4\n"
          "2023-07-28T22:21:09Z\t2023-07-28T23:37:27Z\t2023-07-28T22:21:49Z\t5\t5\t5\n"
          "2023-07-28T23:46:09Z\t2023-07-29T00:55:15Z\t2023-07-28T23:47:20Z\t6\t6\t6")


def __get_standard_expected_data(ed_count) -> list[list[str]]:
  return [["date", "ed_count", "visit_mean", "los_mean", "los_reference", "los_difference", "change"],
          ["2023-W30", ed_count, "3", "70.87", "193.54", "-122.66", "Abnahme"]]


def __compare_r_result_to_expected(los_manager: LosScriptManager, test_zip_path: Path, start_end_cw: tuple[str, str], test_data: list[str],
    expected_data: list[list[str]]) -> bool:
  zip_path = __create_test_zip(test_zip_path, test_data)
  result_path = los_manager.execute_rscript(str(zip_path), *start_end_cw)
  actual_df = pd.read_csv(result_path)
  expected_df = pd.DataFrame(expected_data[1:], columns=expected_data[0])
  return actual_df.astype(str).equals(expected_df)


def __create_test_zip(zip_path: Path, test_data: list[str]) -> Path:
  """
  Creates a zip file containing clinic test data for testing. It mimics the
  expected broker request format: one main zip containing individual clinic
  zip files, each with a case_data.txt. i.e.
  Input: Two csvs as a string list
  Output:
  test.zip/
  --1_result.zip/
  ----case_data.txt
  --2_result.zip/
  ----case_data.txt
  """
  with zipfile.ZipFile(zip_path, "w") as zf:
    for i, clinic_data in enumerate(test_data):
      clinic_zip_name = f"{i}_result.zip"
      clinic_zip_path = zip_path.parent / clinic_zip_name
      with zipfile.ZipFile(clinic_zip_path, "w") as clinic_zf:
        clinic_zf.writestr("case_data.txt", clinic_data)
      zf.write(clinic_zip_path, clinic_zip_name)
  return zip_path
