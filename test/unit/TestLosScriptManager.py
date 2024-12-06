# -*- coding: utf-8 -*-
"""
Created on 06.12.24
@AUTHOR: Wiliam Hoy (whoy@ukaachen.de), Alexander Kombeiz (akombeiz@ukaachen.de)
@VERSION=1.1
"""

import glob
import os
import shutil
import sys
import zipfile
from pathlib import Path

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.los_script import LosScriptManager


class TestLosScriptManager:

  @pytest.fixture(autouse=True)
  def setup(self):
    rscript_path = os.path.join(Path(__file__).parent.parent.parent, 'src', 'resources', 'LOSCalculator.R')
    os.environ['RSCRIPT.SCRIPT_PATH'] = rscript_path
    os.environ['RSCRIPT.LOS_MAX'] = "410"
    os.environ['RSCRIPT.ERROR_MAX'] = "25"
    self.start_cw = "29"
    self.end_cw = "34"
    self.los_manager = LosScriptManager()
    self.test_resources_path = os.path.join(Path(__file__).parent.parent, 'resources')
    self.test_zip_path = os.path.join(self.test_resources_path, 'test.zip')
    # Clean test files before test starts
    self.cleanup_test_files()
    yield
    # Clean test files after test completes or fails
    self.cleanup_test_files()

  def cleanup_test_files(self):
    """
    Removes test.zip and broker_result directory if they exist
    """
    if os.path.exists(self.test_zip_path):
      os.remove(self.test_zip_path)
    broker_result = os.path.join(self.test_resources_path, 'broker_result')
    if os.path.exists(broker_result):
      shutil.rmtree(broker_result)

  def test_single_clinic(self):
    test_data = ["aufnahme_ts\tentlassung_ts\ttriage_ts\ta_encounter_num\ta_encounter_ide\ta_billing_ide\n"
                 "2023-07-28T21:55:36Z\t2023-07-28T23:02:49Z\t2023-07-28T21:58:08Z\t4\t4\t4\n"
                 "2023-07-28T22:21:09Z\t2023-07-28T23:37:27Z\t2023-07-28T22:21:49Z\t5\t5\t5\n"
                 "2023-07-28T23:46:09Z\t2023-07-29T00:55:15Z\t2023-07-28T23:47:20Z\t6\t6\t6"]
    expected = self.__get_standard_expected_data("1")
    assert self.__compare_r_result_to_expected(test_data, expected)

  def test_multiple_clinics(self):
    test_data = [self.__get_standard_test_data(), self.__get_standard_test_data()]
    expected = self.__get_standard_expected_data("2")
    assert self.__compare_r_result_to_expected(test_data, expected)

  def test_missing_values_in_aufnahme_ts(self):
    test_data = ["aufnahme_ts\tentlassung_ts\ttriage_ts\ta_encounter_num\ta_encounter_ide\ta_billing_ide\n"
                 "2023-07-28T23:02:49Z\t2023-07-28T21:55:36Z\t4\t4\t4\n"
                 "2023-07-28T22:21:09Z\t2023-07-28T23:37:27Z\t2023-07-28T22:21:49Z\t5\t5\t5\n"
                 "2023-07-28T23:46:09Z\t2023-07-29T00:55:15Z\t2023-07-28T23:47:20Z\t6\t6\t6"]
    expected = self.__get_standard_expected_data("1")
    assert self.__compare_r_result_to_expected(test_data, expected)

  def test_completely_missing_values_in_aufnahme_ts(self):
    test_data = [
      "aufnahme_ts\tentlassung_ts\ttriage_ts\ta_encounter_num\ta_encounter_ide\ta_billing_ide\n"
      "\t2023-07-28T23:02:49Z\t2023-07-28T21:55:36Z\t4\t4\t4\n"
      "\t2023-07-28T23:37:27Z\t2023-07-28T22:21:09Z\t5\t5\t5\n"
      "\t2023-07-29T00:55:15Z\t2023-07-28T23:46:09Z\t6\t6\t6"]
    expected = self.__get_standard_expected_data("1")
    assert self.__compare_r_result_to_expected(test_data, expected)

  def test_no_column_aufnahme_ts(self):
    test_data = [
      "entlassung_ts\ttriage_ts\ta_encounter_num\ta_encounter_ide\ta_billing_ide\n"
      "2023-07-28T23:02:49Z\t2023-07-28T21:55:36Z\t4\t4\t4\n"
      "2023-07-28T23:37:27Z\t2023-07-28T22:21:09Z\t5\t5\t5\n"
      "2023-07-29T00:55:15Z\t2023-07-28T23:46:09Z\t6\t6\t6"]
    expected = self.__get_standard_expected_data("1")
    assert self.__compare_r_result_to_expected(test_data, expected)

  def test_no_column_entlassung_ts(self):
    test_data = [("triage_ts\ta_encounter_num\ta_encounter_ide\ta_billing_ide\n"
                  "2023-07-28T21:55:36Z\t4\t4\t4\n"
                  "2023-07-28T22:21:09Z\t5\t5\t5\n"
                  "2023-07-28T23:46:09Z\t6\t6\t6")]
    expected = [["no_data"], ["no_data"]]
    assert self.__compare_r_result_to_expected(test_data, expected)

  def __get_standard_expected_data(self, ed_count) -> list[list[str]]:
    return [["date", "ed_count", "visit_mean", "los_mean", "los_reference", "los_difference", "change"],
            ["2023-W30", ed_count, "3", "70.87", "193.54", "-122.66", "Abnahme"]]

  def __get_standard_test_data(self) -> str:
    return ("aufnahme_ts\tentlassung_ts\ttriage_ts\ta_encounter_num\ta_encounter_ide\ta_billing_ide\n"
            "2023-07-28T21:55:36Z\t2023-07-28T23:02:49Z\t2023-07-28T21:58:08Z\t4\t4\t4\n"
            "2023-07-28T22:21:09Z\t2023-07-28T23:37:27Z\t2023-07-28T22:21:49Z\t5\t5\t5\n"
            "2023-07-28T23:46:09Z\t2023-07-29T00:55:15Z\t2023-07-28T23:47:20Z\t6\t6\t6")

  def __compare_r_result_to_expected(self, test_data: list, expected_data: list[list[str]]):
    zip_path = self.__create_test_zip(test_data)
    result_path = self.los_manager.execute_rscript(str(zip_path), self.start_cw, self.end_cw)
    actual_df = pd.read_csv(result_path)
    expected_df = pd.DataFrame(expected_data[1:], columns=expected_data[0])
    return actual_df.equals(expected_df)

  def __create_test_zip(self, test_data: list) -> str:
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
    root_dir = os.path.join(self.test_resources_path, "test_result")
    os.makedirs(root_dir, exist_ok=True)
    # Create individual clinic data files and zips
    for i, clinic_data in enumerate(test_data):
      # Setup clinic directory
      clinic_name = f"{i}_result"
      clinic_dir = os.path.join(root_dir, clinic_name)
      os.makedirs(clinic_dir, exist_ok=True)
      # Save clinic data to file
      data_file = os.path.join(clinic_dir, "case_data.txt")
      with open(data_file, "w") as f:
        f.write(clinic_data)
      # Zip clinic data
      clinic_zip = f"{clinic_dir}.zip"
      with zipfile.ZipFile(clinic_zip, "w") as zf:
        zf.write(data_file, "case_data.txt")
      shutil.rmtree(clinic_dir)
    # Bundle all clinic zips into final zip
    with zipfile.ZipFile(self.test_zip_path, "w") as zf:
      for clinic_zip in glob.glob(os.path.join(root_dir, "*.zip")):
        zf.write(clinic_zip, os.path.basename(clinic_zip))
    shutil.rmtree(root_dir)
    return self.test_zip_path
