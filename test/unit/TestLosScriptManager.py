# -*- coding: utf-8 -*-
"""
Created on 06.12.24
@AUTHOR: Wiliam Hoy (whoy@ukaachen.de), Alexander Kombeiz (akombeiz@ukaachen.de)
@VERSION=1.1
"""

import os
import shutil
import sys
import zipfile
from pathlib import Path
import glob

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).parent.parent.parent))
from src.los_script import LosScriptManager


class TestLosScriptManager:

  def __init__(self):
    rscript_path = os.path.join(Path(__file__).parent.parent.parent, 'src', 'resources', 'LOSCalculator.R')
    os.environ['RSCRIPT.SCRIPT_PATH'] = rscript_path
    os.environ['RSCRIPT.LOS_MAX'] = "410"
    os.environ['RSCRIPT.ERROR_MAX'] = "25"
    self.start_cw = 29
    self.end_cw = 34
    self.los_manager = LosScriptManager()
    self.test_resources_path = os.path.join(Path(__file__).parent.parent, 'resources')
    self.test_zip_path = os.path.join(self.test_resources_path, 'test.zip')

  @pytest.fixture(autouse=True)
  def cleanup(self):
    """Clean test files after each test"""
    if os.path.exists(self.test_zip_path):
      os.remove(self.test_zip_path)

  def __create_test_zip(self, test_data: list[str]) -> str:
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

  def __compare_results(self, actual_path: str, expected_data: list[list[str]]) -> bool:
    actual_df = pd.read_csv(actual_path)
    expected_df = pd.DataFrame(expected_data[1:], columns=expected_data[0])
    return actual_df.equals(expected_df)

  def test_single_clinic(self):
    test_data = ["aufnahme_ts\tentlassung_ts\ttriage_ts\ta_encounter_num\ta_encounter_ide\ta_billing_ide\n"
                 "2023-07-28T21:55:36Z\t2023-07-28T23:02:49Z\t2023-07-28T21:58:08Z\t4\t4\t4\n"
                 "2023-07-28T22:21:09Z\t2023-07-28T23:37:27Z\t2023-07-28T22:21:49Z\t5\t5\t5\n"
                 "2023-07-28T23:46:09Z\t2023-07-29T00:55:15Z\t2023-07-28T23:47:20Z\t6\t6\t6"]
    expected = [
      ["date", "ed_count", "visit_mean", "los_mean", "los_reference", "los_difference", "change"],
      ["2023-W30", "1", "3", "70.87", "193.54", "-122.66", "Abnahme"]
    ]
    zip_path = self.__create_test_zip(test_data)
    result_path = self.los_manager.execute_rscript(str(zip_path), self.start_cw, self.end_cw)
    assert self.__compare_results(result_path, expected) == True




  def test_multiple_clinics(self):
    test_data = [
      "aufnahme_ts	entlassung_ts	triage_ts	a_encounter_num	a_encounter_ide	a_billing_ide\n"
      "2023-07-28T21:55:36Z	2023-07-28T23:02:49Z	2023-07-28T21:58:08Z	4	4	4\n"
      "2023-07-28T22:21:09Z	2023-07-28T23:37:27Z	2023-07-28T22:21:49Z	5	5	5\n"
      "2023-07-28T23:46:09Z	2023-07-29T00:55:15Z	2023-07-28T23:47:20Z	6	6	6",

        "aufnahme_ts	entlassung_ts	triage_ts	a_encounter_num	a_encounter_ide	a_billing_ide\n"
        "2023-07-28T21:55:36Z	2023-07-28T23:02:49Z	2023-07-28T21:58:08Z	4	4	4\n"
        "2023-07-28T22:21:09Z	2023-07-28T23:37:27Z	2023-07-28T22:21:49Z	5	5	5\n"
        "2023-07-28T23:46:09Z	2023-07-29T00:55:15Z	2023-07-28T23:47:20Z	6	6	6"
    ]
    expected_data = [
      ["date", "ed_count", "visit_mean", "los_mean", "los_reference",
       "los_difference", "change"],
      ["2023-W30", "2", "3", "70.87", "193.54", "-122.66", "Abnahme"]]
    self.assertTrue(self.compare_r_result_to_expected(test_data, expected_data))

  def test_missing_values_in_aufnahme_ts(self):
    test_data = [(
      "aufnahme_ts	entlassung_ts	triage_ts	a_encounter_num	a_encounter_ide	a_billing_ide\n"
      "\t2023-07-28T23:02:49Z	2023-07-28T21:55:36Z	4	4	4\n"
      "2023-07-28T22:21:09Z	2023-07-28T23:37:27Z	2023-07-28T22:21:49Z	5	5	5\n"
      "2023-07-28T23:46:09Z	2023-07-29T00:55:15Z	2023-07-28T23:47:20Z	6	6	6")]
    expected_data = [
      ["date", "ed_count", "visit_mean", "los_mean", "los_reference",
       "los_difference", "change"],
      ["2023-W30", "1", "3", "70.87", "193.54", "-122.66", "Abnahme"]]
    self.assertTrue(self.compare_r_result_to_expected(test_data, expected_data))

  def test_completely_missing_values_in_aufnahme_ts(self):
    test_data = [(
      "aufnahme_ts	entlassung_ts	triage_ts	a_encounter_num	a_encounter_ide	a_billing_ide\n"
      "\t2023-07-28T23:02:49Z	2023-07-28T21:55:36Z	4	4	4\n"
      "\t2023-07-28T23:37:27Z	2023-07-28T22:21:09Z	5	5	5\n"
      "\t2023-07-29T00:55:15Z	2023-07-28T23:46:09Z	6	6	6")]
    expected_data = [
      ["date", "ed_count", "visit_mean", "los_mean", "los_reference",
       "los_difference", "change"],
      ["2023-W30", "1", "3", "70.87", "193.54", "-122.66", "Abnahme"]]
    self.assertTrue(self.compare_r_result_to_expected(test_data, expected_data))

  def test_no_column_aufnahme_ts(self):
    test_data = [
      ("entlassung_ts	triage_ts	a_encounter_num	a_encounter_ide	a_billing_ide\n"
       "2023-07-28T23:02:49Z	2023-07-28T21:55:36Z	4	4	4\n"
       "2023-07-28T23:37:27Z	2023-07-28T22:21:09Z	5	5	5\n"
       "2023-07-29T00:55:15Z	2023-07-28T23:46:09Z	6	6	6")]
    expected_data = [
      ["date", "ed_count", "visit_mean", "los_mean", "los_reference",
       "los_difference", "change"],
      ["2023-W30", "1", "3", "70.87", "193.54", "-122.66", "Abnahme"]]
    self.assertTrue(self.compare_r_result_to_expected(test_data, expected_data))

  def test_no_column_entlassug_ts(self):
    test_data = [("triage_ts	a_encounter_num	a_encounter_ide	a_billing_ide\n"
                  "2023-07-28T21:55:36Z	4	4	4\n"
                  "2023-07-28T22:21:09Z	5	5	5\n"
                  "2023-07-28T23:46:09Z	6	6	6")]
    expected_data = [["no_data"], ["no_data"]]
    self.assertTrue(self.compare_r_result_to_expected(test_data, expected_data))

  def compare_r_result_to_expected(self, test_data, expected_data):
    self.__pack_zip__(test_data)
    result_path = self.losman.execute_given_rscript(
        self.abs_path_result).replace("\"", "")

    with open(result_path, "r") as file:
      result = file.read()
      result_data = result.replace('\"', '').split("\n")
      # for each element in result_data split it by the delimiter ","
      result_data = [element.split(",") for element in result_data]
      # create pandas dataframe with results from R script
      results = pd.DataFrame(result_data[1:-1], columns=result_data[0])
      expected = pd.DataFrame(expected_data[1:], columns=expected_data[0])
      self.dir_manager.cleanup()
      return results.equals(expected)

  def __pack_zip__(self, contents: list[str]):
    """
    This method creates a zip file from the given content-array and saves it in the resources folder in the way the broker
    request would structure it.
    :param contents:
    :return:
    """

    for i in range(len(contents)):
      # create a directory "unittest_result" with a subdirectory "i_result"
      i_result_path = self.unittest_result_path + "/" + str(i) + "_result"
      os.makedirs(i_result_path, exist_ok=True)

      # create a file "case_data.txt" with the content
      with open(i_result_path + "/case_data.txt", "w") as file:
        file.write(contents[i])

      # convert 8_result to a zip file
      with zipfile.ZipFile(i_result_path + ".zip", "w") as zip_file:
        for root, dirs, files in os.walk(i_result_path):
          for file in files:
            zip_file.write(os.path.join(root, file),
                           os.path.relpath(os.path.join(root, file),
                                           i_result_path))

      # delete the directory "8_result"
      shutil.rmtree(i_result_path)

    # convert unittest_result to a zip file
    with zipfile.ZipFile(self.unittest_result_path + ".zip", "w") as zip_file:
      for root, dirs, files in os.walk(self.unittest_result_path):
        for file in files:
          zip_file.write(os.path.join(root, file),
                         os.path.relpath(os.path.join(root, file),
                                         self.unittest_result_path))

    # delete the directory "unittest_result"
    shutil.rmtree(self.unittest_result_path)
