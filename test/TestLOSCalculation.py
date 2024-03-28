import os
import shutil
import unittest
import zipfile
from src.los_script import LosScriptManager
import pandas as pd

import toml


class TestLOSCalculation(unittest.TestCase):
    """
    Tests the R script if it returns the assumed result
    """

    def setUp(self):
        config = toml.load('/home/wiliam/Aktin/LOS_broker_calculator/config.toml')
        self.work_directory = config['MISC']['TEMP_DIR']
        self.r_script_path = config['RSCRIPT']['SCRIPT_PATH']
        self.zip_file_path = config['MISC']['TEST_RES_PATH']
        self.losman = LosScriptManager()
        self.result_dir = "resources"
        self.unittest_result_name = "unittest_result"

    def test_single_clinic(self):
        test_data = [("aufnahme_ts	entlassung_ts	triage_ts	a_encounter_num	a_encounter_ide	a_billing_ide\n"
                      "2023-07-28T21:55:36Z	2023-07-28T23:02:49Z	2023-07-28T21:58:08Z	4	4	4\n"
                      "2023-07-28T22:21:09Z	2023-07-28T23:37:27Z	2023-07-28T22:21:49Z	5	5	5\n"
                      "2023-07-28T23:46:09Z	2023-07-29T00:55:15Z	2023-07-28T23:47:20Z	6	6	6")]
        expected_data = [
            ["date", "ed_count", "visit_mean", "los_mean", "los_reference", "los_difference", "change"],
            ["2023-W30", "1", "3", "70.87", "193.54", "-122.66", "Abnahme"]]
        self.assertTrue(self.compare_r_result_to_expected(test_data, expected_data))

    def test_multiple_clinics(self):
        test_data = [("aufnahme_ts	entlassung_ts	triage_ts	a_encounter_num	a_encounter_ide	a_billing_ide\n"
                      "2023-07-28T21:55:36Z	2023-07-28T23:02:49Z	2023-07-28T21:58:08Z	4	4	4\n"
                      "2023-07-28T22:21:09Z	2023-07-28T23:37:27Z	2023-07-28T22:21:49Z	5	5	5\n"
                      "2023-07-28T23:46:09Z	2023-07-29T00:55:15Z	2023-07-28T23:47:20Z	6	6	6"),
                     ("aufnahme_ts	entlassung_ts	triage_ts	a_encounter_num	a_encounter_ide	a_billing_ide\n"
                      "2023-07-28T21:55:36Z	2023-07-28T23:02:49Z	2023-07-28T21:58:08Z	4	4	4\n"
                      "2023-07-28T22:21:09Z	2023-07-28T23:37:27Z	2023-07-28T22:21:49Z	5	5	5\n"
                      "2023-07-28T23:46:09Z	2023-07-29T00:55:15Z	2023-07-28T23:47:20Z	6	6	6")
                     ]
        expected_data = [
                ["date", "ed_count", "visit_mean", "los_mean", "los_reference", "los_difference", "change"],
                ["2023-W30", "2", "3", "70.87", "193.54", "-122.66", "Abnahme"]]
        self.assertTrue(self.compare_r_result_to_expected(test_data, expected_data))

    def test_missing_values_in_aufnahme_ts(self):
        test_data = [("aufnahme_ts	entlassung_ts	triage_ts	a_encounter_num	a_encounter_ide	a_billing_ide\n"
                      "\t2023-07-28T23:02:49Z	2023-07-28T21:55:36Z	4	4	4\n"
                      "2023-07-28T22:21:09Z	2023-07-28T23:37:27Z	2023-07-28T22:21:49Z	5	5	5\n"
                      "2023-07-28T23:46:09Z	2023-07-29T00:55:15Z	2023-07-28T23:47:20Z	6	6	6")]
        expected_data = [
                ["date", "ed_count", "visit_mean", "los_mean", "los_reference", "los_difference", "change"],
                ["2023-W30", "1", "3", "70.87", "193.54", "-122.66", "Abnahme"]]
        self.assertTrue(self.compare_r_result_to_expected(test_data, expected_data))

    def test_completely_missing_values_in_aufnahme_ts(self):
        test_data = [("aufnahme_ts	entlassung_ts	triage_ts	a_encounter_num	a_encounter_ide	a_billing_ide\n"
                      "\t2023-07-28T23:02:49Z	2023-07-28T21:55:36Z	4	4	4\n"
                      "\t2023-07-28T23:37:27Z	2023-07-28T22:21:09Z	5	5	5\n"
                      "\t2023-07-29T00:55:15Z	2023-07-28T23:46:09Z	6	6	6")]
        expected_data = [
            ["date", "ed_count", "visit_mean", "los_mean", "los_reference", "los_difference", "change"],
            ["2023-W30", "1", "3", "70.87", "193.54", "-122.66", "Abnahme"]]
        self.assertTrue(self.compare_r_result_to_expected(test_data, expected_data))

    def test_no_column_aufnahme_ts(self):
        test_data = [("entlassung_ts	triage_ts	a_encounter_num	a_encounter_ide	a_billing_ide\n"
                      "2023-07-28T23:02:49Z	2023-07-28T21:55:36Z	4	4	4\n"
                      "2023-07-28T23:37:27Z	2023-07-28T22:21:09Z	5	5	5\n"
                      "2023-07-29T00:55:15Z	2023-07-28T23:46:09Z	6	6	6")]
        expected_data = [
            ["date", "ed_count", "visit_mean", "los_mean", "los_reference", "los_difference", "change"],
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
        self.losman.execute_given_rscript(self.zip_file_path, self.r_script_path)

        with open("resources/broker_result/timeframe.csv", "r") as file:
            result = file.read()
            result_data = result.replace('\"', '').split("\n")
            # for each element in result_data split it by the delimiter ","
            result_data = [element.split(",") for element in result_data]
            # create pandas dataframe with results from R script
            results = pd.DataFrame(result_data[1:-1], columns=result_data[0])
            expected = pd.DataFrame(expected_data[1:], columns=expected_data[0])
            self.losman.delete_contents_of_dir(self.result_dir)
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
            i_result_path = self.result_dir + "/" + self.unittest_result_name + "/" + str(i) + "_result"
            os.makedirs(i_result_path, exist_ok=True)

            # create a file "case_data.txt" with the content
            with open(i_result_path + "/case_data.txt", "w") as file:
                file.write(contents[i])

            # convert 8_result to a zip file
            with zipfile.ZipFile(i_result_path + ".zip", "w") as zip_file:
                for root, dirs, files in os.walk(i_result_path):
                    for file in files:
                        zip_file.write(os.path.join(root, file),
                                       os.path.relpath(os.path.join(root, file), i_result_path))

            # delete the directory "8_result"
            shutil.rmtree(i_result_path)

        # convert unittest_result to a zip file
        with zipfile.ZipFile("resources/unittest_result.zip", "w") as zip_file:
            for root, dirs, files in os.walk("resources/unittest_result"):
                for file in files:
                    zip_file.write(os.path.join(root, file),
                                   os.path.relpath(os.path.join(root, file), "resources/unittest_result"))

        # delete the directory "unittest_result"
        shutil.rmtree("resources/unittest_result")
