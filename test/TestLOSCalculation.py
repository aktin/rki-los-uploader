class TestLOSCalculation(unittest.TestCase):
    """
    Tests the R script if it returns the assumed result
    """
    def setUp(self, path_toml: str = "config.toml"):
        config = toml.load(path_toml)
        self.work_directory = config['misc']['temp_dir']
        self.r_script_path = os.path.join(self.work_directory, "LOSCalculator.R")
        self.zip_file_path = os.path.join(self.work_directory, 'resources\\unittest_result.zip')

    def test_single_clinic(self):
        test_data = [("aufnahme_ts	entlassung_ts	triage_ts	a_encounter_num	a_encounter_ide	a_billing_ide\n"
                     "2023-07-28T21:55:36Z	2023-07-28T23:02:49Z	2023-07-28T21:58:08Z	4	4	4\n"
                     "2023-07-28T22:21:09Z	2023-07-28T23:37:27Z	2023-07-28T22:21:49Z	5	5	5\n"
                     "2023-07-28T23:46:09Z	2023-07-29T00:55:15Z	2023-07-28T23:47:20Z	6	6	6")]
        self.__pack_zip__(test_data)

        execute_rscript(self.zip_file_path, self.r_script_path)

        with open("resources\\broker_result\\timeframe.csv", "r") as file:
            result = file.read()
            result_data = result.replace('\"', '').split("\n")
            # for each element in result_data split it by the delimiter ","
            result_data = [element.split(",") for element in result_data]
            # create pandas dataframe with results from R script
            results = pd.DataFrame(result_data[1:-1], columns=result_data[0])
            #check if the dataframe has the expected values
            expected_data = [["date", "ed_count", "visit_mean", "los_mean", "los_reference", "los_difference", "change"],
                    ["2023-W30", "1", "3", "69.41", "193.54", "-124.12", "Abnahme"]]

            expected = pd.DataFrame(expected_data[1:], columns=expected_data[0])
            self.assertEqual(results.to_string(), expected.to_string())

    def test_multiple_clinics(self):
        test_data = [("aufnahme_ts	entlassung_ts	triage_ts	a_encounter_num	a_encounter_ide	a_billing_ide\n"
                      "2023-07-28T21:55:36Z	2023-07-28T23:02:49Z	2023-07-28T21:58:08Z	4	4	4\n"
                      "2023-07-28T22:21:09Z	2023-07-28T23:37:27Z	2023-07-28T22:21:49Z	5	5	5\n"
                      "2023-07-28T23:46:09Z	2023-07-29T00:55:15Z	2023-07-28T23:47:20Z	6	6	6")]
        self.__pack_zip__(test_data)

        execute_rscript(self.zip_file_path, self.r_script_path)

        with open("resources\\broker_result\\timeframe.csv", "r") as file:
            result = file.read()
            result_data = result.replace('\"', '').split("\n")
            # for each element in result_data split it by the delimiter ","
            result_data = [element.split(",") for element in result_data]
            # create pandas dataframe with results from R script
            results = pd.DataFrame(result_data[1:-1], columns=result_data[0])
            # check if the dataframe has the expected values
            expected_data = [
                ["date", "ed_count", "visit_mean", "los_mean", "los_reference", "los_difference", "change"],
                ["2023-W30", "1", "3", "69.41", "193.54", "-124.12", "Abnahme"]]

            expected = pd.DataFrame(expected_data[1:], columns=expected_data[0])
            self.assertEqual(results.to_string(), expected.to_string())



    def __pack_zip__(self, contents: list[str]):
        """
        This method creates a zip file from the given content-array and saves it in the resources folder in the way the broker
        request would structure it.
        :param contents:
        :return:
        """

        for i in range(len(contents)):
            #create a directory "unittest_result" with a subdirectory "i_result"
            i_result_path = "resources\\unittest_result\\"+str(i)+"_result"
            os.makedirs(i_result_path, exist_ok=True)

            #create a file "case_data.txt" with the content
            with open(i_result_path+"\\case_data.txt", "w") as file:
                file.write(contents[i])

            # convert 8_result to a zip file
            with zipfile.ZipFile(i_result_path+".zip", "w") as zip_file:
                for root, dirs, files in os.walk(i_result_path):
                    for file in files:
                        zip_file.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), i_result_path))

            # delete the directory "8_result"
            shutil.rmtree(i_result_path)

        # convert unittest_result to a zip file
        with zipfile.ZipFile("resources\\unittest_result.zip", "w") as zip_file:
            for root, dirs, files in os.walk("resources\\unittest_result"):
                for file in files:
                    zip_file.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), "resources\\unittest_result"))

        # delete the directory "unittest_result"
        shutil.rmtree("resources\\unittest_result")
