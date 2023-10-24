import os
import shutil
import urllib

import subprocess
import requests
import re
import zipfile
import xml.etree.ElementTree as et
import logging

from libraries.sftp_export import BrokerRequestResultManager

headers = {'Authorization': 'Bearer xxxAdmin1234'}
zip_name = 'export_data_cache.zip'


def request_highest_id_by_tag_from_broker(tag='pandemieradar') -> list:
    url = "https://aktin-test-broker.klinikum.rwth-aachen.de/broker/request/filtered/"
    url = '?'.join([url, urllib.parse.urlencode({'type': 'application/vnd.aktin.query.request+xml', 'predicate': "//tag='%s'" % tag})])
    response = requests.get(url, headers=headers)

    list_request_id = [element.get('id') for element in et.fromstring(response.content)]
    if len(list_request_id) < 1:
        raise Exception(f"no element with tag:\"{tag}\" was found!")
    return max(list_request_id)


def extract_request_ids(raw_data: str):
    ids = [int(match.split('<request id="')[0]) for match in re.findall(r'<request id="(\d+)">', raw_data)]
    return ids

def delete_contents_of_dir(dir):
    try:
        # Use shutil.rmtree to remove all files and subdirectories within the directory
        shutil.rmtree(dir)
        # Recreate the directory if needed
        os.mkdir(dir)
        print(f"Contents of '{dir}' have been deleted.")
    except Exception as e:
        print(f"An error occurred: {e}")

def create_zip_from_id(_id: int, dest_url):
    delete_contents_of_dir(dest_url)
    # get resource identifier
    uuid_request_url = f"https://aktin-test-broker.klinikum.rwth-aachen.de/broker/export/request-bundle/{_id}"
    uuid = requests.request("POST", url=uuid_request_url, headers=headers).text

    # get resource with patient data from all clinics
    patient_data_url = f"https://aktin-test-broker.klinikum.rwth-aachen.de/broker/download/{uuid}"
    patient_data = requests.request("GET", url=patient_data_url, headers=headers).content

    # save data in a zip directory
    zip_dir = f'{dest_url}{zip_name}'
    with open(zip_dir, mode='wb') as f:
        f.write(patient_data)

    return dest_url, zip_name


def get_case_data_from_zip(zip_dir, zip_name):
    # open export zip file and extract all result sets from each hospital to seperated zip archives
    with zipfile.ZipFile(f"{zip_dir}/{zip_name}") as zf:
        for filename in zf.namelist():
            match = re.findall(r'(\d+)_result.zip', filename)

            # if zip contains a result sub zip directory...
            if len(match) > 0:
                zf.extract(filename, zip_dir)

                # ... extract the case_data.txt from it
                with zipfile.ZipFile(f"{zip_dir}{filename}") as result_dir:
                    hospital_num = filename.split("_")[0]
                    # Todo handle keyerror when no case data exists
                    result_dir.extract("case_data.txt", zip_dir)
                    os.rename(f"{zip_dir}/case_data.txt", f"{zip_dir}/{hospital_num}_case_data.txt")



# gets case data from export with the highest id value
def get_latest_case_data():
    _id = request_highest_id_by_tag_from_broker('LOS')  # TODO for working version delete 'LOS'

    zip_dir, zip_name = create_zip_from_id(_id=_id, dest_url=f'cache/exports/')
    get_case_data_from_zip(zip_dir, zip_name)


def execute_r_file():
    r_script_path = "C:\\Users\\whoy\\PycharmProjects\\pythonProject5\\libraries\\LOS_short.R"
    result = subprocess.run(["Rscript", r_script_path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    # Check the result
    if result.returncode == 0:
        print("R script executed successfully.")
        print("Output:")
        print(result.stdout)
    else:
        print("Error executing R script:")
        print("Exit code:", result.returncode)
        print("Error message:")
        print(result.stderr)





# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    get_latest_case_data()
    # execute_r_file()


# class TestClass(unittest.TestCase):
#     def test_get_request_ids(self):
#         raw = request_from_broker("https://aktin-test-broker.klinikum.rwth-aachen.de/broker/request")
#         id_list = get_request_ids(raw)
#         self.assertEqual(len(id_list), 532)
#         self.assertEqual(id_list[0], 590)
