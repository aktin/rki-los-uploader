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

# TODO encapsulate in another class
# TODO Static variables in seperate config file (toml)
# See https://github.com/aktin/broker-sftp-uploader/blob/main/test/integration/docker/python/resources/settings.toml
headers = {'Authorization': 'Bearer xxxAdmin1234'}
zip_name = 'export_data_cache.zip'


# requests the id of the most recent export on the testbroker
def request_highest_id_by_tag_from_broker(tag='pandemieradar') -> int:
    url = "https://aktin-test-broker.klinikum.rwth-aachen.de/broker/request/filtered/"
    url = '?'.join([url, urllib.parse.urlencode(
        {'type': 'application/vnd.aktin.query.request+xml', 'predicate': "//tag='%s'" % tag})])
    response = requests.get(url, headers=headers)
    response.raise_for_status()

# TODO merge the two iterations to one
    # TODO return type should be int
    list_request_id = [element.get('id') for element in et.fromstring(response.content)]
    if len(list_request_id) < 1:
        raise Exception(f"no element with tag:\"{tag}\" was found!")
    return max(list_request_id)

# TODO store downloaded files in "tmp" directory, remove this method
# This function deletes the content of a given directory. It is used while requesting an export, so older export data
# won't be mixed up with the newer incoming ones
def delete_contents_of_dir(_dir):
    try:
        # Use shutil.rmtree to remove all files and subdirectories within the directory
        shutil.rmtree(_dir)
        # Recreate the directory if needed
        os.mkdir(_dir)
        print(f"Contents of '{_dir}' have been deleted.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Todo return absolute zip path
# creates a zip compressed folder out of the export data and saves it to a hard coded directory. When the path to the
# directory is changed, you must change the directory used with the "delete_contents_of_dir" function as well
def download_broker_export_as_zip(_id: int, dest_url:str) -> str:
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

    return zip_name


# Extracts the "case_data" files from the zip file we created in "create_zip_from_id" and store it in the same
# directory. If there is more than one "case_data" files, they are stored separately and grouped in the "Analyzer" class
def get_case_data_from_zip(zip_dir, zip_name):
    # open export zip file and extract all result sets from each hospital to seperated zip archives
    with zipfile.ZipFile(f"{zip_dir}/{zip_name}") as zf:
        # TODO namelist nach results filtern
        for filename in zf.namelist():
            match = re.findall(r'(\d+)_result.zip', filename)

            # if zip contains a result sub zip directory...
            if match:
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

    zip_dir = f'cache/exports/'
    zip_name2 = download_broker_export_as_zip(_id=_id, dest_url=zip_dir)
    get_case_data_from_zip(zip_dir, zip_name2)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    get_latest_case_data()
