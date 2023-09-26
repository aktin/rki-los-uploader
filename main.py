import requests
import re
import zipfile

headers = {'Authorization': 'Bearer xxxAdmin1234'}
zip_name = 'export_data_cache.zip'


def request_ids_from_broker():
    url = "https://aktin-test-broker.klinikum.rwth-aachen.de/broker/request"
    response = requests.request("GET", url, headers=headers)

    return response.text


def extract_request_ids(raw_data: str):
    ids = [int(match.split('<request id="')[0]) for match in re.findall(r'<request id="(\d+)">', raw_data)]
    return ids


def create_zip_from_id(_id: int, dest_url):
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
    # open export zip file
    with zipfile.ZipFile(f"{zip_dir}/{zip_name}") as zf:
        for filename in zf.namelist():
            match = re.findall(r'(\d+)_result.zip', filename)

            # if zip contains a result sub zip directory...
            if len(match) > 0:
                zf.extract(filename, zip_dir)

                # ... extract the case_data.txt from it
                with zipfile.ZipFile(f"{zip_dir}{filename}") as result_dir:
                    # Todo handle keyerror when no case data exists
                    result_dir.extract('case_data.txt', zip_dir)


# gets case data from export with the highest id value
def get_latest_case_data():
    _id_list_raw = request_ids_from_broker()
    _id_list = extract_request_ids(_id_list_raw)
    _id = max(_id_list)

    zip_dir, zip_name = create_zip_from_id(_id=_id, dest_url=f'cache/exports/')
    get_case_data_from_zip(zip_dir, zip_name)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    get_latest_case_data()


# class TestClass(unittest.TestCase):
#     def test_get_request_ids(self):
#         raw = request_from_broker("https://aktin-test-broker.klinikum.rwth-aachen.de/broker/request")
#         id_list = get_request_ids(raw)
#         self.assertEqual(len(id_list), 532)
#         self.assertEqual(id_list[0], 590)
