# -*- coding: utf-8 -*-
"""
@AUTHOR: Alexander Kombeiz (akombeiz@ukaachen.de)
"""

import os
import time
from xml.dom.minidom import parseString
from xml.etree.ElementTree import Element, SubElement, tostring

import docker
import pytest
import requests

from src.los_script import BrokerRequestResultManager

DOCKER_IMAGE = 'ghcr.io/aktin/aktin-broker:latest'
AKTIN_BROKER_PORT = 'localhost:8080'
REQUESTS_TAG = 'test'
ADMIN_API_KEY = 'xxxAdmin1234'
API_KEY_1 = "xxxApiKey123"


@pytest.fixture(scope="session")
def docker_setup(tmp_path_factory):
  temp_dir = tmp_path_factory.mktemp("broker_test")
  docker_client = docker.from_env()
  container = docker_client.containers.run(
      DOCKER_IMAGE,
      detach=True,
      environment={
        AKTIN_BROKER_PORT: AKTIN_BROKER_PORT
      },
      ports={'8080/tcp': 8080}
  )
  time.sleep(5)
  os.environ.update({
    'BROKER.URL': f'http://{AKTIN_BROKER_PORT}',
    'BROKER.API_KEY': ADMIN_API_KEY,
    'REQUESTS.TAG': REQUESTS_TAG
  })
  yield {'container': container, 'temp_dir': temp_dir}
  print(container.logs().decode())
  container.remove(force=True)


@pytest.fixture(scope="session")
def broker_manager():
  return BrokerRequestResultManager()


def generate_xml_request(request_tag: str) -> str:
  """Generates an XML request based on the given tag."""
  query_request = Element('queryRequest', {'xmlns': 'http://aktin.org/ns/exchange', 'xmlns:xsi': 'http://www.w3.org/2001/XMLSchema-instance'})
  SubElement(query_request, 'id').text = '1337'
  SubElement(query_request, 'reference').text = '2020-01-01T00:00:00.000Z'
  SubElement(query_request, 'scheduled').text = '2020-01-01T12:00:00.000Z'
  query = SubElement(query_request, 'query')
  SubElement(query, 'title').text = 'Title of request'
  SubElement(query, 'description').text = 'Description of request'
  principal = SubElement(query, 'principal')
  SubElement(principal, 'name').text = 'Name of creator'
  SubElement(principal, 'organisation').text = 'Organisation of creator'
  SubElement(principal, 'email').text = 'Email of creator'
  tags = SubElement(principal, 'tags')
  SubElement(tags, 'tag').text = request_tag
  schedule = SubElement(query, 'schedule', {'xsi:type': 'repeatedExecution'})
  SubElement(schedule, 'duration').text = '-P6M'
  SubElement(schedule, 'id').text = '1'
  sql = SubElement(query, 'sql', {'xmlns': 'http://aktin.org/ns/i2b2/sql'})
  SubElement(sql, 'source', {'type': 'application/sql'}).text = 'SELECT * FROM fhir_observation'
  return parseString(tostring(query_request)).toprettyxml()


def submit_broker_request(xml_data: str) -> str:
  """Submits the broker request and extracts the created request ID and publishes the request to all clients."""
  url = f'http://{AKTIN_BROKER_PORT}/broker/request'
  headers = {'Authorization': f'Bearer {ADMIN_API_KEY}', 'Content-Type': 'application/vnd.aktin.query.request+xml'}
  response = requests.post(url, headers=headers, data=xml_data)
  response.raise_for_status()
  request_id = response.headers['Location'].split('/')[-1]
  url = f'http://{AKTIN_BROKER_PORT}/broker/request/{request_id}/publish'
  response = requests.post(url, headers=headers)
  response.raise_for_status()
  return request_id


def submit_dummy_result(api_key: str, request_id: str):
  """Submits a dummy file as a result for a broker request and updates the status to 'completed'."""
  url = f'http://{AKTIN_BROKER_PORT}/aggregator/my/request/{request_id}/result'
  headers = {'Authorization': f'Bearer {api_key}', 'Content-Type': 'text/csv'}
  response = requests.put(url, headers=headers, data="a;b\n1;2\n3;4\n")
  response.raise_for_status()
  url = f'http://{AKTIN_BROKER_PORT}/broker/my/request/{request_id}/status/completed'
  headers = {'Authorization': f'Bearer {api_key}'}
  response = requests.post(url, headers=headers)
  response.raise_for_status()


def test_download_latest_broker_result_multiple_requests(docker_setup, broker_manager):
  """Test that latest request is downloaded when multiple requests exist"""
  xml1 = generate_xml_request(REQUESTS_TAG)
  request_id1 = submit_broker_request(xml1)
  submit_dummy_result(API_KEY_1, request_id1)
  xml2 = generate_xml_request(REQUESTS_TAG)
  request_id2 = submit_broker_request(xml2)
  submit_dummy_result(API_KEY_1, request_id2)
  zip_path = broker_manager.download_latest_broker_result_by_set_tag(docker_setup['temp_dir'])
  assert zip_path.exists()
  assert zip_path.name == f'result{request_id2}.zip'


def test_download_latest_broker_result_wrong_tag(docker_setup):
  """Should not raise an exception but log a warning"""
  os.environ['REQUESTS.TAG'] = 'wrong_tag'
  broker_manager2 = BrokerRequestResultManager()
  with pytest.raises(SystemExit) as cm:
    broker_manager2.download_latest_broker_result_by_set_tag()
  assert cm.value.code == 0
