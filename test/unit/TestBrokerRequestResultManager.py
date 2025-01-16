# -*- coding: utf-8 -*-
"""
@AUTHOR: Alexander Kombeiz (akombeiz@ukaachen.de)
@VERSION=1.0
"""
import os
import unittest
from xml.dom.minidom import parseString
from xml.etree.ElementTree import Element, SubElement, tostring

import docker
import requests

from src.los_script import BrokerRequestResultManager

DOCKER_IMAGE = 'ghcr.io/aktin/aktin-broker:latest'
AKTIN_BROKER_PORT = 'localhost:8080'
REQUESTS_TAG = 'test'
ADMIN_API_KEY = 'xxxAdmin1234'
API_KEY_1 = "xxxApiKey123"


class TestBrokerRequestResultManager(unittest.TestCase):

  @classmethod
  def setup_container(cls):
    cls.container = cls.docker_client.containers.run(
        DOCKER_IMAGE,
        detach=True,
        environment={
          AKTIN_BROKER_PORT: AKTIN_BROKER_PORT
        },
        ports={'8080/tcp': 8080}
    )

  @classmethod
  def setup_broker(cls):
    cls._exec_command('apt-get update')
    cls._exec_command('apt-get install -y curl')

  @classmethod
  def _exec_command(cls, command) -> str:
    exit_code, output = cls.container.exec_run(command, user='root', demux=True)
    if exit_code != 0:
      stdout, stderr = output if output else (None, None)
      error_msg = stderr or stdout or "No output"
      raise RuntimeError(f"Command '{command}' failed with exit code {exit_code}.\nOutput: {error_msg}")
    return output

  @classmethod
  def setUpClass(cls):
    try:
      cls.docker_client = docker.from_env()
      cls.setup_container()
      cls.setup_broker()
      os.environ['BROKER.URL'] = f'http://{AKTIN_BROKER_PORT}'
      os.environ['BROKER.API_KEY'] = ADMIN_API_KEY
      os.environ['REQUESTS.TAG'] = REQUESTS_TAG
      cls.broker_manager = BrokerRequestResultManager()
    except Exception:
      cls.tearDownClass()
      raise

  @classmethod
  def tearDownClass(cls):
    print(cls.container.logs().decode())
    if hasattr(cls, 'container'):
      cls.container.remove(force=True)

  def __generate_xml_request(self, request_tag: str) -> str:
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

  def __submit_broker_request(self, xml_data: str) -> str:
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

  def __submit_dummy_result(self, api_key: str, request_id: str):
    """Submits a dummy file as a result for a broker request and updates the status to 'completed'."""
    url = f'http://{AKTIN_BROKER_PORT}/aggregator/my/request/{request_id}/result'
    headers = {'Authorization': f'Bearer {api_key}', 'Content-Type': 'text/csv'}
    response = requests.put(url, headers=headers, data="a;b\n1;2\n3;4\n")
    response.raise_for_status()
    url = f'http://{AKTIN_BROKER_PORT}/broker/my/request/{request_id}/status/completed'
    headers = {'Authorization': f'Bearer {api_key}'}
    response = requests.post(url, headers=headers)
    response.raise_for_status()

  def test_download_latest_broker_result_multiple_requests(self):
    """Test that latest request is downloaded when multiple requests exist"""
    xml1 = self.__generate_xml_request(REQUESTS_TAG)
    request_id1 = self.__submit_broker_request(xml1)
    self.__submit_dummy_result(API_KEY_1, request_id1)
    # Submit second request
    xml2 = self.__generate_xml_request(REQUESTS_TAG)
    request_id2 = self.__submit_broker_request(xml2)
    self.__submit_dummy_result(API_KEY_1, request_id2)
    zip_path = self.broker_manager.download_latest_broker_result_by_set_tag()
    # Verify we got the latest request (highest ID)
    expected_filename = f'result{request_id2}.zip'
    self.assertTrue(os.path.exists(zip_path))
    self.assertTrue(zip_path.endswith(expected_filename))
    os.remove(zip_path)

  def test_download_latest_broker_result_wrong_tag(self):
    # Should not raise an exception, only warning
    os.environ['REQUESTS.TAG'] = 'wrong_tag'
    broker_manager2 = BrokerRequestResultManager()
    with (self.assertRaises(SystemExit) as cm):
      broker_manager2.download_latest_broker_result_by_set_tag()
    self.assertEqual(cm.exception.code, 0)


if __name__ == '__main__':
  unittest.main()
