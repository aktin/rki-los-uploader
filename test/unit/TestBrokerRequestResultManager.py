# -*- coding: utf-8 -*-
"""
@AUTHOR: Alexander Kombeiz (akombeiz@ukaachen.de)
@VERSION=1.0
"""

import os
import unittest
from xml.dom.minidom import parseString
from xml.etree import ElementTree as ET
from xml.etree.ElementTree import Element, SubElement, tostring

import docker
import requests

from src.los_script import BrokerRequestResultManager

DOCKER_IMAGE = 'ghcr.io/aktin/aktin-broker:latest'
AKTIN_BROKER_PORT = 'localhost:8080'
REQUESTS_TAG = 'test'
ADMIN_API_KEY = 'xxxAdmin1234'
API_KEY_1 = "xxxApiKey123"
API_KEY_2 = "xxxApiKey567"


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

######################
  def fetch_connected_nodes(self) -> int:
    url = f'http://{AKTIN_BROKER_PORT}/broker/node'
    headers = {'Authorization': f'Bearer {ADMIN_API_KEY}'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()  # Raise an error for HTTP codes >= 400
    root = ET.fromstring(response.text)
    id_count = len(root.findall(".//id"))
    return id_count

######################
  def generate_xml_request(self, request_tag) -> str:
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

######################
  def submit_broker_request(self, xml_data) -> str:
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

######################
  def test_broker_request_creation(self):
    xml_data = self.generate_xml_request(REQUESTS_TAG)
    print(f"Generated XML: {xml_data}")
    request_id = self.submit_broker_request(xml_data)
    print(f"Request ID: {request_id}")
    print(f"Request {request_id} published successfully.")

  def test_upload_file(self):
    connected_node_count = self.fetch_connected_nodes()
    print(f"Number of connected client nodes: {connected_node_count}")
    self.assertGreaterEqual(connected_node_count, 0, "There should be no negative node count.")

  if __name__ == '__main__':
    unittest.main()
