# -*- coding: utf-8 -*-
"""
@AUTHOR: Alexander Kombeiz (akombeiz@ukaachen.de)
@VERSION=1.0
"""

import os
import shutil
import tempfile
import unittest
from pathlib import Path

import docker

from src.los_script import SftpFileManager

DOCKER_IMAGE = 'ubuntu:20.04'
USER_NAME = 'sftpuser'
USER_PASSWORD = 'sftppassword'
USER_GROUP = 'sftp'
SFTP_DIRNAME = 'test'
PORT = 2222


class TestSftpFileManager(unittest.TestCase):

  @classmethod
  def setup_container(cls):
    cls.container = cls.docker_client.containers.run(
        DOCKER_IMAGE,
        detach=True,
        command='sleep infinity',  # Keep container running
        ports={'22/tcp': PORT}
    )

  @classmethod
  def setup_ssh(cls):
    cls._exec_command('apt-get update')
    cls._exec_command('apt-get install -y openssh-server ssh')

  @classmethod
  def setup_user(cls):
    cls._exec_command(f'addgroup {USER_GROUP}')
    cls._exec_command(f'useradd -m {USER_NAME} -g {USER_GROUP}')
    cls._exec_command(f'sh -c "echo \'{USER_NAME}:{USER_PASSWORD}\' | chpasswd"')

  @classmethod
  def setup_sftp_directory(cls):
    cls._exec_command('mkdir -p /var/sftp/' + SFTP_DIRNAME)
    cls._exec_command('chown root:root /var/sftp')
    cls._exec_command('chmod 755 /var/sftp')
    cls._exec_command(f'chown {USER_NAME}:{USER_GROUP} /var/sftp/' + SFTP_DIRNAME)

  @classmethod
  def configure_sshd(cls):
    config = f"""
Port 22
Match User {USER_NAME}
ForceCommand internal-sftp
PasswordAuthentication yes
ChrootDirectory /var/sftp
PermitTunnel no
AllowAgentForwarding no
AllowTcpForwarding no
X11Forwarding no
"""
    config_path = '/etc/ssh/sshd_config'
    cls._exec_command(f'sh -c "echo \'{config}\' >> {config_path}"')
    cls._exec_command('service ssh restart')

  @classmethod
  def _exec_command(cls, command) -> str:
    exit_code, output = cls.container.exec_run(command, demux=True)
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
      cls.setup_ssh()
      cls.setup_user()
      cls.setup_sftp_directory()
      cls.configure_sshd()
      os.environ['SFTP.HOST'] = '127.0.0.1'
      os.environ['SFTP.PORT'] = str(PORT)
      os.environ['SFTP.USERNAME'] = USER_NAME
      os.environ['SFTP.PASSWORD'] = USER_PASSWORD
      os.environ['SFTP.TIMEOUT'] = '30'
      os.environ['SFTP.FOLDERNAME'] = SFTP_DIRNAME
      cls.sftp_manager = SftpFileManager()
      cls.temp_dir = tempfile.mkdtemp()
    except Exception:
      cls.tearDownClass()
      raise

  @classmethod
  def tearDownClass(cls):
    print(cls.container.logs().decode())
    shutil.rmtree(cls.temp_dir)
    if hasattr(cls, 'container'):
      cls.container.remove(force=True)

  def test_upload_file(self):
    test_file = Path(self.temp_dir) / 'test_upload.txt'
    test_content = 'test content'
    test_file.write_text(test_content)
    self.sftp_manager.upload_file(str(test_file))
    files = self.sftp_manager.list_files()
    self.assertIn('test_upload.txt', files)

  def test_upload_file_overwrite(self):
    test_file = Path(self.temp_dir) / 'test_overwrite.txt'
    test_file.write_text('initial content')
    self.sftp_manager.upload_file(str(test_file))
    # Modify and upload again
    test_file.write_text('new content')
    self.sftp_manager.upload_file(str(test_file))
    # Verify only one file exists
    files = self.sftp_manager.list_files()
    self.assertEqual(files.count('test_overwrite.txt'), 1)

  def test_list_files(self):
    test_files = ['test1.txt', 'test2.txt', 'test3.txt']
    for filename in test_files:
      test_file = Path(self.temp_dir) / filename
      test_file.write_text('content')
      self.sftp_manager.upload_file(str(test_file))
    files = self.sftp_manager.list_files()
    for filename in test_files:
      self.assertIn(filename, files)

  def test_delete_file(self):
    test_file = Path(self.temp_dir) / 'test_delete.txt'
    test_file.write_text('content')
    self.sftp_manager.upload_file(str(test_file))
    self.sftp_manager.delete_file('test_delete.txt')
    files = self.sftp_manager.list_files()
    self.assertNotIn('test_delete.txt', files)

  def test_delete_nonexistent_file(self):
    # Should not raise an exception
    self.sftp_manager.delete_file('nonexistent.txt')


if __name__ == '__main__':
  unittest.main()
