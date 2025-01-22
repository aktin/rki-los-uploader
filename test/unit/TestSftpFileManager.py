# -*- coding: utf-8 -*-
"""
@AUTHOR: Alexander Kombeiz (akombeiz@ukaachen.de)
"""

import os

import docker
import pytest

from src.los_script import SftpFileManager

DOCKER_IMAGE = 'ubuntu:20.04'
USER_NAME = 'sftpuser'
USER_PASSWORD = 'sftppassword'
USER_GROUP = 'sftp'
SFTP_DIRNAME = 'test'
PORT = 2222


@pytest.fixture(scope="session")
def docker_setup(tmp_path_factory):
  temp_dir = tmp_path_factory.mktemp("sftp_test")
  docker_client = docker.from_env()
  container = docker_client.containers.run(
      DOCKER_IMAGE,
      detach=True,
      command='sleep infinity',
      ports={'22/tcp': PORT}
  )

  def exec_command(command) -> str:
    exit_code, output = container.exec_run(command, demux=True)
    if exit_code != 0:
      stdout, stderr = output if output else (None, None)
      error_msg = stderr or stdout or "No output"
      raise RuntimeError(f"Command '{command}' failed with exit code {exit_code}.\nOutput: {error_msg}")
    return output

  exec_command('apt-get update')
  exec_command('apt-get install -y openssh-server ssh')
  exec_command(f'addgroup {USER_GROUP}')
  exec_command(f'useradd -m {USER_NAME} -g {USER_GROUP}')
  exec_command(f'sh -c "echo \'{USER_NAME}:{USER_PASSWORD}\' | chpasswd"')
  exec_command(f'mkdir -p /var/sftp/{SFTP_DIRNAME}')
  exec_command('chown root:root /var/sftp')
  exec_command('chmod 755 /var/sftp')
  exec_command(f'chown {USER_NAME}:{USER_GROUP} /var/sftp/{SFTP_DIRNAME}')
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
  exec_command(f'sh -c "echo \'{config}\' >> /etc/ssh/sshd_config"')
  exec_command('service ssh restart')
  os.environ.update({
    'SFTP.HOST': '127.0.0.1',
    'SFTP.PORT': str(PORT),
    'SFTP.USERNAME': USER_NAME,
    'SFTP.PASSWORD': USER_PASSWORD,
    'SFTP.TIMEOUT': '30',
    'SFTP.FOLDERNAME': SFTP_DIRNAME
  })
  yield {'container': container, 'temp_dir': temp_dir}
  print(container.logs().decode())
  container.remove(force=True)


@pytest.fixture(scope="session")
def sftp_manager():
  return SftpFileManager()


def test_upload_file(docker_setup, sftp_manager):
  test_file = docker_setup['temp_dir'] / 'test_upload.txt'
  test_file.write_text('test content')
  sftp_manager.upload_file(test_file)
  files = sftp_manager.list_files()
  assert test_file.name in files


def test_upload_file_overwrite(docker_setup, sftp_manager):
  test_file = docker_setup['temp_dir'] / 'test_overwrite.txt'
  test_file.write_text('initial content')
  sftp_manager.upload_file(test_file)
  test_file.write_text('new content')
  sftp_manager.upload_file(test_file)
  files = sftp_manager.list_files()
  assert files.count(test_file.name) == 1


def test_upload_nonexistent_file(docker_setup, sftp_manager):
  missing_file = docker_setup['temp_dir'] / 'missing.txt'
  with pytest.raises(FileNotFoundError):
    sftp_manager.upload_file(missing_file)


def test_delete_file(docker_setup, sftp_manager):
  test_file = docker_setup['temp_dir'] / 'test_delete.txt'
  test_file.write_text('content')
  sftp_manager.upload_file(test_file)
  sftp_manager.delete_file(test_file.name)
  files = sftp_manager.list_files()
  assert test_file.name not in files


def test_delete_nonexistent_file(sftp_manager):
  """
    Ensure deleting a non-existent file does not raise an exception.
    """
  sftp_manager.delete_file('nonexistent.txt')
