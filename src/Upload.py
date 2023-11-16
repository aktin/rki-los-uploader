from libraries import sftp_export
from cryptography.fernet import Fernet


def generate_key():
    with open('rki.key', 'wb') as key:
        key.write(Fernet.generate_key())


def upload_to_sftp(file_path):
    fm = sftp_export.SftpFileManager()
    try:
        # encrypted_File = fm.__encrypt_file(file_path)
        fm.upload_file(encrypted_File)
    finally:
        print("File Uploaded!")
