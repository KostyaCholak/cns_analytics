import logging
import os

import feather
import pandas as pd
import paramiko
import paramiko.util

from dotenv import load_dotenv

from cns_analytics import Symbol
from cns_analytics.entities import MDType
from paramiko.py3compat import decodebytes


paramiko.util.get_logger('paramiko').setLevel(logging.WARN)
load_dotenv('.env')

class Storage:
    local_folder = os.getenv("STORAGE_FOLDER", ".cache/")
    remote_folder = "/upload/cns_analytics/"
    _storage = None

    @classmethod
    def get(cls):
        if cls._storage is None or not cls._storage.ssh.get_transport().is_active():
            cls._storage = cls()
        return cls._storage

    def __init__(self):
        ssh = paramiko.SSHClient()

        if "STORAGE_HOST_KEY" in os.environ:
            key = paramiko.Ed25519Key(data=decodebytes(os.environ["STORAGE_HOST_KEY"].encode()))
            ssh.get_host_keys().add(os.environ["STORAGE_HOST_NAME"], 'ssh-ed25519', key)

        ssh.connect(
            hostname=os.environ['STORAGE_HOST'],
            username=os.environ['STORAGE_USER'],
            password=os.environ['STORAGE_PASSWORD'],
            port=int(os.environ['STORAGE_PORT'])
        )
        sftp = ssh.open_sftp()

        self.sftp = sftp
        self.ssh = ssh

    def _exists_remote(self, key):
        remote_path = os.path.join(self.remote_folder, key)
        try:
            self.sftp.stat(remote_path)
            return True
        except FileNotFoundError:
            return False

    def _exists_locally(self, key):
        local_path = os.path.join(self.local_folder, key)
        return os.path.exists(local_path)

    def _upload(self, key):
        local_path = os.path.join(self.local_folder, key)
        remote_path = os.path.join(self.remote_folder, key)
        folders, filename = os.path.split(key)
        base = self.remote_folder

        for folder in folders.split('/'):
            base = os.path.join(base, folder)
            try:
                self.sftp.mkdir(base)
            except OSError:
                continue

        self.sftp.put(local_path, remote_path)

    def _download(self, key):
        remote_path = os.path.join(self.remote_folder, key)
        local_path = os.path.join(self.local_folder, key)

        folders, filename = os.path.split(key)
        os.makedirs(os.path.join(folders), exist_ok=True)

        self.sftp.get(remote_path, local_path)

    def _deserialize(self, key):
        local_path = os.path.join(self.local_folder, key)
        df = feather.read_dataframe(local_path)
        if 'ts' in df.columns:
            df.rename(columns={
                'ts': 'time'
            }, inplace=True)
            df.set_index('time', inplace=True)
        df = df.sort_index()
        return df

    def _serialize(self, key, data: pd.DataFrame):
        local_path = os.path.join(self.local_folder, key)
        folders, filename = os.path.split(local_path)
        os.makedirs(os.path.join(folders), exist_ok=True)
        return feather.write_dataframe(data, local_path)

    @staticmethod
    def _get_key(symbol: Symbol, md_type: MDType) -> str:
        return f"{symbol.exchange.name}/{md_type.name}/{symbol.name}"

    @classmethod
    def load_data(cls, symbol: Symbol, md_type: MDType) -> pd.DataFrame:
        storage = cls.get()
        key = storage._get_key(symbol, md_type)

        if not storage._exists_locally(key):
            if not storage._exists_remote(key):
                raise KeyError(symbol.name)
            storage._download(key)

        return storage._deserialize(key)

    @classmethod
    def save_data(cls, symbol: Symbol, md_type: MDType, data: pd.DataFrame):
        storage = cls.get()
        key = storage._get_key(symbol, md_type)
        storage._serialize(key, data)
        storage._upload(key)
