import base64
import json
from typing import Union, Optional

import boto3
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from snowflake.snowpark import Session

from .config.manager import SnowProfile


class SnowClient:
    def __init__(self, snow_profile: SnowProfile):
        self.profile = snow_profile

        self._snowflake_params = {
            'account': self.profile.account,
            'user': self.profile.user,
            'role': self.profile.role,
            'warehouse': self.profile.warehouse,
            'database': self.profile.database,
            'schema': self.profile.sf_schema
        }

        # Remote profile with RSA creds stored in AWS secrets manager
        if self.profile.private_key_secret and self.profile.passphrase_secret:
            self._aws_session = boto3.session.Session(profile_name=self.profile.aws_named_profile)
            self._snowflake_params.update({'private_key': self._get_remote_key()})

        # Local profile with local RSA pem file
        elif self.profile.private_key_path and self.profile.private_key_passphrase:
            self._aws_session = None
            self._snowflake_params.update({'private_key': self._get_local_key()})
        else:
            raise Exception('Snow config file is malformed')

        self._snowpark_session = Session.builder.configs(self._snowflake_params).create()

    @property
    def session(self) -> Session:
        """
        :return: Snowpark session object.
        """
        return self._snowpark_session

    @property
    def s3_client(self) -> boto3.client:
        """
        :return: A boto3 S3 client object.
        """
        if self._aws_session:
            return self._aws_session.client(service_name='s3')
        return None

    @property
    def secrets_manger_client(self) -> boto3.client:
        """
        :return: A boto3 secrets manager client object.
        """
        if self._aws_session:
            return self._aws_session.client(service_name='secretsmanager')
        return None

    def _get_local_key(self) -> Optional[bytes]:
        with open(self.profile.private_key_path, mode='rb') as pem_file:
            p_key = serialization.load_pem_private_key(
                data=pem_file.read(),
                password=self.profile.private_key_passphrase.encode(),
                backend=default_backend()
            )

            pk_bytes = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
        return pk_bytes

    def _get_remote_key(self):
        pem = self._get_secret(self.profile.private_key_secret)
        pw = self._get_secret(self.profile.passphrase_secret)

        if isinstance(pw, str):
            try:
                passphrase = json.loads(pw).get('passphrase').encode()
            except json.JSONDecodeError:
                passphrase = pw.encode()
        else:
            passphrase = pw

        p_key = serialization.load_pem_private_key(
            data=pem.encode(),
            password=passphrase,
            backend=default_backend()
        )

        pk_bytes = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        return pk_bytes

    def _get_secret(self, secret_name: str) -> Union[str, bytes]:
        secret_response = self.secrets_manger_client.get_secret_value(SecretId=secret_name)
        if 'SecretString' in secret_response:
            return secret_response.get('SecretString')
        else:
            return base64.b64decode(secret_response.get('SecretBinary'))
