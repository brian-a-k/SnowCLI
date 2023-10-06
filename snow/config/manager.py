import os
from yaml import safe_load
from pathlib import Path
from typing import Optional, Dict

import click
from pydantic import BaseModel, Field, validator
from pydantic.error_wrappers import ValidationError
from pydantic_yaml import YamlModel

from .settings import DEFAULT_NAME, CONFIG_FILE_NAME


def _parse_raw_config(raw_val: str) -> dict:
    try:
        if Path(raw_val).is_file():
            with open(raw_val, 'r') as conf_file:
                return safe_load(conf_file)
    except OSError:
        return safe_load(raw_val)
    else:
        return safe_load(raw_val)


class SnowProfile(BaseModel):
    # Snowflake connection params
    user: str
    account: str
    warehouse: str
    role: Optional[str] = None
    database: str

    # the "schema" keyword conflicts with a pydantic.BaseModel method (using alias "schema")
    sf_schema: str = Field(alias='schema')

    # aws secrets manager keys
    private_key_secret: Optional[str] = None
    passphrase_secret: Optional[str] = None

    # local rsa key file & passphrase
    private_key_path: Optional[str] = None
    private_key_passphrase: Optional[str] = None

    # aws profile name
    aws_named_profile: Optional[str] = None

    @validator('role')
    def get_role(cls, role_val: Optional[str]) -> Optional[str]:
        if role_val == DEFAULT_NAME or role_val is None:
            return None
        else:
            return role_val


class SnowConfig(YamlModel):
    profiles: Dict[str, SnowProfile] = dict()

    def update(self, name: str, pro: SnowProfile) -> None:
        self.profiles.update({name: pro})

    def get(self, name: str) -> SnowProfile:
        return self.profiles.get(name)


class ConfigManager:
    def __init__(self, profile_name: str, base_path: str):
        self._profile_name = profile_name
        self._base_path = base_path
        self._snow_config_file = os.path.join(self._base_path, CONFIG_FILE_NAME)
        self._config = self._load()

    @property
    def config(self) -> Optional[SnowConfig]:
        return self._config

    def _load(self) -> Optional[SnowConfig]:
        if Path(self._snow_config_file).is_file():
            if Path(self._snow_config_file).stat().st_size == 0:
                return None  # config file exists but is empty
            else:
                try:
                    return SnowConfig.parse_file(self._snow_config_file)
                except ValidationError as e:
                    click.echo(f'Snow config file is malformed: {e}')
        else:
            Path(self._base_path).mkdir(parents=True, exist_ok=True)
            with open(self._snow_config_file, mode='w') as _:
                pass

    def write(self, **kwargs) -> None:
        new_profile = SnowProfile(**kwargs)
        if not self._config:
            self._config = SnowConfig(profiles={self._profile_name: new_profile})
        else:
            self._config.update(self._profile_name, new_profile)

        with open(self._snow_config_file, mode='w') as np_file:
            np_file.write(self._config.yaml(by_alias=True, exclude_none=True))

    def read(self) -> Optional[SnowProfile]:
        if self._config and self._profile_name in self._config.profiles.keys():
            return self._config.get(self._profile_name)
        else:
            click.echo(f'Profile [{self._profile_name}] not found in {self._snow_config_file}')
            return None
