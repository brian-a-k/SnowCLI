import json
from pathlib import Path

from yaml import safe_load


def get_catalog_string(file_path: str) -> str:
    """
    Testing Utility function to mock object schemas/catalogs as JSON strings.
    :param file_path: test file path.
    :return: JSON string
    """
    with open(file_path, 'r') as cat_file:
        return json.dumps(safe_load(cat_file))


def get_resource_path(file_name: str = '') -> str:
    """
    :param file_name:
    :return:
    """
    if str(Path.cwd()).endswith('tests'):
        return str(Path.cwd().joinpath(f'test_resources/{file_name}'))
    else:
        return str(Path.cwd().joinpath(f'tests/test_resources/{file_name}'))
