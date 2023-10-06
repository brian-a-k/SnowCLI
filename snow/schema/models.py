from typing import Dict, Optional, List, Union

from pydantic import BaseModel, validator
from pydantic_yaml import YamlModel


def quote_column_name(column_val: Union[str, List[str], None]) -> Union[str, List[str], None]:
    """
    Double quotes column names if any spaces and/or special characters are found.
    https://docs.snowflake.com/en/sql-reference/identifiers-syntax#double-quoted-identifiers

    Example:
    - source_column_name: F2 (EXPORT ONLY)
    - Snowflake Column: "F2 (EXPORT ONLY)"
    """
    # all punctuation & special characters that need to be quoted for a snowflake column name
    special_characters = (
        ' ', '(', ')', '[', ']', '{', '}',
        '.', "'", '!', ':', ';', ',', '?',
        '@', '#', '$', '%', '^', '&', '*',
        '/', '|', '-', '+', '=', '<', '>'
    )

    # list of columns (used for catalog primary keys)
    if isinstance(column_val, list):
        quoted_columns = []

        for column_name in column_val:
            if any([char in special_characters for char in column_name]):
                quoted_columns.append(f'"{column_name}"')
            else:
                quoted_columns.append(column_name)

        return quoted_columns

    # single column string
    elif isinstance(column_val, str):
        if any([char in special_characters for char in column_val]):
            return f'"{column_val}"'
        else:
            return column_val
    else:
        return column_val


class CustomQuery(BaseModel):
    query: str
    parameters: Optional[Dict] = None


class ObjectMetadata(BaseModel):
    source_object_name: Optional[str] = None
    source_system_name: Optional[str] = None
    source_schema: Optional[str] = None
    additional_filters: Optional[List] = None
    custom_query: Optional[CustomQuery] = None
    destination_object_name: Optional[str] = None
    primary_key: Union[str, List[str]]
    incremental_key: Optional[str] = None

    # validator for `primary_key` and `incremental_key`
    _quote_primary_key = validator('primary_key', allow_reuse=True, pre=True, always=True)(quote_column_name)
    _quote_incremental_key = validator('incremental_key', allow_reuse=True, pre=True, always=True)(quote_column_name)


class ColumnMetadata(BaseModel):
    source_column_name: str
    destination_datatype: str
    is_nullable: bool = True
    data_classification: Optional[str] = None
    is_pii: bool = False

    # validator for `source_column_name`
    _quote_source_column_name = validator(
        'source_column_name', allow_reuse=True, pre=True, always=True)(quote_column_name)


class ObjectDefinition(YamlModel):
    source_object_metadata: ObjectMetadata
    columns: List[ColumnMetadata]
