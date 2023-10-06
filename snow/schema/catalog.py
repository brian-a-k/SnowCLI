from pathlib import Path
from typing import Union, Dict, List, Tuple, Optional

from snowflake.snowpark.types import StructType, StructField, StringType, TimestampType, DataType
from yaml import safe_load

from .models import ObjectDefinition
from .snow_types import SnowTypeMap
from pydantic.error_wrappers import ValidationError


class SnowCatalog:
    """
    Class for representing a target Snowflake tables schema definition.
    """

    def __init__(self, catalog_data: Union[Dict, str]) -> None:
        """
        :param catalog_data: Dict or string (file path string or JSON string).
        """
        try:
            self.catalog = self._load(catalog_data)
        except ValidationError:
            singer_cat = self._load_singer_catalog(catalog_data)
            self.catalog = self._load(singer_cat)

        self._type_map = SnowTypeMap()

        # audit columns snowpark schema
        self._audit_schema = StructType([
            StructField(column_identifier='CREATED_BY', datatype=StringType(), nullable=False),
            StructField(column_identifier='CREATED_TIMESTAMP', datatype=TimestampType(), nullable=False),
            StructField(column_identifier='MODIFIED_BY', datatype=StringType(), nullable=False),
            StructField(column_identifier='MODIFIED_TIMESTAMP', datatype=TimestampType(), nullable=False)
        ])

        # audit column default values
        self._audit_defaults = {
            'CREATED_BY': 'CURRENT_USER',
            'CREATED_TIMESTAMP': 'CURRENT_TIMESTAMP::TIMESTAMP_NTZ',
            'MODIFIED_BY': 'CURRENT_USER',
            'MODIFIED_TIMESTAMP': 'CURRENT_TIMESTAMP::TIMESTAMP_NTZ'
        }

        # audit fields for STAGING TABLES
        self.stage_audit_fields = ['CREATED_BY', 'CREATED_TIMESTAMP']

        # audit fields for base/target TABLES
        self.base_audit_fields = ['MODIFIED_BY', 'MODIFIED_TIMESTAMP']

    def snowpark_schema(self, upper_case: bool = False) -> StructType:
        """
        :param upper_case: Boolean flag to UPPERCASE all column names, default False.
        :return: Snowpark StructType based on the passed schema definition.
        """
        _fields = []
        for column in self.catalog.columns:
            # uppercase column name if flag is passed
            _col_identifier = column.source_column_name.upper() if upper_case else column.source_column_name

            _field = StructField(
                column_identifier=_col_identifier,
                datatype=self._type_map.get(column.destination_datatype),
                nullable=column.is_nullable
            )
            _fields.append(_field)

        return StructType(fields=_fields)

    @property
    def column_type_map(self) -> Dict[str, DataType]:
        """
        :return: Column name data-type mapping.
        """
        return {field.name: field.datatype for field in self.snowpark_schema(upper_case=True).fields}

    @property
    def snowflake_schema(self) -> List[Tuple[str, str, bool]]:
        """
        :return: List of tuples: (column name: str, Snowflake data type name: str, nullable: bool)
        """
        return [(f.name, self._type_map.convert(f.datatype), f.nullable) for f in self.snowpark_schema().fields]

    @property
    def audit_schema(self) -> List[Tuple]:
        """
        Audit columns for any Snowflake table created with Snow (for compatibility with SnowBall pipelines).
        :return: List of tuples: (column name: str, Snowflake data type name: str, nullable: bool, default value))
        """
        audit_fields = []
        for audit_field in self._audit_schema.fields:
            audit_default_val = self._audit_defaults.get(audit_field.name)

            audit_fields.append((
                audit_field.name,
                self._type_map.convert(audit_field.datatype),
                audit_field.nullable,
                audit_default_val
            ))

        return audit_fields

    @property
    def primary_keys(self) -> List[str]:
        """
        :return: List of primary key(s) for the table based on passed object definition.
        """
        pks = self.catalog.source_object_metadata.primary_key
        if isinstance(pks, str):
            return [pks.upper()]

        return [col.upper() for col in pks]

    @property
    def column_tags(self) -> Dict[str, Optional[str]]:
        """
        :return: Dict of column names with any tags/data classifications.
        """
        tags = dict()
        for column in self.catalog.columns:
            # data classification tag names
            if column.data_classification is not None:
                if column.data_classification.upper().endswith('_TAG'):
                    tags[column.source_column_name.upper()] = column.data_classification.upper()
                else:
                    tags[column.source_column_name.upper()] = f'{column.data_classification.upper()}_TAG'
            # PII tag
            elif column.is_pii:
                tags[column.source_column_name.upper()] = 'PII_TAG'
            # no PII tag or data classification
            else:
                tags[column.source_column_name.upper()] = None

        return tags

    @staticmethod
    def _load(val: Union[Dict, str]) -> ObjectDefinition:
        """
        :param val: Dict or string (file path string or JSON string).
        :return: ObjectDefinition base model object.
        """
        if isinstance(val, dict):
            return ObjectDefinition.parse_obj(val)

        try:
            if Path(val).is_file():
                return ObjectDefinition.parse_file(val)
        except OSError:
            return ObjectDefinition.parse_obj(safe_load(val))

    @staticmethod
    def _load_singer_catalog(val: Union[Dict, str]) -> Dict:
        """
        :param val: Dict or string (file path string or JSON string).
        :return: Singer catalog parsed into the ObjectDefinition model structure.
        """
        if isinstance(val, dict):
            singer_catalog = val

        try:
            if Path(val).is_file():
                with open(val, mode='r') as cat_file:
                    singer_catalog = safe_load(cat_file)
        except OSError:
            singer_catalog = safe_load(val)

        # splitting into object and column metadata
        object_metadata = singer_catalog.get('streams').pop()
        column_metadata = object_metadata.get('metadata')

        object_meta = dict()
        columns_meta = []

        for item in column_metadata:
            if item.get('breadcrumb'):
                column = {
                    'source_column_name': item.get('breadcrumb')[1],
                    'destination_datatype': item.get('metadata').get('sql-datatype'),
                    'is_nullable': True,
                    'data_classification': item.get('metadata').get('data_classification')
                }

                # checking that type does not include null, not contained in the metadata section of the singer dict
                prop_type = object_metadata.get('schema').get('properties').get(item.get('breadcrumb')[1]).get('type')
                if "null" not in prop_type:
                    column.update({'is_nullable': False})

                columns_meta.append(column)
            else:
                object_meta.update({
                    'primary_key': item.get('metadata').get('table-key-properties'),
                    'incremental_key': item.get('metadata').get('replication-key'),
                    'source_schema': item.get('metadata').get('schema-name'),
                    'custom_query': item.get('metadata').get('custom_query'),
                    'source_object_name': item.get('metadata').get('destination-table-name'),
                    'source_system_name': item.get('metadata').get('system-name')
                })

        return {'source_object_metadata': object_meta, 'columns': columns_meta}
