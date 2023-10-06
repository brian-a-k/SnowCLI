import pytest
from snowflake.snowpark.types import (
    StructType,
    StructField,
    StringType,
    DecimalType
)

from snow.schema.catalog import SnowCatalog
from snow.schema.inspector import SchemaInspector
from snow.schema.snow_types import VarcharSizeType
from .utils import get_resource_path


class TestSchemaInspector:
    _test_catalog_path = get_resource_path('test_object.yml')

    test_columns = ('MOD_COL', 'MOD_COL_NON_NULL')
    test_catalog = SnowCatalog(_test_catalog_path)

    def test_get_new_fields(self):
        test_tbl_schema = StructType(
            [f for f in self.test_catalog.snowpark_schema(upper_case=True).fields if f.name not in self.test_columns])

        test_inspector = SchemaInspector(catalog=self.test_catalog, curr_table_schema=test_tbl_schema)
        test_new_fields = test_inspector.get_new_fields()

        assert isinstance(test_new_fields, StructType)
        assert all([isinstance(f, StructField) for f in test_new_fields.fields])
        assert len(test_new_fields.fields) == 2
        assert all([f.name in self.test_columns for f in test_new_fields.fields])

    def test_get_deleted_fields(self):
        del_def = SnowCatalog._load(self._test_catalog_path)
        del_cols = [c for c in del_def.columns if c.source_column_name.upper() not in self.test_columns]
        del_def.columns = del_cols

        del_catalog = SnowCatalog(del_def.dict())
        test_tbl_schema = self.test_catalog.snowpark_schema(upper_case=True)

        test_inspector = SchemaInspector(catalog=del_catalog, curr_table_schema=test_tbl_schema)
        test_del_fields = test_inspector.get_deleted_fields()

        assert isinstance(test_del_fields, StructType)
        assert all([isinstance(f, StructField) for f in test_del_fields.fields])
        assert len(test_del_fields.fields) == 2
        assert all([f.name in self.test_columns for f in test_del_fields.fields])

    def test_get_nullability_fields(self):
        test_tbl_schema = self.test_catalog.snowpark_schema(upper_case=True)

        for f in test_tbl_schema.fields:
            if f.name in self.test_columns:
                f.nullable = not f.nullable

        test_inspector = SchemaInspector(catalog=self.test_catalog, curr_table_schema=test_tbl_schema)
        test_null_changes = test_inspector.get_nullability_fields()

        assert isinstance(test_null_changes, StructType)
        assert all([isinstance(f, StructField) for f in test_null_changes.fields])
        assert len(test_null_changes.fields) == 2
        assert all([f.name in self.test_columns for f in test_null_changes.fields])

    def test_get_tagged_fields(self):
        test_catalog = SnowCatalog(self._test_catalog_path)
        test_tbl_schema = self.test_catalog.snowpark_schema(upper_case=True)
        test_inspector = SchemaInspector(catalog=test_catalog, curr_table_schema=test_tbl_schema)

        test_tagged_cols = [
            ('NAME_COL', 'PII_TAG', True),
            ('EMAIL_COL', 'PII_TAG', True),
            ('ACCOUNT_COL', 'PII_TAG', True),
            ('TAG_COLUMN_PII', 'PII_TAG', True)
        ]

        # adding tags test
        assert test_inspector.get_tagged_fields(table_tags=None) == test_tagged_cols

        # no tag change test
        assert test_inspector.get_tagged_fields(table_tags=test_catalog.column_tags) is None

        test_change_tag = {
            'NAME_COL': 'PII_TAG',
            'EMAIL_COL': 'PII_TAG',
            'ACCOUNT_COL': 'PII_TAG',
            'TAG_COLUMN_PII': 'TEST_TAG',
        }

        # changed tag test
        expected_change_tag = [('TAG_COLUMN_PII', 'TEST_TAG', False), ('TAG_COLUMN_PII', 'PII_TAG', True)]
        assert test_inspector.get_tagged_fields(table_tags=test_change_tag) == expected_change_tag

    def test_get_type_change_fields(self):
        test_inspector = SchemaInspector(
            catalog=self.test_catalog,
            curr_table_schema=StructType([StructField('', StringType())])
        )

        # no changes
        test_tbl_types = {'VARCHAR_LENGTH': VarcharSizeType(50), 'MOD_NUMBER_COL': DecimalType(38, 0)}
        assert test_inspector.get_type_change_fields(test_tbl_types) is None

        # increase varchar length
        test_tbl_types = {'VARCHAR_LENGTH': VarcharSizeType(10)}
        assert test_inspector.get_type_change_fields(test_tbl_types) == {'VARCHAR_LENGTH': VarcharSizeType(50)}

        # increase precision
        test_tbl_types = {'MOD_NUMBER_COL': DecimalType(16, 0)}
        assert test_inspector.get_type_change_fields(test_tbl_types) == {'MOD_NUMBER_COL': DecimalType(38, 0)}

        # decrease precision
        test_tbl_types = {'NUMBER_COL_1': DecimalType(6, 6)}
        assert test_inspector.get_type_change_fields(test_tbl_types) == {'NUMBER_COL_1': DecimalType(10, 6)}

        # decrease varchar length from max - error
        test_tbl_types = {'VARCHAR_LENGTH': VarcharSizeType()}
        with pytest.raises(Exception) as exp:
            _ = test_inspector.get_type_change_fields(test_tbl_types)
        assert exp.type == Exception
        assert str(exp.value) == 'Reducing VARCHAR length is not supported - Column: VARCHAR_LENGTH'

        # decrease varchar length with StringType() - error
        test_tbl_types = {'VARCHAR_LENGTH': StringType()}
        with pytest.raises(Exception) as exp:
            _ = test_inspector.get_type_change_fields(test_tbl_types)
        assert exp.type == Exception
        assert str(exp.value) == 'Reducing VARCHAR length is not supported - Column: VARCHAR_LENGTH'

        # decrease varchar length from higher length - error
        test_tbl_types = {'VARCHAR_LENGTH': VarcharSizeType(100)}
        with pytest.raises(Exception) as exp:
            _ = test_inspector.get_type_change_fields(test_tbl_types)
        assert exp.type == Exception
        assert str(exp.value) == 'Reducing VARCHAR length is not supported - Column: VARCHAR_LENGTH'

        # change numeric scale - error
        test_tbl_types = {'MOD_NUMBER_COL': DecimalType(38, 2)}
        with pytest.raises(Exception) as exp:
            _ = test_inspector.get_type_change_fields(test_tbl_types)
        assert exp.type == Exception
        assert str(exp.value) == 'Changing numeric scale is not supported - Column: MOD_NUMBER_COL'

        # change data type - error
        test_tbl_types = {'MOD_NUMBER_COL': StringType()}
        with pytest.raises(Exception) as exp:
            _ = test_inspector.get_type_change_fields(test_tbl_types)
        assert exp.type == Exception
        assert str(exp.value) == (
            'Changing data type StringType() to DecimalType(38, 0) is not supported - Column MOD_NUMBER_COL'
        )
