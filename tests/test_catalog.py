from snowflake.snowpark.types import StructType, StructField, DataType

from snow.schema.catalog import SnowCatalog
from snow.schema.models import ObjectDefinition
from .utils import get_catalog_string, get_resource_path


class TestSnowCatalog:
    _test_catalog_json = get_resource_path('test_object.json')
    _test_catalog_yml = get_resource_path('test_object.yml')
    _test_catalog_singer = get_resource_path('test_singer.json')

    def test_snowpark_schema_files(self):
        json_catalog = SnowCatalog(self._test_catalog_json)
        yml_catalog = SnowCatalog(self._test_catalog_yml)
        singer_catalog = SnowCatalog(self._test_catalog_singer)

        assert isinstance(json_catalog.snowpark_schema(), StructType)
        assert isinstance(yml_catalog.snowpark_schema(), StructType)
        assert isinstance(singer_catalog.snowpark_schema(), StructType)
        assert json_catalog.snowpark_schema() == yml_catalog.snowpark_schema() == singer_catalog.snowpark_schema()

        assert all([isinstance(f, StructField) for f in json_catalog.snowpark_schema().fields])
        assert all([isinstance(f, StructField) for f in yml_catalog.snowpark_schema().fields])
        assert all([isinstance(f, StructField) for f in singer_catalog.snowpark_schema().fields])

        zip_cats = zip(
            json_catalog.snowpark_schema().fields,
            yml_catalog.snowpark_schema().fields,
            singer_catalog.snowpark_schema().fields
        )

        for f1, f2, f3 in zip_cats:
            assert f1 == f2 == f3

    def test_snowpark_schema_strings(self):
        json_catalog_str = SnowCatalog(get_catalog_string(self._test_catalog_json))
        yml_catalog_str = SnowCatalog(get_catalog_string(self._test_catalog_yml))
        singer_catalog_str = SnowCatalog(get_catalog_string(self._test_catalog_singer))

        assert isinstance(json_catalog_str.snowpark_schema(), StructType)
        assert isinstance(yml_catalog_str.snowpark_schema(), StructType)
        assert isinstance(singer_catalog_str.snowpark_schema(), StructType)
        assert json_catalog_str.snowpark_schema() == yml_catalog_str.snowpark_schema() == \
               singer_catalog_str.snowpark_schema()

        assert all([isinstance(f, StructField) for f in json_catalog_str.snowpark_schema().fields])
        assert all([isinstance(f, StructField) for f in yml_catalog_str.snowpark_schema().fields])
        assert all([isinstance(f, StructField) for f in singer_catalog_str.snowpark_schema().fields])

        zip_cats = zip(
            json_catalog_str.snowpark_schema().fields,
            yml_catalog_str.snowpark_schema().fields,
            singer_catalog_str.snowpark_schema().fields
        )

        for f1, f2, f3 in zip_cats:
            assert f1 == f2 == f3

    def test_snowpark_schema_upper_case(self):
        json_catalog = SnowCatalog(self._test_catalog_json)
        yml_catalog = SnowCatalog(self._test_catalog_yml)
        singer_catalog = SnowCatalog(self._test_catalog_singer)

        assert all([f.name.isupper() for f in json_catalog.snowpark_schema(upper_case=True).fields])
        assert all([f.name.isupper() for f in yml_catalog.snowpark_schema(upper_case=True).fields])
        assert all([f.name.isupper() for f in singer_catalog.snowpark_schema(upper_case=True).fields])

    def test_snowflake_schema(self):
        json_catalog = SnowCatalog(self._test_catalog_json)
        yml_catalog = SnowCatalog(self._test_catalog_yml)
        singer_catalog = SnowCatalog(self._test_catalog_singer)

        assert isinstance(json_catalog.snowflake_schema, list)
        assert isinstance(yml_catalog.snowflake_schema, list)
        assert isinstance(singer_catalog.snowflake_schema, list)

        assert all([isinstance(f, tuple) for f in json_catalog.snowflake_schema])
        assert all([isinstance(f, tuple) for f in yml_catalog.snowflake_schema])
        assert all([isinstance(f, tuple) for f in singer_catalog.snowflake_schema])

        assert json_catalog.snowflake_schema == yml_catalog.snowflake_schema == singer_catalog.snowflake_schema

    def test_audit_schema(self):
        json_catalog = SnowCatalog(self._test_catalog_json)
        yml_catalog = SnowCatalog(self._test_catalog_yml)
        singer_catalog = SnowCatalog(self._test_catalog_singer)

        test_audit_schema = [
            ('CREATED_BY', 'VARCHAR', False, 'CURRENT_USER'),
            ('CREATED_TIMESTAMP', 'TIMESTAMP_NTZ', False, 'CURRENT_TIMESTAMP::TIMESTAMP_NTZ'),
            ('MODIFIED_BY', 'VARCHAR', False, 'CURRENT_USER'),
            ('MODIFIED_TIMESTAMP', 'TIMESTAMP_NTZ', False, 'CURRENT_TIMESTAMP::TIMESTAMP_NTZ')
        ]

        assert isinstance(json_catalog.audit_schema, list)
        assert isinstance(yml_catalog.audit_schema, list)
        assert isinstance(singer_catalog.audit_schema, list)

        assert all([isinstance(f, tuple) for f in json_catalog.audit_schema])
        assert all([isinstance(f, tuple) for f in yml_catalog.audit_schema])
        assert all([isinstance(f, tuple) for f in singer_catalog.audit_schema])

        assert json_catalog.audit_schema == yml_catalog.audit_schema == singer_catalog.audit_schema

        assert yml_catalog.audit_schema == test_audit_schema
        assert json_catalog.audit_schema == test_audit_schema
        assert singer_catalog.audit_schema == test_audit_schema

    def test_column_type_map(self):
        json_catalog = SnowCatalog(self._test_catalog_json)
        yml_catalog = SnowCatalog(self._test_catalog_yml)
        singer_catalog = SnowCatalog(self._test_catalog_singer)

        assert yml_catalog.column_type_map == json_catalog.column_type_map == singer_catalog.column_type_map

        assert isinstance(yml_catalog.column_type_map, dict)
        assert isinstance(json_catalog.column_type_map, dict)
        assert isinstance(singer_catalog.column_type_map, dict)

        assert all([isinstance(key, str) for key in yml_catalog.column_type_map])
        assert all([isinstance(key, str) for key in json_catalog.column_type_map])
        assert all([isinstance(key, str) for key in singer_catalog.column_type_map])

        assert all([isinstance(dt, DataType) for dt in yml_catalog.column_type_map.values()])
        assert all([isinstance(dt, DataType) for dt in json_catalog.column_type_map.values()])
        assert all([isinstance(dt, DataType) for dt in singer_catalog.column_type_map.values()])

        zip_cats = zip(
            yml_catalog.column_type_map.items(),
            json_catalog.column_type_map.items(),
            singer_catalog.column_type_map.items()
        )

        for col_dt_1, col_dt_2, col_dt_3 in zip_cats:
            assert col_dt_1 == col_dt_2 == col_dt_3

    def test__load(self):
        # json and yaml files
        assert isinstance(SnowCatalog._load(self._test_catalog_json), ObjectDefinition)
        assert isinstance(SnowCatalog._load(self._test_catalog_yml), ObjectDefinition)

        # strings
        assert isinstance(SnowCatalog._load(get_catalog_string(self._test_catalog_json)), ObjectDefinition)
        assert isinstance(SnowCatalog._load(get_catalog_string(self._test_catalog_yml)), ObjectDefinition)

    def test__load_singer_catalog(self):
        # files
        singer_file_dict = SnowCatalog._load_singer_catalog(self._test_catalog_singer)
        assert isinstance(singer_file_dict, dict)
        assert isinstance(SnowCatalog._load(singer_file_dict), ObjectDefinition)

        # strings
        singer_str_dict = SnowCatalog._load_singer_catalog(get_catalog_string(self._test_catalog_singer))
        assert isinstance(singer_str_dict, dict)
        assert isinstance(SnowCatalog._load(singer_str_dict), ObjectDefinition)

    def test_column_tags(self):
        yml_tags = SnowCatalog(self._test_catalog_yml).column_tags
        json_tags = SnowCatalog(self._test_catalog_json).column_tags
        singer_tags = SnowCatalog(self._test_catalog_singer).column_tags

        test_tags = {
            'NAME_COL': 'PII_TAG',
            'EMAIL_COL': 'PII_TAG',
            'ACCOUNT_COL': 'PII_TAG',
            'TAG_COLUMN_PII': 'PII_TAG'
        }

        for col, tag in test_tags.items():
            assert yml_tags.get(col) == json_tags.get(col) == singer_tags.get(col) == tag
