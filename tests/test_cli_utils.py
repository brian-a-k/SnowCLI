import pytest
from snow.utility.cli_utils import TableNamespace


class TestTableNamespace:
    def test_name(self):
        assert TableNamespace('db.schema.test_table').name == '"DB"."SCHEMA"."TEST_TABLE"'
        assert TableNamespace('DB.SCHEMA.TEST_TABLE').name == '"DB"."SCHEMA"."TEST_TABLE"'
        assert TableNamespace('"DB"."SCHEMA"."TEST_TABLE"').name == '"DB"."SCHEMA"."TEST_TABLE"'

    def test_staging(self):
        assert TableNamespace('db.schema.test_table').staging == '"DB"."STAGING"."SCHEMA_TEST_TABLE"'
        assert TableNamespace('DB.SCHEMA.TEST_TABLE').staging == '"DB"."STAGING"."SCHEMA_TEST_TABLE"'
        assert TableNamespace('DB.STAGING.SCHEMA_TEST_TABLE').staging == '"DB"."STAGING"."SCHEMA_TEST_TABLE"'
        assert TableNamespace('"DB"."STAGING"."SCHEMA_TEST_TABLE"').staging == '"DB"."STAGING"."SCHEMA_TEST_TABLE"'
        assert TableNamespace('"DB"."STAGING"."SCHEMA_TEST_TABLE"').staging == '"DB"."STAGING"."SCHEMA_TEST_TABLE"'

    def test_base(self):
        assert TableNamespace('db.schema.test_table').base == '"DB"."SCHEMA"."TEST_TABLE"'
        assert TableNamespace('DB.SCHEMA.TEST_TABLE').base == '"DB"."SCHEMA"."TEST_TABLE"'
        assert TableNamespace('DB.STAGING.SCHEMA_TEST_TABLE').base == '"DB"."SCHEMA"."TEST_TABLE"'
        assert TableNamespace('"DB"."SCHEMA"."TEST_TABLE"').base == '"DB"."SCHEMA"."TEST_TABLE"'
        assert TableNamespace('"DB"."SCHEMA"."TEST_TABLE"').base == '"DB"."SCHEMA"."TEST_TABLE"'

    def test_stream(self):
        assert TableNamespace('db.schema.test_table').stream == 'DB.SCHEMA.TEST_TABLE__STREAM'
        assert TableNamespace('DB.SCHEMA.TEST_TABLE').stream == 'DB.SCHEMA.TEST_TABLE__STREAM'
        assert TableNamespace('"DB"."SCHEMA"."TEST_TABLE"').stream == 'DB.SCHEMA.TEST_TABLE__STREAM'
        assert TableNamespace('DB.STAGING.SCHEMA_TEST_TABLE').stream == 'DB.SCHEMA.TEST_TABLE__STREAM'

    def test_parse_table(self):
        assert TableNamespace.parse_table('db.schema.test_table') == ('db', 'schema', 'test_table')
        assert TableNamespace.parse_table('"db"."schema"."test_table"') == ('db', 'schema', 'test_table')

        # error tests
        with pytest.raises(Exception) as exp:
            _ = TableNamespace.parse_table('schema.test_table')
        assert exp.type == Exception
        assert str(exp.value) == 'schema.test_table: Must be fully qualified table namespace: DATABASE.SCHEMA.TABLE'

        with pytest.raises(Exception) as exp:
            _ = TableNamespace.parse_table('test_table')
        assert exp.type == Exception
        assert str(exp.value) == 'test_table: Must be fully qualified table namespace: DATABASE.SCHEMA.TABLE'

        with pytest.raises(Exception) as exp:
            _ = TableNamespace.parse_table('db.sch1.sch2.test_table')
        assert exp.type == Exception
        assert str(
            exp.value) == 'db.sch1.sch2.test_table: Must be fully qualified table namespace: DATABASE.SCHEMA.TABLE'
