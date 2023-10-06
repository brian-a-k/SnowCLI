from typing import Tuple
from snow.client import SnowClient
from snow.utility.ddl_utils import SnowDDLUtil
from snow.utility.dcl_utils import SnowDCLUtil
from snow.schema.catalog import SnowCatalog


class TableNamespace:
    """
    Utility class to enforce full table namespaces in the snow lib.
    """

    def __init__(self, table_val: str) -> None:
        """
        :param table_val: User passed snowflake table name/namespace.
        """
        self.database, self.schema, self.table = self.parse_table(table_val)
        self.staging_schema = 'STAGING'

    @property
    def name(self):
        """
        :return: Fully qualified table namespace for passed table.
        """
        return f'"{self.database}"."{self.schema}"."{self.table}"'.upper()

    @property
    def base(self) -> str:
        """
        :return: Fully qualified table namespace for base table.
        """
        if self.schema.upper() != self.staging_schema:
            return self.name

        base_schema = self.table.split('_').pop(0)
        base_tbl = self.table.replace(f'{base_schema}_', '')
        return f'"{self.database}"."{base_schema}"."{base_tbl}"'.upper()

    @property
    def staging(self) -> str:
        """
        Function to infer the staging table name from the base/target table.

        Example:
        `DATABASE.REF.MY_TABLE` -> returns: `DATABASE.STAGING.REF_MY_TABLE`

        :return: Inferred Stage table name.
        """
        if self.schema.upper() == self.staging_schema:
            return self.name

        stage_tbl = f'{self.schema}_{self.table}'
        return f'"{self.database}"."{self.staging_schema}"."{stage_tbl}"'.upper()

    @property
    def stream(self) -> str:
        """
        Function to infer the stream name from the base table.

        :return: Fully qualified stream namespace.
        """
        base_table = self.base.replace('"', '')
        return f'{base_table}__STREAM'

    @staticmethod
    def parse_table(table_val: str) -> Tuple[str, ...]:
        """
        :param table_val: User passed snowflake table name/namespace.
        :return: Tuple of (DATABASE, SCHEMA, TABLE).
        :raises: Exception if the passed table is not fully qualified.
        """
        parsed_tbl = tuple(table_val.replace('"', '').split('.'))

        if len(parsed_tbl) != 3:
            raise Exception(f'{table_val}: Must be fully qualified table namespace: DATABASE.SCHEMA.TABLE')

        return parsed_tbl


def setup_table_namespace(
        client: SnowClient,
        table: TableNamespace,
        catalog: SnowCatalog,
        staging: bool,
        cdc: bool = False
) -> str:
    """
    Utility function for creating snowflake SCHEMA.TABLE namespace & assigning it proper ownership privileges.

    :param client: SnowClient for Snowflake SnowPark connection.
    :param table: TableNamespace object.
    :param catalog: SnowCatalog object.
    :param staging: Boolean flag for creating either the stage or base table namespace.
    :param cdc: Boolean flag for enabling CDC streams on a target table.
    :return: Fully qualified table name.
    """
    dcl_util = SnowDCLUtil(client)
    ddl_util = SnowDDLUtil(client)

    # get database ownership
    db_owner = dcl_util.get_database_owner(table.database)

    # either staging or base table
    if staging:
        schema_name = table.staging_schema
        table_name = table.staging
    else:
        schema_name = table.schema
        table_name = table.base

    # create snowflake schema if not exists and assign ownership to the database owner
    if ddl_util.schema_exists(table.database, schema_name) is False:
        ddl_util.create_schema(table.database, schema_name)
        dcl_util.grant_schema_ownership(database=table.database, schema_name=schema_name, role=db_owner)

    # create either base or staging table and assign ownership to the database owner
    if cdc is True:
        # if CDC table do not add audit columns
        ddl_util.create_table(table_name=table_name, catalog=catalog, stage=staging, audit=False)
    else:
        # default to adding audit columns
        ddl_util.create_table(table_name=table_name, catalog=catalog, stage=staging, audit=True)

    dcl_util.grant_table_ownership(table_name=table_name, role=db_owner)

    return table_name
