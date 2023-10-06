from snow.client import SnowClient
from snowflake.snowpark.functions import col, upper


class SnowDCLUtil:
    def __init__(self, client: SnowClient) -> None:
        self.client = client

    def get_database_owner(self, database: str) -> str:
        """
        :param database: Snowflake database name
        :return: String role/username that owns the database
        """
        meta_tbl_name = f'{database.upper()}.INFORMATION_SCHEMA.DATABASES'
        meta_tbl = self.client.session.table(meta_tbl_name)

        owner_query = meta_tbl.filter(
            upper(col('DATABASE_NAME')) == database.upper()
        ).select(col('DATABASE_OWNER')).collect()

        if owner_query:
            return owner_query.pop().as_dict().get('DATABASE_OWNER')
        else:
            raise Exception(f'Database: {database} does not exist')

    def grant_schema_ownership(self, database: str, schema_name: str, role: str) -> None:
        """
        :param database: Snowflake database name
        :param schema_name: Snowflake schema name
        :param role: Snowflake role to assign schema ownership
        :return: None, displays results of grant statement
        """
        _schema = f'{database.upper()}.{schema_name.upper()}'
        _query = f'GRANT OWNERSHIP ON SCHEMA {_schema} TO ROLE {role.upper()} COPY CURRENT GRANTS;'
        self.client.session.sql(_query).show()

    def grant_table_ownership(self, table_name: str, role: str) -> None:
        """
        :param table_name: Fully qualified table name: database.schema.table
        :param role: Snowflake role to assign table ownership
        :return: None, displays results of grant statement
        """
        _query = f'GRANT OWNERSHIP ON TABLE {table_name} TO ROLE {role.upper()} COPY CURRENT GRANTS;'
        self.client.session.sql(_query).show()
