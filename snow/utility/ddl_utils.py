from typing import List, Union, Tuple, Dict, Optional

from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import col, upper, sql_expr, lit
from snowflake.snowpark.types import StructType, DataType

from snow.client import SnowClient
from snow.schema.catalog import SnowCatalog
from snow.schema.snow_types import SnowTypeMap


class SnowDDLUtil:
    """
    Class for running all Snowflake Data Definition (DDL) statements.

    - Create tables & schemas
    - Alter tables & columns
    """

    def __init__(self, client: SnowClient) -> None:
        """
        :param client: SnowClient for Snowpark connection.
        """
        self.client = client
        self.namespace = f'{self.client.session.get_current_database()}.{self.client.session.get_current_schema()}'

    def schema_exists(self, database: str, schema_name: str) -> bool:
        """
        :param database: Snowflake database name
        :param schema_name: Snowflake schema name
        :return: Boolean, if the schema exists in the selected database
        """
        meta_tbl_name = f'{database.upper()}.INFORMATION_SCHEMA.SCHEMATA'
        meta_tbl = self.client.session.table(meta_tbl_name)

        schema_query = meta_tbl.filter(
            upper(col('SCHEMA_NAME')) == schema_name.upper()
        ).select(col('SCHEMA_NAME')).collect()

        if schema_query:
            return True
        else:
            return False

    def table_exists(self, table_name: str) -> Tuple[bool, int]:
        """
        :param table_name: Fully qualified table name: database.schema.table
        :return: Tuple of (True, table row count int) if table exists, else (False, 0).
        """
        try:
            tbl = self.client.session.table(table_name)
            return True, tbl.count()
        except SnowparkSQLException:
            return False, 0

    def create_schema(self, database: str, schema_name: str) -> None:
        """
        :param database: Snowflake database name
        :param schema_name: Snowflake schema name
        :return: None, displays results of create statement
        """
        _query = f'CREATE SCHEMA IF NOT EXISTS {database.upper()}.{schema_name.upper()};'
        self.client.session.sql(_query).show()

    def create_table(self, table_name: str, catalog: SnowCatalog, audit: bool = True, stage: bool = False) -> None:
        """
        :param table_name: Fully qualified table name: database.schema.table
        :param catalog: SnowCatalog object.
        :param audit: Boolean flag to add audit columns to created table (default True).
        :param stage: Boolean flag to add only staging audit columns to created table (default False).
        :return: None, runs a CREATE TABLE IF NOT EXISTS query and displays results.
        """
        table_tags = catalog.column_tags
        table_columns = []

        # add schema columns
        for name, d_type, nullable in catalog.snowflake_schema:
            if nullable is False:
                col_str = f'{name.upper()} {d_type} NOT NULL'
            else:
                col_str = f'{name.upper()} {d_type}'

            # add column tags
            col_tag = table_tags.get(name.upper())

            if col_tag is not None:
                tag_col_str = f"{col_str} WITH TAG ({self.namespace}.{col_tag} = 'tag-based policies')"
                table_columns.append(tag_col_str)
            else:
                table_columns.append(col_str)

        # add audit columns
        if audit:
            for name, d_type, nullable, default_val in catalog.audit_schema:
                if nullable is False:
                    audit_col_str = f'{name.upper()} {d_type} NOT NULL DEFAULT {default_val}'
                else:
                    audit_col_str = f'{name.upper()} {d_type} DEFAULT {default_val}'

                # if staging table only add CREATED_* audit columns
                if stage and name not in catalog.stage_audit_fields:
                    continue
                # not a staging table add ALL audit columns
                else:
                    table_columns.append(audit_col_str)

        create_table_query = f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(table_columns)});'
        self.client.session.sql(create_table_query).show()

    def drop_columns(self, table_name: str, drop_cols: Union[str, List[str], StructType]) -> None:
        """
        :param table_name: Fully qualified table name: database.schema.table
        :param drop_cols: Single string or list of column name strings to drop, or Snowpark StructType.
        :return: None, runs ALTER TABLE DROP COLUMN query.
        """
        if isinstance(drop_cols, str):
            drop_query = (
                f'ALTER TABLE {table_name} '
                f'DROP COLUMN {drop_cols};'
            )
        elif isinstance(drop_cols, StructType):
            drop_query = (
                f'ALTER TABLE {table_name} '
                f'DROP COLUMN {", ".join([f.name.upper() for f in drop_cols.fields])};'
            )
        else:
            drop_query = (
                f'ALTER TABLE {table_name} '
                f'DROP COLUMN {", ".join([f.upper() for f in drop_cols])};'
            )

        self.client.session.sql(drop_query).show()

    def add_columns(self, table_name: str, add_cols: StructType) -> None:
        """
        :param table_name: Fully qualified table name: database.schema.table
        :param add_cols: Snowpark StructType of columns to add to the passed table.
        :return: None, runs ALTER TABLE ADD COLUMN statement.
        """
        # error if new columns have a non-null constraint AND the table is not empty
        tbl_exists, tbl_cnt = self.table_exists(table_name)

        if tbl_exists and tbl_cnt > 0:
            new_non_nulls = [f.name for f in add_cols.fields if f.nullable is False]
            if new_non_nulls:
                raise Exception(f'Non-nullable column(s): {new_non_nulls} cannot be added to a non-empty table')

        # add new column(s)
        type_map = SnowTypeMap()

        for field in add_cols.fields:
            if field.nullable is False:
                # the table is empty so we can add the non-null constraint
                add_query = (
                    f'ALTER TABLE {table_name} '
                    f'ADD COLUMN {field.name} {type_map.convert(field.datatype)} NOT NULL;'
                )
            else:
                add_query = (
                    f'ALTER TABLE {table_name} '
                    f'ADD COLUMN {field.name} {type_map.convert(field.datatype)};'
                )

            self.client.session.sql(add_query).show()

    def alter_nullability(self, table_name: str, null_cols: StructType) -> None:
        """
        :param table_name: Fully qualified table name: database.schema.table
        :param null_cols: Snowpark StructType of columns with modified nullability constraints.
        :return: None, runs ALTER COLUMN SET/DROP NOT NULL statement for each column passed.
        """
        null_counts = self.get_null_counts(table_name)

        for field in null_cols.fields:
            if field.nullable is False and null_counts.get(field.name.upper()) > 0:
                raise Exception(f'Column {field} contains null values cannot add non-null constraint')

            elif field.nullable is False and null_counts.get(field.name.upper()) == 0:
                self.client.session.sql((
                    f'ALTER TABLE {table_name} '
                    f'ALTER COLUMN {field.name.upper()} SET NOT NULL;'
                )).show()

            elif field.nullable is True:
                self.client.session.sql((
                    f'ALTER TABLE {table_name} '
                    f'ALTER COLUMN {field.name.upper()} DROP NOT NULL;'
                )).show()

    def get_table_data_types(self, table_name: str) -> Dict[str, DataType]:
        """
        :param table_name: Fully qualified table name: database.schema.table
        :return: Dict of column names and Snow data types.
        """
        type_map = SnowTypeMap()
        desc_tbl_query = f'DESCRIBE TABLE {table_name};'

        tbl_desc_df = [
            row.as_dict() for row in self.client.session.sql(desc_tbl_query).select(['"name"', '"type"']).collect()
        ]

        return {row.get('name'): type_map.get(row.get('type')) for row in tbl_desc_df}

    def get_null_counts(self, table_name: str) -> Dict[str, int]:
        """
        :param table_name: Fully qualified table name: database.schema.table
        :return: Dict of column names with count of null values for each column.
        """
        tbl = self.client.session.table(table_name)

        null_expr = []
        for field in tbl.schema.fields:
            null_cnt = sql_expr(f'ZEROIFNULL(COUNT_IF({field.name} IS NULL))').alias(f'{field.name}')
            null_expr.append(null_cnt)

        return tbl.select(null_expr).collect().pop().as_dict()

    def get_table_tags(self, table_name: str) -> Optional[Dict[str, str]]:
        """
        :param table_name: Fully qualified table name: database.schema.table
        :return: Dict of column name: tag name for any columns with set tags, else None.
        """
        curr_db = self.client.session.get_current_database()

        table_tags_ref = self.client.session.table_function(
            f'{curr_db}.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS', lit(table_name), lit('TABLE')
        ).select(['COLUMN_NAME', 'TAG_NAME']).collect()

        if not table_tags_ref:
            return None

        return {
            row.as_dict().get('COLUMN_NAME'): row.as_dict().get('TAG_NAME') for row in table_tags_ref
        }

    def alter_column_tags(self, table_name: str, tag_cols: List[Tuple[str, str, bool]]) -> None:
        """
        :param table_name: Fully qualified table name: database.schema.table
        :param tag_cols: list of tuples for column tag alters.
        :return: None, runs ALTER COLUMN SET/UNSET TAG statements.
        """
        for field_name, tag_name, tag_operation in tag_cols:
            # Set tag
            if tag_operation:
                self.client.session.sql((
                    f"ALTER TABLE {table_name} "
                    f"ALTER COLUMN {field_name} "
                    f"SET TAG {self.namespace}.{tag_name} = 'tag-based policies';"
                )).show()
            # Unset tag
            else:
                self.client.session.sql((
                    f"ALTER TABLE {table_name} "
                    f"ALTER COLUMN {field_name} "
                    f"UNSET TAG {self.namespace}.{tag_name};"
                )).show()

    def alter_column_types(self, table_name: str, new_data_types: Dict[str, DataType]) -> None:
        """
        :param table_name: Fully qualified table name: database.schema.table
        :param new_data_types: Dict of column names and new (supported) data type conversions.
        :return: None, runs ALTER COLUMN SET DATA TYPE statements.
        """
        type_map = SnowTypeMap()

        for field_name, data_type in new_data_types.items():
            sf_type = type_map.convert(data_type)

            self.client.session.sql((
                f'ALTER TABLE {table_name} '
                f'ALTER COLUMN {field_name} SET DATA TYPE {sf_type};'
            )).show()
