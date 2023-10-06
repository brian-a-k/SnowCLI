from typing import Optional, List, Dict, Union

import click
from snowflake.snowpark import Row
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import sql_expr, udf
from snowflake.snowpark.types import StringType, BooleanType

from snow.client import SnowClient
from snow.schema.catalog import SnowCatalog
from snow.utility.dcl_utils import SnowDCLUtil
from snow.utility.ddl_utils import SnowDDLUtil


class DataLoader:
    """
    Snow Data loading class.

    Supports loading CSV, PARQUET, and JSON files from an S3 Stage.
    """

    def __init__(
            self,
            catalog: SnowCatalog,
            client: SnowClient,
            stage_name: str,
            file_format_name: Optional[str] = None
    ) -> None:
        """
        :param catalog: SnowCatalog object
        :param client: SnowClient for Snowflake SnowPark connection
        :param stage_name: Named external Snowflake stage
        :param file_format_name: Named Snowflake file format
        """
        self.catalog = catalog
        self.client = client
        self.stage = stage_name
        self.ddl_util = SnowDDLUtil(client)
        self.dcl_util = SnowDCLUtil(client)
        self.stage = f'{self.client.profile.database}.{self.client.profile.sf_schema}.{stage_name}'

        if file_format_name:
            self.file_format = f'{self.client.profile.database}.{self.client.profile.sf_schema}.{file_format_name}'
        else:
            self.file_format = None

    def load_csv(self, prefix: str, table_name: str) -> Dict[str, Union[str, int]]:
        """
        Loading CSV data into a snowflake table

        :param prefix: External stage prefix for staged files
        :param table_name: Target table name to load into, Fully qualified table name: database.schema.table
        :return: Dict of load results.
        """
        # CSV file format options
        file_format_ops = {
            'format_name': self.file_format or self.get_stage_file_format(),
            'error_on_column_count_mismatch': False  # this is manually set to handle audit columns
        }

        # error if no named file format for the stage is defined & no named file format is passed
        if file_format_ops.get('format_name') is None:
            raise Exception((
                f'No default named file format for stage: {self.stage}\n '
                f'Try a different stage or pass in a named file format with the --file_format parameter'
            ))

        # load CSV data
        stage_prefix = f'@{self.stage}/{prefix}'
        csv_reader = self.client.session.read.schema(self.catalog.snowpark_schema())
        stage_df = csv_reader.options(file_format_ops).csv(path=stage_prefix)
        target_columns = [f.name for f in self.catalog.snowpark_schema(upper_case=True).fields]

        # Error with Snowpark _table_exists function with double-quoted table namespace: "DATABASE"."SCHEMA"."TABLE"
        sp_table_exists = self.client.session._table_exists(table_name)
        snow_table_exists, _ = self.ddl_util.table_exists(table_name)

        if sp_table_exists is False and snow_table_exists is True:
            # reset local var here for any further exceptions
            table_name = table_name.split('.')

            click.echo((
                f"WARNING Snowpark table_exists error: snowpark: {sp_table_exists}, snowCLI: {snow_table_exists} "
                f'Attempting table name split - {table_name}'
            ))

        copy_results = stage_df.copy_into_table(
            table_name=table_name,
            target_columns=target_columns,
            format_type_options=file_format_ops
        )

        return self.get_load_summary(table_name=table_name, copy_results=copy_results)

    def load_semi_structured_file(self, prefix: str, table_name: str, file_type: str) -> Dict[str, Union[str, int]]:
        """
        Loading JSON or PARQUET data into a snowflake table

        :param prefix: External stage prefix for staged files
        :param table_name: Target table name to load into, Fully qualified table name: database.schema.table
        :param file_type: String either JSON, PARQUET
        :return: Dict of load results.
        """
        # JSON/PARQUET file format options
        file_format_ops = {
            'format_name': self.file_format or self.get_stage_file_format(),
            'infer_schema': False  # for PARQUET files supress inferred schema query
        }

        # error if no named file format for the stage is defined & no named file format is passed
        if file_format_ops.get('format_name') is None:
            raise Exception((
                f'No default named file format for stage: {self.stage}\n '
                f'Try a different stage or pass in a named file format with the --file_format parameter'
            ))

        stage_prefix = f'@{self.stage}/{prefix}'

        # reader object based on file type
        if file_type == 'PARQUET':
            reader = self.client.session.read.options(file_format_ops).parquet(path=stage_prefix)
        elif file_type == 'JSON':
            reader = self.client.session.read.options(file_format_ops).json(path=stage_prefix)
        else:
            raise Exception(f'File type: {file_type} is not supported')

        # Error with Snowpark _table_exists function with double-quoted table namespace: "DATABASE"."SCHEMA"."TABLE"
        sp_table_exists = self.client.session._table_exists(table_name)
        snow_table_exists, _ = self.ddl_util.table_exists(table_name)

        if sp_table_exists is False and snow_table_exists is True:
            # reset local var here for any further exceptions
            table_name = table_name.split('.')

            click.echo((
                f"WARNING Snowpark table_exists error: snowpark: {sp_table_exists}, snowCLI: {snow_table_exists} "
                f'Attempting table name split - {table_name}'
            ))

        base_transform_statement = [sql_expr(f'$1:{name}::{typ}') for name, typ, _ in self.catalog.snowflake_schema]
        target_columns = [f.name for f in self.catalog.snowpark_schema(upper_case=True).fields]

        # Base COPY INTO operation with basic data-type casting
        try:
            click.echo('Attempting data load with basic data-type casting')

            copy_results = reader.copy_into_table(
                table_name=table_name,
                target_columns=target_columns,
                format_type_options=file_format_ops,
                transformations=base_transform_statement
            )

        # Basic data-type casting failed, wrap with TRY_CAST
        # TODO remove in future iteration source data types should be transformed AFTER they are loaded into Snowflake
        except SnowparkSQLException:
            click.echo(f'Basic data-type casting failed - Retrying data load with data-type transformation')

            # TODO REMOVE THIS (inner functions are BAD but only way snowpark registers the UDF from 3rd party lib)
            @udf(
                name='salesforce_boolean_transform',
                replace=True,
                input_types=[StringType()],
                return_type=BooleanType()
            )
            def boolean_transform(val) -> Optional[bool]:
                boolean_mapping = {
                    # True values
                    "true": True,
                    "t": True,
                    "yes": True,
                    "y": True,
                    "on": True,
                    "1": True,
                    "1.0": True,

                    # False values
                    "false": False,
                    "f": False,
                    "no": False,
                    "n": False,
                    "off": False,
                    "0": False,
                    "0.0": False
                }

                format_val = str(val).strip().lower()
                return boolean_mapping.get(format_val)

            try_cast_transform_statement = []
            for name, typ, _ in self.catalog.snowflake_schema:
                if typ == 'BOOLEAN':
                    click.echo(f'Boolean UDF applied: {name}')
                    tf_exp = boolean_transform(sql_expr(f'$1:{name}')).as_(name)
                else:
                    tf_exp = sql_expr(f'TRY_CAST($1:{name}::STRING AS {typ})').as_(name)

                try_cast_transform_statement.append(tf_exp)

            copy_results = reader.copy_into_table(
                table_name=table_name,
                target_columns=target_columns,
                format_type_options=file_format_ops,
                transformations=try_cast_transform_statement
            )

        return self.get_load_summary(table_name=table_name, copy_results=copy_results)

    def get_stage_file_format(self) -> Optional[str]:
        """
        :return: The named file format set as a default for a Snowflake external stage.
        """
        _query = f'DESC STAGE {self.stage}'
        stage_params = [row.as_dict() for row in self.client.session.sql(_query).collect()]

        for param in stage_params:
            if param.get('parent_property') == 'STAGE_FILE_FORMAT' and param.get('property') == 'FORMAT_NAME':
                ff_namespace = f'{self.client.profile.database}.{self.client.profile.sf_schema}'
                return f'{ff_namespace}.{param.get("property_value")}'

        return None

    @staticmethod
    def get_load_summary(
            table_name: Union[str, List[str]],
            copy_results: List[Row],
            verbose: bool = True
    ) -> Dict[str, Union[str, int]]:
        """
        :param table_name: Fully qualified table name: database.schema.table, or list of [database, schema, table]
        :param copy_results: List of Row objects returned from COPY INTO query.
        :param verbose: Log individual file load results.
        :return: Dict of aggregated load results.
        """
        zero_load_result = {'status': 'Copy executed with 0 files processed.'}

        load_summary = {
            'table_name': table_name,
            'files_processed': 0,
            'files_loaded': 0,
            'rows_parsed': 0,
            'rows_loaded': 0
        }

        load_results = [row.as_dict() for row in copy_results]
        files_processed = len(load_results)

        if files_processed == 1 and load_results[0] == zero_load_result:
            return load_summary
        else:
            load_summary['files_processed'] = files_processed

        load_summary['files_loaded'] = len([res for res in load_results if res.get('status') == 'LOADED'])
        load_summary['rows_parsed'] = sum([res.get('rows_parsed') for res in load_results])
        load_summary['rows_loaded'] = sum([res.get('rows_loaded') for res in load_results])

        if verbose:
            click.echo('Load Results:')
            for res in load_results:
                click.echo(res)

        return load_summary
