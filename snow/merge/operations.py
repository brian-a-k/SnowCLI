import click
from typing import Optional
from snowflake.snowpark import (
    Column,
    DataFrame,
    WhenMatchedClause,
    WhenNotMatchedClause,
    MergeResult
)
from snowflake.snowpark.functions import when_matched, when_not_matched

from snow.client import SnowClient
from snow.schema.catalog import SnowCatalog
from snow.utility.dml_utils import SnowDMLUtil


class UpsertOperation:
    """
    Pattern for performing an update/insert (Upsert) MERGE INTO operation from a source table to a target table.

    https://docs.snowflake.com/en/sql-reference/sql/merge.html

    - `join_expr`: Generated from the primary_key columns in the catalog.
    - `matchedClause`: When a match is found in the target table update row values from the source table.
    - `notMatchedClause`: When not matched, inserts new rows from the source table to the target table.

    The source/staging table is cleared after the merge is complete for subsequent incremental data loads.

    """

    def __init__(self, catalog: SnowCatalog, client: SnowClient, cdc: bool):
        """
        :param catalog: SnowCatalog object
        :param client: SnowClient for Snowflake SnowPark connection
        :param cdc: Boolean flag for enabling CDC streams on a target table.
        """
        self.catalog = catalog
        self.client = client
        self.cdc = cdc
        self.dml = SnowDMLUtil(client)

    def merge(
            self,
            source_table: str,
            target_table: str,
            stream_name: Optional[str] = None,
            distinct: bool = False
    ) -> MergeResult:
        """
        :param source_table: Fully qualified table name: database.schema.table for staging/source table.
        :param target_table: Fully qualified table name: database.schema.table for target/base table.
        :param stream_name: Fully qualified stream name if using CDC on the target table.
        :param distinct: SQL SELECT DISTINCT flag for the source/staging table (default False).
        :return: Snowpark MergeResult.
        """
        # Snowpark dataframes for source & target tables
        target_df = self.client.session.table(target_table)

        # distinct rows from source/staging table
        if distinct is True:
            click.echo(f'SELECT DISTINCT flag enabled for source/staging table: {source_table}')
            _ = self.dml.distinct_diff(table_name=source_table)

            source_df = self.client.session.table(source_table).distinct()
        else:
            source_df = self.client.session.table(source_table)

        # get join expression
        join_expr = self._get_join_expression(stage_table=source_df, target_table=target_df)

        # get WHEN MATCHED (update) & WHEN NOT MATCHED (insert) clauses
        update_clause = self._get_when_matched_clause(source_table=source_df)
        insert_clause = self._get_when_not_matched_clause(source_table=source_df)

        # create CDC stream on target table before DML merge operation
        if self.cdc is True and stream_name is not None:
            click.echo(f'CDC Enabled - Table: {target_table}, Stream: {stream_name}')

            self.client.session.sql(
                f'CREATE OR REPLACE STREAM {stream_name} ON TABLE {target_table};'
            ).show()

        # MERGE INTO results
        merge_result = target_df.merge(
            source=source_df,
            join_expr=join_expr,
            clauses=[update_clause, insert_clause],
            block=True
        )

        # for upsert pattern delete all rows from source/staging table after merge is complete
        if merge_result:
            if distinct is True:
                stage_delete_result = self.client.session.table(source_table).delete(block=True)
            else:
                stage_delete_result = source_df.delete(block=True)

            click.echo(f'Clearing Staging Table: {source_table}, {stage_delete_result}')

        return merge_result

    def _get_join_expression(self, stage_table: DataFrame, target_table: DataFrame) -> Column:
        """
        :param stage_table: Snowpark Dataframe for MERGE INTO source table.
        :param target_table: Snowpark Dataframe for MERGE INTO target table.
        :return: Join expression for MERGE INTO statement based on catalog primary key(s).
        """
        # get first or single primary key
        p_keys = self.catalog.primary_keys
        first_pk = p_keys.pop(0)
        join_expr = stage_table[first_pk].__eq__(target_table[first_pk])

        # if multiple primary keys join expression needs to be concatenated with `&` bitwise operator
        for pk in p_keys:
            join_expr = join_expr.__and__(stage_table[pk].__eq__(target_table[pk]))

        return join_expr

    def _get_when_matched_clause(self, source_table: DataFrame) -> WhenMatchedClause:
        """
        :param source_table: Snowpark Dataframe for MERGE INTO source table.
        :return: Snowpark WhenMatchedClause.
        """

        # schema columns for update clause
        update_columns = {
            f.name: source_table[f.name] for f in self.catalog.snowpark_schema(upper_case=True).fields
        }

        if self.cdc is False:
            # dict of MODIFIED_* audit columns from the base table updated with CREATED_* columns from staging table
            audit_update_columns = dict(zip(
                self.catalog.base_audit_fields, [source_table[col] for col in self.catalog.stage_audit_fields]
            ))

            update_columns.update(audit_update_columns)

        return when_matched().update(update_columns)

    def _get_when_not_matched_clause(self, source_table: DataFrame) -> WhenNotMatchedClause:
        """
        :param source_table: Snowpark Dataframe for MERGE INTO source table.
        :return: Snowpark WhenNotMatchedClause.
        """

        # schema columns for insert clause
        insert_columns = {
            f.name: source_table[f.name] for f in self.catalog.snowpark_schema(upper_case=True).fields
        }

        if self.cdc is False:
            # audit CREATED_* columns from staging table
            audit_insert_columns = {col: source_table[col] for col in self.catalog.stage_audit_fields}
            insert_columns.update(audit_insert_columns)

        return when_not_matched().insert(insert_columns)


class InsertOperation:
    """
    Pattern for performing an INSERT INTO operation from a source table to a target table.

    Used for Full-refresh loading pattern & Insert/append only operations.

    https://docs.snowflake.com/en/sql-reference/sql/insert.html

    Steps:
        - Delete all from base/target table (Full-refresh pattern)
        - Run INSERT INTO operation
        - After insert is complete, delete all rows from staging/source table for subsequent full-refresh loads.

    """

    def __init__(self, catalog: SnowCatalog, client: SnowClient) -> None:
        """
        :param catalog: SnowCatalog object
        :param client: SnowClient for Snowflake SnowPark connection
        """
        self.catalog = catalog
        self.client = client
        self.dml = SnowDMLUtil(client)

    def insert(self, source_table: str, target_table: str, distinct: bool = False) -> int:
        """
        :param source_table: Fully qualified table name: database.schema.table for staging/source table.
        :param target_table: Fully qualified table name: database.schema.table for target/base table.
        :param distinct: SQL SELECT DISTINCT flag for the source/staging table (default False).
        :return: Int, count of rows in the target table after insert operation is complete.
        """
        # Snowpark dataframes for source & target tables
        target_df = self.client.session.table(target_table)

        # distinct rows from source/staging table
        if distinct is True:
            click.echo(f'SELECT DISTINCT flag enabled for source/staging table: {source_table}')
            _ = self.dml.distinct_diff(table_name=source_table)

            source_df = self.client.session.table(source_table).distinct()
        else:
            source_df = self.client.session.table(source_table)

        # delete from base table
        target_delete_result = target_df.delete(block=True)
        click.echo(f'Full-Refresh Pattern')
        click.echo(f'Clearing Base Table: {target_table}, {target_delete_result}')

        try:
            # insert from stage to base table
            schema_fields = [f.name for f in self.catalog.snowpark_schema(upper_case=True).fields]
            insert_source_df = source_df.select(schema_fields + self.catalog.stage_audit_fields)

            insert_source_df.write.save_as_table(
                table_name=target_table,
                mode='append',
                column_order='name',
                block=True
            )

        finally:
            # delete from stage table
            if distinct is True:
                stage_delete_result = self.client.session.table(source_table).delete(block=True)
            else:
                stage_delete_result = source_df.delete(block=True)

            click.echo(f'Clearing Staging Table: {source_table}, {stage_delete_result}')

        return target_df.count(block=True)
