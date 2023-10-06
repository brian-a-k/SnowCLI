import click

from snow.client import SnowClient


class SnowDMLUtil:
    def __init__(self, client: SnowClient) -> None:
        """
        :param client: SnowClient for Snowpark connection.
        """
        self.client = client

    def distinct_diff(self, table_name: str) -> bool:
        """
        Utility function for detecting duplicates.

        :param table_name: Fully qualified table name: database.schema.table
        :return: Boolean, True if table contains all distinct rows, else False
        """
        base_df = self.client.session.table(table_name)
        distinct_df = base_df.distinct()

        base_count = base_df.count()
        distinct_count = distinct_df.count()

        if base_count == distinct_count:
            click.echo(f'Table: {table_name}, contains distinct rows')
            return True
        else:
            click.echo(f'WARNING Table: {table_name}, contains duplicate rows')
            click.echo((
                f'Total Row Count: {base_count}, \n'
                f'Distinct Row Count: {distinct_count}, \n'
                f'Row Count Diff: {abs(base_count - distinct_count)}, \n'
            ))
            return False
