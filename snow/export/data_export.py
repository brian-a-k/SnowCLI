from typing import List, Optional

import click
from snowflake.snowpark.functions import col

from snow.client import SnowClient


class DataExporter:
    def __init__(self, client: SnowClient, stage_name: str) -> None:
        """
        :param client: SnowClient for Snowflake SnowPark connection.
        :param stage_name: Named External Snowflake Stage.
        """
        self.client = client
        self.stage = stage_name

    def export_query(self, sql: str, prefix: str) -> List[str]:
        """
        :param sql: SQL query/statement string.
        :param prefix: Stage prefix to write exported data.
        :return:
        """
        copy_options = {
            'DETAILED_OUTPUT': True,
            'MAX_FILE_SIZE': 512_000_000,
            'OVERWRITE': True
        }

        query_df = self.client.session.sql(sql)

        stage_prefix = f'@{self.stage}/{prefix}'
        copy_result = query_df.write.copy_into_location(location=stage_prefix, header=True, **copy_options)

        click.echo(f'Copy Result: {copy_result}')

        # return full s3 bucket/prefix for exported file
        export_s3_keys = [
            row.as_dict().get('name') for row in self.client.session.sql(f'LIST {stage_prefix};').collect()
        ]

        return export_s3_keys

    def export_stream(self, stream_name: str, columns: List[str], prefix: str) -> Optional[str]:
        """
        :param stream_name: Table Stream name.
        :param columns: List of columns to export.
        :param prefix: S3 key/prefix for exported file.
        :return: String, full S3 bucket/prefix for the exported file.
        """
        copy_options = {
            'SINGLE': True,
            'DETAILED_OUTPUT': True,
            'MAX_FILE_SIZE': 512_000_000,
            'OVERWRITE': True
        }

        stage_prefix = f'@{self.stage}/{prefix}'
        stream_df = self.client.session.table(stream_name)

        # row counts for inserts & updates
        insert_count = stream_df.filter(
            (col('METADATA$ACTION') == 'INSERT') &
            (col('METADATA$ISUPDATE').__eq__(False))
        ).count()

        update_count = stream_df.filter(
            (col('METADATA$ACTION') == 'INSERT') &
            (col('METADATA$ISUPDATE').__eq__(True))
        ).count()

        click.echo(f'Insert Row Count: {insert_count}, Update Row Count: {update_count}')

        # export stream to S3
        export_df = stream_df.filter((col('METADATA$ACTION') == 'INSERT')).select(columns)
        copy_result = export_df.write.copy_into_location(location=stage_prefix, header=True, **copy_options)
        click.echo(f'Copy Result: {copy_result}')

        # return full s3 bucket/prefix for exported file
        export_s3_key = [
            row.as_dict().get('name') for row in self.client.session.sql(f'LIST {stage_prefix};').collect()
        ]

        if export_s3_key:
            return export_s3_key.pop(0)
        else:
            return None
