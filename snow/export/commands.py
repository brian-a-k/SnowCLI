from datetime import datetime

import click
import json

from snow.client import SnowClient
from snow.schema.catalog import SnowCatalog
from snow.utility.cli_utils import TableNamespace
from .data_export import DataExporter


@click.group(help='Data Export Sub Command')
def export() -> click.command:
    pass


@export.command(help='Export CDC stream to S3')
@click.option('--catalog', help='Catalog file path or JSON string', required=True, type=click.Path(dir_okay=True))
@click.option('--table', help='Fully qualified table name: database.schema.table', required=True)
@click.option('--stage', help='Snowflake named external stage', required=True)
@click.option('--xcom/--no-xcom', help='Flag for XCOM output', is_flag=True, default=True)
@click.pass_context
def stream(ctx, catalog, table: str, stage: str, xcom: bool):
    snow_client = SnowClient(ctx.obj)
    catalog = SnowCatalog(catalog_data=catalog)
    data_exporter = DataExporter(client=snow_client, stage_name=stage)

    stream_name = TableNamespace(table_val=table).stream
    prefix_ts = datetime.now().strftime('%Y%m%dT%H%M%S')
    s3_prefix = f'export/{stream_name}/{prefix_ts}/{stream_name}.csv.gz'

    export_path = data_exporter.export_stream(
        stream_name=stream_name,
        columns=[f.name for f in catalog.snowpark_schema(upper_case=True).fields],
        prefix=s3_prefix
    )

    if xcom is True:
        click.echo('Writing XCOM file')

        with open(file='/airflow/xcom/return.json', mode='w') as xcom_file:
            json.dump({'path': export_path}, xcom_file, separators=(',', ':'))
    else:
        click.echo(f'Export S3 Key: {export_path}')


@export.command(help='Export query result to S3')
@click.option('--sql', help='Snowflake SQL statement string', required=True)
@click.option('--root', help='Root name for S3 prefix', required=True)
@click.option('--stage', help='Snowflake named external stage', required=True)
@click.option('--xcom/--no-xcom', help='Flag for XCOM output', is_flag=True, default=True)
@click.pass_context
def query(ctx, sql: str, root: str, stage: str, xcom: bool):
    snow_client = SnowClient(ctx.obj)
    data_exporter = DataExporter(client=snow_client, stage_name=stage)

    prefix_ts = datetime.now().strftime('%Y%m%dT%H%M%S')
    s3_prefix = f'export/{root}/{prefix_ts}/'

    export_paths = data_exporter.export_query(sql=sql, prefix=s3_prefix)

    if xcom is True:
        click.echo('Writing XCOM file')

        with open(file='/airflow/xcom/return.json', mode='w') as xcom_file:
            json.dump({'path': export_paths}, xcom_file, separators=(',', ':'))
    else:
        click.echo(f'Query Export Result: {export_paths}')


export.add_command(stream)
export.add_command(query)
