import click

from snow.client import SnowClient
from snow.schema.catalog import SnowCatalog
from snow.utility.cli_utils import TableNamespace, setup_table_namespace
from .data_loader import DataLoader


@click.group(help='Data Loading Sub Command')
def load() -> click.command:
    pass


@load.command(help='Load CSV data from S3')
@click.option('--catalog', help='Catalog file path or JSON string', required=True, type=click.Path(dir_okay=True))
@click.option('--stage', help='Snowflake named external stage', required=True)
@click.option('--table', help='Fully qualified table name: database.schema.table', required=True)
@click.option('--prefix', help='S3 Prefix for staged data files', required=True)
@click.option('--file_format', help='Named file format in Snowflake', required=False)
@click.option('--is_stage', help='Optional flag for loading into the staging table', is_flag=True)
@click.pass_context
def csv(ctx, catalog: str, stage: str, table: str, prefix: str, file_format: str, is_stage: bool):
    snow_client = SnowClient(ctx.obj)
    catalog = SnowCatalog(catalog_data=catalog)

    # table & schema setup
    target_tbl_name = setup_table_namespace(
        client=snow_client,
        table=TableNamespace(table),
        catalog=catalog,
        staging=is_stage
    )

    # if loading to a staging table clear the table before loading new data
    if is_stage is True:
        stage_tbl = snow_client.session.table(target_tbl_name)
        stage_delete_result = stage_tbl.delete(block=True)
        click.echo(f'Clearing Staging Table: {target_tbl_name}, {stage_delete_result}')

    csv_loader = DataLoader(
        catalog=catalog,
        client=snow_client,
        stage_name=stage,
        file_format_name=file_format
    )

    load_summary = csv_loader.load_csv(prefix=prefix, table_name=target_tbl_name)

    click.echo(f'Load Summary: {load_summary}')
    snow_client.session.close()


@load.command(help='Load PARQUET data from S3')
@click.option('--catalog', help='Catalog file path or JSON string', required=True, type=click.Path(dir_okay=True))
@click.option('--stage', help='Snowflake named external stage', required=True)
@click.option('--table', help='Fully qualified table name: database.schema.table', required=True)
@click.option('--prefix', help='S3 Prefix for staged data files', required=True)
@click.option('--file_format', help='Named file format in Snowflake', required=False)
@click.option('--is_stage', help='Optional flag for loading into the staging table', is_flag=True)
@click.pass_context
def parquet(ctx, catalog: str, stage: str, table: str, prefix: str, file_format: str, is_stage: bool):
    snow_client = SnowClient(ctx.obj)
    catalog = SnowCatalog(catalog_data=catalog)

    # table & schema setup
    target_tbl_name = setup_table_namespace(
        client=snow_client,
        table=TableNamespace(table),
        catalog=catalog,
        staging=is_stage
    )

    # if loading to a staging table clear the table before loading new data
    if is_stage is True:
        stage_tbl = snow_client.session.table(target_tbl_name)
        stage_delete_result = stage_tbl.delete(block=True)
        click.echo(f'Clearing Staging Table: {target_tbl_name}, {stage_delete_result}')

    parquet_loader = DataLoader(
        catalog=catalog,
        client=snow_client,
        stage_name=stage,
        file_format_name=file_format
    )

    load_summary = parquet_loader.load_semi_structured_file(
        prefix=prefix,
        table_name=target_tbl_name,
        file_type='PARQUET'
    )

    click.echo(f'Load Summary: {load_summary}')
    snow_client.session.close()


@load.command(help='Load JSON data from S3')
@click.option('--catalog', help='Catalog file path or JSON string', required=True, type=click.Path(dir_okay=True))
@click.option('--stage', help='Snowflake named external stage', required=True)
@click.option('--table', help='Fully qualified table name: database.schema.table', required=True)
@click.option('--prefix', help='S3 Prefix for staged data files', required=True)
@click.option('--file_format', help='Named file format in Snowflake', required=False)
@click.option('--is_stage', help='Optional flag for loading into the staging table', is_flag=True)
@click.pass_context
def json(ctx, catalog: str, stage: str, table: str, prefix: str, file_format: str, is_stage: bool):
    snow_client = SnowClient(ctx.obj)
    catalog = SnowCatalog(catalog_data=catalog)

    # table & schema setup
    target_tbl_name = setup_table_namespace(
        client=snow_client,
        table=TableNamespace(table),
        catalog=catalog,
        staging=is_stage
    )

    # if loading to a staging table clear the table before loading new data
    if is_stage is True:
        stage_tbl = snow_client.session.table(target_tbl_name)
        stage_delete_result = stage_tbl.delete(block=True)
        click.echo(f'Clearing Staging Table: {target_tbl_name}, {stage_delete_result}')

    json_loader = DataLoader(
        catalog=catalog,
        client=snow_client,
        stage_name=stage,
        file_format_name=file_format
    )

    load_summary = json_loader.load_semi_structured_file(
        prefix=prefix,
        table_name=target_tbl_name,
        file_type='JSON'
    )

    click.echo(f'Load Summary: {load_summary}')
    snow_client.session.close()


load.add_command(csv)
load.add_command(parquet)
load.add_command(json)
