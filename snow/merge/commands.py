import click
from snow.client import SnowClient
from snow.schema.catalog import SnowCatalog
from snow.utility.cli_utils import TableNamespace
from .operations import UpsertOperation, InsertOperation


@click.group(help='Data Merge Sub Command')
def merge() -> click.command:
    pass


@merge.command(help='Run an UPSERT merge pattern for incremental data loads')
@click.option('--catalog', help='Catalog file path or JSON string', required=True, type=click.Path(dir_okay=True))
@click.option('--target', help='Fully qualified table namespace for target/base table', required=True)
@click.option('--source', help='Optional source/staging table, inferred if not passed', required=False, default=None)
@click.option('--cdc', help='CDC flag', is_flag=True)
@click.option('--distinct', help='SELECT DISTINCT * flag for the source/staging table', is_flag=True)
@click.pass_context
def upsert(ctx, catalog: str, target: str, source, cdc: bool, distinct: bool):
    snow_client = SnowClient(ctx.obj)
    catalog = SnowCatalog(catalog_data=catalog)

    target_ns = TableNamespace(target)

    # infer optional source table with matching STAGING table
    if source is None:
        source_ns = target_ns.staging
        click.echo(f'Inferred Staging Table: {source_ns}')
    else:
        source_ns = TableNamespace(source).name

    upsert_merger = UpsertOperation(catalog=catalog, client=snow_client, cdc=cdc)

    if cdc is True:
        # run upsert operation with CDC enabled
        upsert_result = upsert_merger.merge(
            source_table=source_ns,
            target_table=target_ns.name,
            stream_name=target_ns.stream,
            distinct=distinct
        )
    else:
        # run default upsert operation
        upsert_result = upsert_merger.merge(
            source_table=source_ns,
            target_table=target_ns.name,
            stream_name=None,
            distinct=distinct
        )

    click.echo(f'Upsert Complete - Source Table: {source_ns}, Target Table: {target_ns.name}')
    click.echo(f'Upsert Merge Result: {upsert_result}')
    snow_client.session.close()


@merge.command(help='Run and INSERT only pattern for full-refresh data loads')
@click.option('--catalog', help='Catalog file path or JSON string', required=True, type=click.Path(dir_okay=True))
@click.option('--target', help='Fully qualified table namespace for target/base table', required=True)
@click.option('--source', help='Optional source/staging table, inferred if not passed', required=False, default=None)
@click.option('--distinct', help='SELECT DISTINCT * flag for the source/staging table', is_flag=True)
@click.pass_context
def insert(ctx, catalog: str, target: str, source, distinct: bool):
    snow_client = SnowClient(ctx.obj)
    catalog = SnowCatalog(catalog_data=catalog)

    target_ns = TableNamespace(target)

    # infer optional source table with matching STAGING table
    if source is None:
        source_ns = target_ns.staging
        click.echo(f'Inferred Staging Table: {source_ns}')
    else:
        source_ns = TableNamespace(source).name

    # run insert operation
    insert_op = InsertOperation(catalog=catalog, client=snow_client)
    insert_result = insert_op.insert(
        source_table=source_ns,
        target_table=target_ns.name,
        distinct=distinct
    )

    click.echo(f'Insert Complete - Source Table: {source_ns}, Target Table: {target_ns.name}')
    click.echo(f'Inserted Rows: {insert_result}')
    snow_client.session.close()


merge.add_command(upsert)
merge.add_command(insert)
