import click

from snow.client import SnowClient
from snow.schema.catalog import SnowCatalog
from snow.schema.inspector import SchemaInspector
from snow.utility.cli_utils import TableNamespace, setup_table_namespace
from snow.utility.ddl_utils import SnowDDLUtil


@click.command(help='Create a Snowflake table based on a schema definition')
@click.option('--catalog', help='Catalog file path or JSON string', required=True, type=click.Path(dir_okay=True))
@click.option('--table', help='Fully qualified table name: database.schema.table', required=True)
@click.option('--is_stage', is_flag=True, help='Optional flag for creating a staging table', show_default=True)
@click.pass_context
def create_table(ctx, catalog: str, table: str, is_stage: bool):
    snow_client = SnowClient(ctx.obj)
    catalog = SnowCatalog(catalog_data=catalog)

    new_table = setup_table_namespace(
        client=snow_client,
        table=TableNamespace(table),
        catalog=catalog,
        staging=is_stage
    )

    click.echo(f'New Table Created: {new_table}')
    snow_client.session.close()


@click.command(help='ALTER table/column subcommand')
@click.option('--catalog', help='Catalog file path or JSON string', required=True, type=click.Path(dir_okay=True))
@click.option('--table', help='Fully qualified table name: database.schema.table', required=True)
@click.option('--is_stage', help='Optional flag for operating on the staging table', is_flag=True)
@click.option('--cdc', help='CDC flag', is_flag=True)
@click.option('--dry', is_flag=True, help='Dry-run option, display alters without executing any SQL')
@click.pass_context
def alter_table(ctx, catalog: str, table: str, is_stage: bool, cdc: bool, dry: bool):
    snow_client = SnowClient(ctx.obj)
    catalog = SnowCatalog(catalog_data=catalog)
    ddl_util = SnowDDLUtil(snow_client)

    # table & schema setup
    target_tbl_name = setup_table_namespace(
        client=snow_client,
        table=TableNamespace(table),
        catalog=catalog,
        staging=is_stage,
        cdc=cdc
    )

    # inspect user schema & current table schema and gather changes
    table_df = snow_client.session.table(target_tbl_name)
    schema_inspector = SchemaInspector(catalog=catalog, curr_table_schema=table_df.schema)
    deleted_fields = schema_inspector.get_deleted_fields()
    new_fields = schema_inspector.get_new_fields()
    null_change_fields = schema_inspector.get_nullability_fields()
    tag_change_fields = schema_inspector.get_tagged_fields(
        table_tags=ddl_util.get_table_tags(table_name=target_tbl_name)
    )

    type_change_fields = schema_inspector.get_type_change_fields(
        table_data_types=ddl_util.get_table_data_types(table_name=target_tbl_name)
    )

    # table change logging messages
    deleted_fields_msg = ', '.join([f.name for f in deleted_fields.fields]) if deleted_fields else None
    new_fields_msg = ', '.join([f.name for f in new_fields.fields]) if new_fields else None
    null_change_msg = ', '.join(
        [f"{f.name} nullable={f.nullable}" for f in null_change_fields.fields]) if null_change_fields else None

    if tag_change_fields:
        tagged_fields_msg = ', '.join(
            [f'{field} tag={tag}' if op else f'{field} unset={tag}' for field, tag, op in tag_change_fields]
        )
    else:
        tagged_fields_msg = None

    if type_change_fields:
        type_change_fields_msg = ', '.join([f'{field} type={ty}' for field, ty in type_change_fields.items()])
    else:
        type_change_fields_msg = None

    alter_log_result = (
        f'Snow ALTER Results: \n'
        f'Data Type Changes: {type_change_fields_msg} \n'
        f'Added Columns: {new_fields_msg} \n'
        f'Deleted Columns: {deleted_fields_msg} \n'
        f'Null Constraint Changes: {null_change_msg} \n'
        f'Column Tag Changes: {tagged_fields_msg} \n'
    )

    if dry is False:
        # execute sql ALTER statements if not dry run
        if type_change_fields:
            ddl_util.alter_column_types(table_name=target_tbl_name, new_data_types=type_change_fields)

        if new_fields:
            ddl_util.add_columns(table_name=target_tbl_name, add_cols=new_fields)

        if deleted_fields:
            ddl_util.drop_columns(table_name=target_tbl_name, drop_cols=deleted_fields)

        if null_change_fields:
            ddl_util.alter_nullability(table_name=target_tbl_name, null_cols=null_change_fields)

        if tag_change_fields:
            ddl_util.alter_column_tags(table_name=target_tbl_name, tag_cols=tag_change_fields)

    # display any changes with user schema & table schema
    click.echo(alter_log_result)

    snow_client.session.close()
