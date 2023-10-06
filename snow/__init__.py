import sys

import click

from .client import SnowClient
from .config.commands import configure
from .config.manager import ConfigManager
from .config.settings import DEFAULT_NAME, DEFAULT_CONFIG_PATH, CONFIG_SUB_COMMAND
from .schema.catalog import SnowCatalog
from .loading.commands import load
from .schema.commands import create_table, alter_table
from .merge.commands import merge
from .export.commands import export
from .logger import get_logger

LOGGER = get_logger()


@click.group(help='Snow(ball) CLI.')
@click.option('--profile', default=DEFAULT_NAME, help='Specify a profile name. Uses "default" otherwise.')
@click.option('--config-path', default=DEFAULT_CONFIG_PATH, type=click.Path(dir_okay=True), help='Path CLI config file',
              show_default=True)
@click.version_option('1.0')
@click.pass_context
def entry_point(ctx, profile: str, config_path: str):
    if ctx.invoked_subcommand != CONFIG_SUB_COMMAND and '--help' not in sys.argv[1:]:
        snow_conf = ConfigManager(profile_name=profile, base_path=config_path).read()
        if not snow_conf:
            ctx.abort()

        ctx.obj = snow_conf


@entry_point.command(help='Test the current Snowflake connection')
@click.pass_context
def connect(ctx):
    test_client = SnowClient(ctx.obj)
    role = test_client.session.get_current_role()
    current_db = test_client.session.get_current_database()
    current_schema = test_client.session.get_current_schema()

    click.echo(
        'Valid Snowflake Connection \n'
        f'User: {ctx.obj.user}, \n'
        f'Account: {ctx.obj.account}, \n'
        f'Role: {role}, \n'
        f'Database: {current_db}, \n'
        f'Schema: {current_schema} \n'
    )

    test_client.session.close()


entry_point.add_command(configure)
entry_point.add_command(connect)
entry_point.add_command(load)
entry_point.add_command(create_table)
entry_point.add_command(alter_table)
entry_point.add_command(merge)
entry_point.add_command(export)

if __name__ == '__main__':
    entry_point()
