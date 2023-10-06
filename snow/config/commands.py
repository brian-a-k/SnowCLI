import click

from .manager import ConfigManager, _parse_raw_config
from .settings import DEFAULT_NAME, DEFAULT_CONFIG_PATH, DEFAULT_SNOWFLAKE_ACCOUNT


@click.group(help='Configure the CLI.')
def configure() -> click.command:
    pass


@configure.command('remote', help='Profile with Snowflake creds stored on AWS')
@click.option('--profile-name', prompt=True, help='CLI profile name', default=DEFAULT_NAME, show_default=True)
@click.option('--config-path', required=False, help='Config file path', default=DEFAULT_CONFIG_PATH,
              type=click.Path(dir_okay=True))
@click.option('--user', prompt=True, help='Snowflake user')
@click.option('--account', prompt=True, help='Snowflake account', default=DEFAULT_SNOWFLAKE_ACCOUNT, show_default=True)
@click.option('--warehouse', prompt=True, help='Snowflake warehouse')
@click.option('--role', prompt=True, help='Snowflake role', default=DEFAULT_NAME, show_default=True)
@click.option('--database', prompt=True, help='Snowflake database')
@click.option('--schema', prompt=True, help='Snowflake schema')
@click.option('--private_key_secret', prompt=True, help='AWS secret key ID for Snowflake PEM')
@click.option('--passphrase_secret', prompt=True, help='AWS secret key for PEM passphrase')
@click.option('--aws-profile', prompt=True, help='AWS named profile', default=DEFAULT_NAME, show_default=True)
def remote(profile_name: str, config_path: str, **conf_kwargs) -> None:
    manager = ConfigManager(profile_name=profile_name, base_path=config_path)
    manager.write(**conf_kwargs)
    click.echo(f'Saved Remote Snow Profile: {profile_name}')


@configure.command('local', help='Profile with local Snowflake creds')
@click.option('--profile-name', prompt=True, help='CLI profile name', default=DEFAULT_NAME, show_default=True)
@click.option('--config-path', required=False, help='Config file path', default=DEFAULT_CONFIG_PATH,
              type=click.Path(dir_okay=True))
@click.option('--user', prompt=True, help='Snowflake user')
@click.option('--account', prompt=True, help='Snowflake account', default=DEFAULT_SNOWFLAKE_ACCOUNT, show_default=True)
@click.option('--warehouse', prompt=True, help='Snowflake warehouse')
@click.option('--role', prompt=True, help='Snowflake role', default=DEFAULT_NAME, show_default=True)
@click.option('--database', prompt=True, help='Snowflake database')
@click.option('--schema', prompt=True, help='Snowflake schema')
@click.option('--private_key_path', prompt=True, help='Local path for Snowflake PEM')
@click.option('--private_key_passphrase', prompt=True, help='Snowflake PEM passphrase')
@click.option('--aws-profile', prompt=True, help='AWS named profile', default=DEFAULT_NAME, show_default=True)
def local(profile_name: str, config_path: str, **conf_kwargs) -> None:
    manager = ConfigManager(profile_name=profile_name, base_path=config_path)
    manager.write(**conf_kwargs)
    click.echo(f'Saved Local Snow Profile: {profile_name}')


@configure.command('set', help='Profile with raw Snowflake creds as JSON file or JSON string')
@click.option('--profile-name', required=False, help='CLI profile name', default=DEFAULT_NAME)
@click.option('--config-path', required=False, help='Config file path', default=DEFAULT_CONFIG_PATH)
@click.option('--config', required=True, help='Snowflake config JSON (file path or string)')
def conf_set(profile_name: str, config_path: str, config: str):
    manager = ConfigManager(profile_name=profile_name, base_path=config_path)
    conf_val = _parse_raw_config(config)
    manager.write(**conf_val)
    click.echo(f'Saved Snow Profile: {profile_name}')


configure.add_command(remote)
configure.add_command(local)
configure.add_command(conf_set)
