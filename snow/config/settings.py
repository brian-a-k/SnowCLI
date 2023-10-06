import os
from pathlib import Path

# Default profile to be used (for local or remote connections)
DEFAULT_NAME = 'default'

# Default path where any configuration files are written
# Docker --- /root/.snow
DEFAULT_CONFIG_PATH = os.path.join(str(Path.home()), '.snow')

# Config file name
CONFIG_FILE_NAME = 'snow_config.yml'

# Default Snowflake Account
DEFAULT_SNOWFLAKE_ACCOUNT = ''

# Configuration sub-command
CONFIG_SUB_COMMAND = 'configure'
