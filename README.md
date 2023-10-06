# Snow CLI

CLI utility library for Snowflake operations

## Developer Setup

This repo relies on the following requirements:

- **Python Version 3.8+** (required for snowflake-snowpark)
- https://github.com/snowflakedb/snowpark-python

```
boto3==1.15.18
click==8.1.3
pydantic==1.9.1
cryptography==3.2.1
pydantic==1.9.1
pydantic-yaml==0.8.0
PyYAML==6.0.0
snowflake-snowpark-python==1.4.0
```

## Local Setup

- For local usage run: `snow configure local`
- This will provide prompts for setting up a local connection to snowflake with your personal RSA key.
- You can also include all the required options below without going through the prompts.

```
Options:
  --profile-name TEXT            CLI profile name  [default: default]
  --user TEXT                    Snowflake user
  --account TEXT                 Snowflake account
  --warehouse TEXT               Snowflake warehouse
  --role TEXT                    Snowflake role (Optinoal: uses the default role for the user if not passed)
  --database TEXT                Snowflake database
  --schema TEXT                  Snowflake schema
  --private_key_path TEXT        Local path for Snowflake PEM
  --private_key_passphrase TEXT  Snowflake PEM passphrase
```

## Remote Setup

- For remote usage run: `snow configure remote`
- A remote profile sets the RSA Key/Passphrase from secrets stored in AWS secrets manager.
- **AWS access is required to set up/use a remote profile.**
- You can also include all the required options below without going through the prompts.
- **Note that the `private_key_secret` & `passphrase_secret` parameters are the Secret key names in AWS secrets
  manager.**
- The `aws-profile` parameter is the named AWS profile to use when retrieving secrets, if not passed it will
  use `default`.

```
Options:
  --profile-name TEXT        CLI profile name  [default: default]
  --user TEXT                Snowflake user
  --account TEXT             Snowflake account 
  --warehouse TEXT           Snowflake warehouse
  --role TEXT                Snowflake role (Optinoal: uses the default role for the user if not passed)
  --database TEXT            Snowflake database
  --schema TEXT              Snowflake schema
  --private_key_secret TEXT  AWS secret key ID for Snowflake PEM
  --passphrase_secret TEXT   AWS secret key for PEM passphrase
  --aws-profile TEXT         AWS named profile  [default: default]
```

## JSON Config Setup

- A Snow profile can be set with a JSON file path or JSON string passed in the CLI.
- Example with a raw JSON string:

```
snow configure set --config '{"user":"default_user","account":"default_account","warehouse":"default_wh","role":"default_role","database":"default_db","schema":"default_schema","private_key_secret": "default_key","passphrase_secret":"default_pw","aws_named_profile":"default"}'
```

- Example with a json file:

```
snow configure set --config path/my_config.json
```

- **This command can be used when running in a Docker container.**

## Test Snowflake Connection

- **Run at least of the above configuration steps first.**
- Run snow `snow connect` (using default profile) or `snow --profile <profile name> connect` (using a named profile).
- Valid connection output:

```
Valid Snowflake Connection 
User: brian.kalinowski@gmail.com, 
Account: briank.us-east-1, 
Role: "PLATFORM_ENGINEER", 
Database: "MY_DB", 
Schema: "MY_SCHEMA" 
```

## Data Loading

- Snow supports loading files in CSV, JSON, or PARQUET formats.
- Each file format has its own sub-command
    - `snow load csv ...`
    - `snow load json ...`
    - `snow load parquet ...`

The CLI options for each file format type are the same:

```
Options:
  --catalog PATH      Catalog file path or JSON string  [required]
  --stage TEXT        Snowflake named external stage  [required]
  --table TEXT        Fully qualified table name: database.schema.table  [required]
  --prefix TEXT       S3 Prefix for staged data files  [required]
  --is_stage          Optional flag for loading into the staging table
  --file_format TEXT  Named file format in Snowflake
```

- The `--file_format` option is not required, but if not passed snow will use the default file format configured for the
  passed external stage.
- **It's recommended to always pass a named file-format that is suited to the data being loaded.**
- If the `--is_stage` parameter is pass the staging table will be cleared before loading new data.

## Table Alters

- Snow supports running some `ALTER TABLE` & `ALTER COLUMN` statements via the `alter-table` sub-command.
- Supported Alters are: *data type changes*, Adding new columns, removing columns, modifying null constraints, and
  modifying column tags.
- **Supported data type changes: increasing varchar max length, increasing or decreasing the precision of numeric
  columns.**
- Snow compares the passed schema with the current table schema to identify changes.
- The `--dry` option flag is useful for identifying schema/table differences **without running any SQL statements**
- The `--cdc` option flag ignores audit columns for a table for use with CDC streams.

```
Usage: snow alter-table [OPTIONS]

  ALTER table/column subcommand

Options:
  --catalog PATH   Catalog file path or JSON string  [required]
  --table TEXT     Fully qualified table name: database.schema.table  [required]
  --is_stage       Optional flag for operating on the staging table
  --cdc            CDC flag
  --dry            Dry-run option, display alters without executing any SQL
```

### Alter Limitations

- Adding new columns with a **non-null constraint** to a **non-empty table** is not supported.
- Modifying a nullable field to non-nullable **if the field currently contains null values** is not supported.
- Changing a column data type to a different type is not supported.
- Changing the scale of a numeric column is not supported.
- See https://docs.snowflake.com/en/sql-reference/sql/alter-table-column.html for more info on Snowflakes ALTER
  limitations.

## Merge Patterns

- 2 main merge patterns are supported
- **Assumes both the base/target table and source/staging table exist.**

```
Usage: snow merge [OPTIONS] COMMAND [ARGS]...

  Data Merge Sub Command

Commands:
  insert  Run and INSERT only pattern for full-refresh data loads
  upsert  Run an UPSERT merge pattern for incremental data loads
    
```

### Upsert Merge Pattern

```
Usage: snow merge upsert [OPTIONS]

  Run an UPSERT merge pattern for incremental data loads

Options:
  --catalog   Catalog file path or JSON string  [required]
  --target    Fully qualified table namespace for target/base table [required]
  --source    Optional source/staging table, inferred if not passed
  --cdc       CDC flag
  --distinct  SELECT DISTINCT * flag for the source/staging table
```

- This command runs an update/insert (upsert) into the target table from the source table
- Only the `--catalog` & `--target` parameters are required. The `--source` table can be inferred from the name of the
  target table if not passed
- **The `--target` table parameter must be the fully qualified table name: `DATABASE.SCHEMA.TABLE`**'
- The `--cdc` option flag enables a Stream on the table for tracking CDC.
- The `--distinct` option flag uses **only distinct rows (full rows)** from the source/staging table. Used for
  preventing duplicate rows in the base table.

### Insert Merge Pattern

```
Usage: snow merge insert [OPTIONS]

  Run and INSERT only pattern for full-refresh data loads

Options:
  --catalog   Catalog file path or JSON string  [required]
  --target    Fully qualified table namespace for target/base table [required]
  --source    Optional source/staging table, inferred if not passed
  --distinct  SELECT DISTINCT * flag for the source/staging table
```

- This command runs just an insert into the target table from the source table
- Only the `--catalog` & `--target` parameters are required. The `--source` table can be inferred from the name of the
  target table if not passed (assumes our staging pattern `DATABASE.STAGING.table_name`).
- **The `--target` table parameter must be the fully qualified table name: `DATABASE.SCHEMA.TABLE`**
- The `--distinct` option flag uses **only distinct rows (full rows)** from the source/staging table. Used for
  preventing duplicate rows in the base table.

## Exporting Data

- Snow supports exporting/unloading data from snowflake to an external stage (S3).
- Tables and CDC streams can be exported with the `snow export` subcommand.

```
Options:
  --catalog PATH      Catalog file path or JSON string  [required]
  --table TEXT        Fully qualified table name: database.schema.table [required]
  --stage TEXT        Snowflake named external stage  [required]
  --xcom / --no-xcom  Flag for XCOM output
```

## Docker

- To create a snow docker image run: `docker build -t <image name>:<tag id> .` in the root directory of this repo.
- The snow docker image is created with Ubuntu Linux with Python 3.8.13 and all required Python libraries.

Example:

```
docker build -t snow_cli:1.0.0 .
```

### Running with Docker locally

- A Snow profile can be set in the docker image with a raw JSON string (see JSON Config Setup above).
- You can use your local Snow profiles in a docker container by mounting the local `~/.snow` directory and your
  Snowflake RSA key:

```
docker run -it -v $(pwd)/.snow/:/root/.snow/ -v <LOCAL RSA KEY PATH>:/<CONTAINER KEY PATH> snow_cli:1.0.0 bash
```

- Using a remote Snow profile (RSA credentials stored in AWS secrets manager) in a docker container requires mounting
  AWS credentials:

```
docker run -it -v $(pwd)/.aws/:/root/.aws/ snow_cli:1.0.0 bash
```
