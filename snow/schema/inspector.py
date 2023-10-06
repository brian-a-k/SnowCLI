from typing import Optional, Dict, List, Tuple

from snowflake.snowpark.types import StructType, DataType, DecimalType, StringType
from snow.schema.snow_types import VarcharSizeType

from snow.schema.catalog import SnowCatalog


class SchemaInspector:
    """
    Class for comparing current snowflake table schema and passed schema definition.

    - New columns added
    - Deleted/removed fields
    - Null constraints
    - Set/Unset Column Tags
    """

    def __init__(self, catalog: SnowCatalog, curr_table_schema: StructType) -> None:
        """
        :param catalog: Snow Catalog object defining target user schema.
        :param curr_table_schema: Snowpark StructType schema for an existing Snowflake table.
        """
        # for compatibility with SnowBall tables
        self._audit_field_names = [name.upper() for name, _, _, _ in catalog.audit_schema]

        # Snowpark StringType synonymous with snow VarcharSizeType()
        self._string_types = (VarcharSizeType, StringType)

        self.user_schema = {f.name: f for f in catalog.snowpark_schema(upper_case=True).fields}
        self.user_tags = catalog.column_tags
        self.user_data_types = catalog.column_type_map
        self.table_schema = {
            f.name: f for f in curr_table_schema.fields if f.name.upper() not in self._audit_field_names
        }

    def get_new_fields(self) -> Optional[StructType]:
        """
        :return: StructType with any new fields based on the passed schema definition, or None if no new fields found
        """
        new_cols = [field for field_name, field in self.user_schema.items() if field_name not in self.table_schema]

        if not new_cols:
            return None

        return StructType(new_cols)

    def get_deleted_fields(self) -> Optional[StructType]:
        """
        :return: StructType with any removed fields based on the passed schema definition, or None
        """
        drop_cols = [field for field_name, field in self.table_schema.items() if field_name not in self.user_schema]

        if not drop_cols:
            return None

        return StructType(drop_cols)

    def get_nullability_fields(self) -> Optional[StructType]:
        """
        :return: StructType of fields with modified nullability constraints, or None
        """
        null_change_cols = []

        for field_name, schema_field in self.user_schema.items():
            tbl_field = self.table_schema.get(field_name)

            if tbl_field is not None:
                if schema_field.nullable != tbl_field.nullable:
                    null_change_cols.append(schema_field)

        if not null_change_cols:
            return None

        return StructType(null_change_cols)

    def get_tagged_fields(
            self,
            table_tags: Optional[Dict[str, str]] = None
    ) -> Optional[List[Tuple[str, str, bool]]]:
        """
        - (field_name, tag name, True): Set tag on the column.
        - (field_name, tag name, False): Unset tag on the column.

        :param table_tags: Dict of column name: tag name for any existing tags on the table columns.
        :return: List of tuples for any modified tag changes, or None
        """
        # Table columns have current tags
        if table_tags is not None:
            tag_change_cols = []

            for field_name, user_tag in self.user_tags.items():
                tbl_tag = table_tags.get(field_name)

                if user_tag != tbl_tag:
                    # Set tag for column with no existing tag
                    if user_tag is not None and tbl_tag is None:
                        tag_change_cols.append((field_name, user_tag, True))

                    # Unset existing tag for column
                    elif user_tag is None and tbl_tag is not None:
                        tag_change_cols.append((field_name, tbl_tag, False))

                    # Set new tag for a column with an existing tag (unset current tag then set new tag)
                    else:
                        tag_change_cols.append((field_name, tbl_tag, False))
                        tag_change_cols.append((field_name, user_tag, True))

        # No current tags on table columns just return user tags
        else:
            tag_change_cols = [
                (field_name, user_tag, True) for field_name, user_tag in self.user_tags.items() if user_tag is not None
            ]

        if not tag_change_cols:
            return None

        return tag_change_cols

    def get_type_change_fields(self, table_data_types: Dict[str, DataType]) -> Optional[Dict[str, DataType]]:
        """
        :param table_data_types: Dict of column names and Snow parsed data-types.
        :return: Dict of column names and new data types, or None.
        """
        # column ordering might cause this to be false
        if self.user_data_types == table_data_types:
            return None

        type_change_fields = {}

        for field_name, user_type in self.user_data_types.items():
            current_type = table_data_types.get(field_name)

            if user_type != current_type:

                # comparing varchar/string columns
                if isinstance(user_type, self._string_types) and isinstance(current_type, self._string_types):
                    len_error_msg = f'Reducing VARCHAR length is not supported - Column: {field_name}'

                    # user type is snowpark StringType and table type is VarcharSizeType - Supported
                    if not isinstance(user_type, VarcharSizeType) and isinstance(current_type, VarcharSizeType):
                        type_change_fields[field_name] = user_type

                    # user type is VarcharSizeType and table type is snowpark StringType - Unsupported
                    elif isinstance(user_type, VarcharSizeType) and not isinstance(current_type, VarcharSizeType):
                        raise Exception(len_error_msg)

                    # user type is varchar max length and table type is shorter - Supported
                    elif user_type.length is None and current_type.length is not None:
                        type_change_fields[field_name] = user_type

                    # user type is length enforced table type is varchar max length - Unsupported
                    elif user_type.length is not None and current_type.length is None:
                        raise Exception(len_error_msg)

                    # increase varchar length
                    elif user_type.length > current_type.length:
                        type_change_fields[field_name] = user_type
                    else:
                        raise Exception(len_error_msg)

                # increase/decrease precision for numeric columns
                elif isinstance(user_type, DecimalType) and isinstance(current_type, DecimalType):
                    if user_type.precision != current_type.precision:
                        type_change_fields[field_name] = user_type
                    else:
                        raise Exception(f'Changing numeric scale is not supported - Column: {field_name}')

                # ignore new/added columns
                elif current_type is None:
                    pass

                # catch-all for unsupported data-type changes
                else:
                    raise Exception(
                        f'Changing data type {current_type} to {user_type} is not supported - Column {field_name}'
                    )

        if not type_change_fields:
            return None

        return type_change_fields
