import click
from typing import Any, Optional

from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    TimestampType,
    TimeType,
    VariantType,
)


class VarcharSizeType(StringType):
    """
    Type class for holding VARCHAR/StringType() types with max character length parameter.

    https://docs.snowflake.com/en/sql-reference/data-types-text.html#varchar
    """

    def __init__(self, length: Optional[int] = None):
        """
        :param length: Optional integer to enforce VARCHAR column length.
        """
        self._MAX_CHARACTER_LENGTH = 16_777_216

        if isinstance(length, int) and (length <= 0 or length > self._MAX_CHARACTER_LENGTH):
            raise Exception(f'Invalid Character Length: {length}')

        if length == self._MAX_CHARACTER_LENGTH or length is None:
            self.length = None
        else:
            self.length = length

        super(VarcharSizeType, self).__init__()

    def __repr__(self) -> str:
        if self.length:
            return f'VARCHAR({self.length})'
        else:
            return StringType().__repr__()

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, VarcharSizeType):
            return self.length == other.length
        else:
            return isinstance(other, StringType) and self.length is None


class SnowTypeMap:
    def __init__(self):
        self._snowpark_type_map = {
            'VARCHAR': StringType(),
            'STRING': StringType(),
            'NUMBER': DecimalType(38, 0),
            'INTEGER': DecimalType(38, 0),
            'INT': DecimalType(38, 0),
            'DOUBLE': DoubleType(),
            'FLOAT': FloatType(),
            'LONG': LongType(),
            'BIGINT': LongType(),
            'SMALLINT': ShortType(),
            'BYTEINT': ByteType(),
            'BINARY': BinaryType(),
            'BOOLEAN': BooleanType(),
            'DATE': DateType(),
            'DATETIME': TimestampType(),
            'TIME': TimeType(),
            'TIMESTAMP_NTZ': TimestampType(),
            'VARIANT': VariantType(),
            'ARRAY': ArrayType()
        }

        self._snowflake_type_map = {
            StringType: 'VARCHAR',
            IntegerType: 'INTEGER',
            DoubleType: 'DOUBLE',
            FloatType: 'FLOAT',
            LongType: 'BIGINT',
            ByteType: 'BYTEINT',
            BinaryType: 'BINARY',
            ShortType: 'SMALLINT',
            BooleanType: 'BOOLEAN',
            DateType: 'DATE',
            TimeType: 'TIME',
            TimestampType: 'TIMESTAMP_NTZ',
            VariantType: 'VARIANT',
            ArrayType: 'ARRAY'
        }

    def get(self, val: str, error: bool = False) -> Optional[DataType]:
        """
        Get a corresponding SnowPark data type from a string data type name.

        :param val: String name of a data type.
        :param error: Optional flag to raise an exception if data type string cannot be parsed (default False).
        :return: SnowPark data type.
        """
        unknown_type_msg = f'Unknown Data Type: {val}'
        warning_msg = f'{unknown_type_msg} - Setting as StringType'

        # Numeric types with precision & scale parameters
        if '(' in val and ',' in val and ')' in val:
            return self._covert_precision(val.replace(' ', ''))

        # Varchar types with length parameter
        elif '(' in val and ')' in val and val.upper().startswith('VARCHAR'):
            return self._convert_length(val.replace(' ', ''))

        # Other types with arbitrary parameters
        elif '(' in val and ')' in val:
            other_type = self._snowpark_type_map.get(val.upper().split('(')[0].replace(' ', ''))

            if other_type is None:
                if error:
                    raise Exception(unknown_type_msg)
                else:
                    click.echo(warning_msg, err=True)
                    return StringType()

            return other_type

        # Default base type without parameters
        else:
            base_type = self._snowpark_type_map.get(val.upper().replace(' ', ''))

            if base_type is None:
                if error:
                    raise Exception(unknown_type_msg)
                else:
                    click.echo(warning_msg, err=True)
                    return StringType()

            return base_type

    def convert(self, data_type: DataType) -> str:
        """
        Convert a SnowPark data type to its corresponding Snowflake data type.

        :param data_type: Any SnowPark DataType subclass object.
        :return: String representation of the SnowPark data type in Snowflake.
        """
        if isinstance(data_type, DecimalType):
            return f'NUMBER({data_type.precision}, {data_type.scale})'

        if isinstance(data_type, VarcharSizeType):
            return repr(data_type)

        return self._snowflake_type_map.get(type(data_type))

    @staticmethod
    def _covert_precision(str_val: str) -> DecimalType:
        """
        Converts the precision/scale kwargs for a Snowpark DecimalType object.

        :param str_val: String data type name with precision/scale values.
        :return: SnowPark DecimalType.
        """
        type_args = [int(x) for x in str_val[str_val.find('(') + 1:str_val.rfind(')')].split(',')]
        return DecimalType(*type_args)

    @staticmethod
    def _convert_length(str_val: str) -> VarcharSizeType:
        """
        :param str_val: String data type name with VARCHAR(n) length value.
        :return: VarcharSizeType
        """
        len_arg = int(str_val[str_val.find('(') + 1:str_val.rfind(')')])
        return VarcharSizeType(length=len_arg)
