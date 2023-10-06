import pytest
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

from snow.schema.snow_types import SnowTypeMap, VarcharSizeType


class TestSnowTypeMap:
    test_type_map = SnowTypeMap()

    test_data_types = [
        ('NUMBER(31,9)', DecimalType(31, 9)),
        ('NUMBER(38,4)', DecimalType(38, 4)),
        ('TIMESTAMP_NTZ', TimestampType()),
        ('binary', BinaryType()),
        ('boolean', BooleanType()),
        ('date', DateType()),
        ('datetime', TimestampType()),
        ('integer', DecimalType(38, 0)),
        ('number', DecimalType()),
        ('number(10, 6)', DecimalType(10, 6)),
        ('number(10,6)', DecimalType(10, 6)),
        ('number(16, 2)', DecimalType(16, 2)),
        ('number(18, 4)', DecimalType(18, 4)),
        ('number(18,4)', DecimalType(18, 4)),
        ('number(31,16)', DecimalType(31, 16)),
        ('number(38, 8)', DecimalType(38, 8)),
        ('number(38,1)', DecimalType(38, 1)),
        ('number(7,2)', DecimalType(7, 2)),
        ('varchar', StringType()),
        ('float', FloatType()),
        ('Random', StringType())
    ]

    test_numeric_types = [
        ('number(10, 6)', DecimalType(10, 6)),
        ('number(10,6)', DecimalType(10, 6)),
        ('number(16, 2)', DecimalType(16, 2)),
        ('number(18, 4)', DecimalType(18, 4)),
        ('number(18,4)', DecimalType(18, 4)),
        ('number(31,16)', DecimalType(31, 16)),
        ('number(38, 8)', DecimalType(38, 8)),
        ('number(38,1)', DecimalType(38, 1)),
        ('number(7,2)', DecimalType(7, 2))
    ]

    test_varchar_size_types = [
        ('varchar(1)', VarcharSizeType(1)),
        ('varchar(12)', VarcharSizeType(12)),
        ('varchar(123)', VarcharSizeType(123)),
        ('varchar(1234)', VarcharSizeType(1234)),
        ('varchar(12345)', VarcharSizeType(12345)),
        ('varchar(123456)', VarcharSizeType(123456))
    ]

    test_conversions = [
        (StringType(), 'VARCHAR'),
        (IntegerType(), 'INTEGER'),
        (DoubleType(), 'DOUBLE'),
        (FloatType(), 'FLOAT'),
        (LongType(), 'BIGINT'),
        (ByteType(), 'BYTEINT'),
        (ShortType(), 'SMALLINT'),
        (BooleanType(), 'BOOLEAN'),
        (DateType(), 'DATE'),
        (TimeType(), 'TIME'),
        (TimestampType(), 'TIMESTAMP_NTZ'),
        (VariantType(), 'VARIANT'),
        (ArrayType(), 'ARRAY'),
        (DecimalType(), 'NUMBER(38, 0)'),
        (DecimalType(38, 38), 'NUMBER(38, 38)'),
        (DecimalType(10, 6), 'NUMBER(10, 6)'),
        (VarcharSizeType(), 'StringType()'),
        (VarcharSizeType(1), 'VARCHAR(1)'),
        (VarcharSizeType(10), 'VARCHAR(10)'),
        (VarcharSizeType(123), 'VARCHAR(123)')
    ]

    @pytest.mark.parametrize('str_type,snow_type', test_data_types)
    def test_get(self, str_type: str, snow_type: DataType):
        assert self.test_type_map.get(str_type) == snow_type

    @pytest.mark.parametrize('str_type,snow_type', test_numeric_types)
    def test__covert_precision(self, str_type: str, snow_type: DataType):
        assert self.test_type_map._covert_precision(str_type) == snow_type

    @pytest.mark.parametrize('str_type,snow_type', test_varchar_size_types)
    def test__convert_length(self, str_type: str, snow_type: DataType):
        assert self.test_type_map._convert_length(str_type) == snow_type

    @pytest.mark.parametrize('snow_type,sf_type', test_conversions)
    def test_convert(self, snow_type: DataType, sf_type: str):
        assert self.test_type_map.convert(snow_type) == sf_type


class TestVarcharSizeType:
    def test__init(self):
        test_type_1 = VarcharSizeType()
        test_type_2 = VarcharSizeType(100)

        assert test_type_1._MAX_CHARACTER_LENGTH == 16_777_216
        assert test_type_2._MAX_CHARACTER_LENGTH == 16_777_216

        assert test_type_1.length is None
        assert test_type_2.length == 100

        with pytest.raises(Exception) as exp:
            _ = VarcharSizeType(18_000_000)
        assert exp.type == Exception
        assert str(exp.value) == 'Invalid Character Length: 18000000'

        with pytest.raises(Exception) as exp:
            _ = VarcharSizeType(0)
        assert exp.type == Exception
        assert str(exp.value) == 'Invalid Character Length: 0'

    def test_equals_self(self):
        assert VarcharSizeType() == VarcharSizeType()
        assert VarcharSizeType(100) == VarcharSizeType(100)

        assert VarcharSizeType(100) != VarcharSizeType()
        assert VarcharSizeType(100) != VarcharSizeType(10)
        assert VarcharSizeType(10) != VarcharSizeType(100)

    def test_equals_string_type(self):
        assert VarcharSizeType() == StringType()
        assert StringType() == VarcharSizeType()

        assert VarcharSizeType(length=None) == StringType()
        assert VarcharSizeType(16_777_216) == StringType()
        assert StringType() == VarcharSizeType(length=None)
        assert StringType() == VarcharSizeType(16_777_216)

        assert VarcharSizeType(100) != StringType()
        assert StringType() != VarcharSizeType(100)

    def test__repr(self):
        assert repr(VarcharSizeType()) == repr(StringType())
        assert repr(StringType()) == repr(VarcharSizeType())

        assert repr(VarcharSizeType()) == 'StringType()'
        assert repr(VarcharSizeType(100)) == 'VARCHAR(100)'
        assert repr(VarcharSizeType(1234)) == 'VARCHAR(1234)'
