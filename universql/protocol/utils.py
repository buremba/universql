import typing
from traceback import print_exc

import duckdb
import pyarrow as pa
import sqlglot
from pyarrow import ChunkedArray, Table
from snowflake.connector.constants import FIELD_TYPES, FIELD_NAME_TO_ID
from snowflake.connector.cursor import ResultMetadataV2

from universql.util import SnowflakeError

class DuckDBFunctions:
    @staticmethod
    def register(db: duckdb.DuckDBPyConnection):
        db.create_function("CURRENT_WAREHOUSE", DuckDBFunctions.current_warehouse, [], duckdb.typing.VARCHAR)
        pass

    @staticmethod
    def current_warehouse() -> str:
        return "x-duck"


def get_field_for_snowflake(column: ResultMetadataV2, value: typing.Optional[pa.Array] = None) -> \
        typing.Tuple[
            pa.Field, pa.Array]:
    arrow_field = FIELD_TYPES[column.type_code]

    metadata = {
        "logicalType": arrow_field.name,
        "charLength": "8388608",
        "byteLength": "8388608",
    }

    if arrow_field.name == "GEOGRAPHY":
        metadata["logicalType"] = "OBJECT"

    precision = column.precision
    scale = column.precision

    if arrow_field.name == 'FIXED':
        ### Rmove this line
        if value is not None and pa.types.is_integer(value.type):
            pa_type = value.type
        else:
            ###
            pa_type = pa.decimal128(column.precision
                                    # or value.type.precision
                                    or 38,
                                    column.scale
                                    # or value.type.precision
                                    or 0)
        precision = precision or 1
        scale = scale or 0
        if value is not None:
            try:
                value = value.cast(pa_type, safe=False)
            except Exception as e:
                raise e
    elif arrow_field.name == 'DATE':
        pa_type = pa.date32()
        if value is not None:
            value = value.cast(pa_type)
    elif arrow_field.name == 'TIME':
        pa_type = pa.int64()
        if value is not None:
            value = value.cast(pa_type)
    elif arrow_field.name == 'TIMESTAMP_LTZ' or arrow_field.name == 'TIMESTAMP_NTZ' or arrow_field.name == 'TIMESTAMP':
        metadata["final_type"] = "T"
        timestamp_fields = [
            pa.field("epoch", nullable=False, type=pa.int64(), metadata=metadata),
            pa.field("fraction", nullable=False, type=pa.int32(), metadata=metadata),
        ]
        pa_type = pa.struct(timestamp_fields)
        if value is not None:
            epoch = pa.compute.divide(value.cast(pa.int64()), 1_000_000_000).combine_chunks()
            value = pa.StructArray.from_arrays(arrays=[epoch, pa.nulls(len(value), type=pa.int32())],
                                               fields=timestamp_fields)
    elif arrow_field.name == 'TIMESTAMP_TZ':
        metadata["final_type"] = "T"
        timestamp_fields = [
            pa.field("epoch", nullable=False, type=pa.int64(), metadata=metadata),
            pa.field("fraction", nullable=False, type=pa.int32(), metadata=metadata),
            pa.field("timezone", nullable=False, type=pa.int32(), metadata=metadata),
        ]
        pa_type = pa.struct(timestamp_fields)
        if value is not None:
            epoch = pa.compute.divide(value.cast(pa.int64()), 1_000_000_000).combine_chunks()

            value = pa.StructArray.from_arrays(
                arrays=[epoch,
                        # TODO: modulos 1_000_000_000 to get the fraction of a second, pyarrow doesn't support the operator yet
                        pa.nulls(len(value), type=pa.int32()),
                        # TODO: reverse engineer the timezone conversion
                        pa.nulls(len(value), type=pa.int32()),
                        ],
                fields=timestamp_fields)
    else:
        pa_type = arrow_field.pa_type(column)

    if precision is not None:
        metadata["precision"] = str(precision)
    if scale is not None:
        metadata["scale"] = str(scale)

    field = pa.field(column.name, type=pa_type, nullable=column.is_nullable, metadata=metadata)
    return (field, value)


def arrow_to_snowflake_type_id(column_type: pa.DataType):
    if pa.types.is_decimal(column_type) or pa.types.is_integer(column_type):
        type_code = FIELD_NAME_TO_ID["FIXED"]
    elif pa.types.is_date(column_type):
        type_code = FIELD_NAME_TO_ID["DATE"]
    elif pa.types.is_floating(column_type):
        type_code = FIELD_NAME_TO_ID["REAL"]
    elif pa.types.is_timestamp(column_type):
        # Check if it should be TIMESTAMP_TZ
        type_code = FIELD_NAME_TO_ID["TIMESTAMP"]
    elif pa.types.is_boolean(column_type):
        type_code = FIELD_NAME_TO_ID["BOOLEAN"]
    elif pa.types.is_string(column_type):
        type_code = FIELD_NAME_TO_ID["TEXT"]
    elif pa.types.is_struct(column_type):
        # Check if it should be MAP
        type_code = FIELD_NAME_TO_ID["VARIANT"]
    elif pa.types.is_list(column_type):
        type_code = FIELD_NAME_TO_ID["ARRAY"]
    elif pa.types.is_binary(column_type):
        type_code = FIELD_NAME_TO_ID["BINARY"]
    elif pa.types.is_time(column_type):
        type_code = FIELD_NAME_TO_ID["TIME"]
    else:
        # Unsupported types: VECTOR, GEOMETRY, GEOGRAPHY
        raise ValueError(f"Unsupported type: {column_type}")

    return type_code


def get_field_from_duckdb(column: list[str], arrow_table: Table, idx: int) -> typing.Tuple[
    typing.Optional[ChunkedArray], pa.Field]:
    (field_name, field_type) = column[0], column[1]
    pa_type = arrow_table.schema[idx].type

    metadata = {}
    value = arrow_table[idx]

    if field_type == 'NUMBER':

        if (  # no harm for int types
                pa_type != pa.int64() and
                pa_type != pa.int32() and
                pa_type != pa.int16() and
                pa_type != pa.int8()):
            pa_type = pa.decimal128(getattr(value.type, 'precision', 38), getattr(value.type, 'scale', 0))
        try:
            value = value.cast(pa_type)
        except Exception as e:
            pass
        metadata["logicalType"] = "FIXED"
        metadata["precision"] = "1"
        metadata["scale"] = "0"
        metadata["physicalType"] = "SB1"
        metadata["final_type"] = "T"
    elif field_type == 'Date':
        pa_type = pa.date32()
        value = value.cast(pa_type)
        metadata["logicalType"] = "DATE"
    elif field_type == 'Time':
        pa_type = pa.int64()
        value = value.cast(pa_type)
        metadata["logicalType"] = "TIME"
    elif field_type == "BINARY":
        pa_type = pa.binary()
        metadata["logicalType"] = "BINARY"
    elif field_type == "TIMESTAMP" or field_type == "DATETIME" or field_type == "TIMESTAMP_LTZ":
        metadata["logicalType"] = "TIMESTAMP_LTZ"
        metadata["precision"] = "0"
        metadata["scale"] = "9"
        metadata["physicalType"] = "SB16"
        metadata["final_type"] = "T"
        timestamp_fields = [
            pa.field("epoch", nullable=False, type=pa.int64(), metadata=metadata),
            pa.field("fraction", nullable=False, type=pa.int32(), metadata=metadata),
        ]
        pa_type = pa.struct(timestamp_fields)
        epoch = pa.compute.divide(value.cast(pa.int64()), 1_000_000_000).combine_chunks()
        value = pa.StructArray.from_arrays(arrays=[epoch, pa.nulls(len(value), type=pa.int32())],
                                           fields=timestamp_fields)
    elif field_type == "TIMESTAMP_NTZ":
        metadata["logicalType"] = "TIMESTAMP_NTZ"
        metadata["precision"] = "0"
        metadata["scale"] = "9"
        metadata["physicalType"] = "SB16"
        timestamp_fields = [
            pa.field("epoch", nullable=False, type=pa.int64(), metadata=metadata),
            pa.field("fraction", nullable=False, type=pa.int32(), metadata=metadata),
        ]
        pa_type = pa.struct(timestamp_fields)
        epoch = pa.compute.divide(value.cast(pa.int64()), 1_000_000_000).combine_chunks()
        value = pa.StructArray.from_arrays(arrays=[epoch, pa.nulls(len(value), type=pa.int32())],
                                           fields=timestamp_fields)
    elif field_type == "TIMESTAMP_TZ":
        timestamp_fields = [
            pa.field("epoch", nullable=False, type=pa.int64(), metadata=metadata),
            pa.field("fraction", nullable=False, type=pa.int32(), metadata=metadata),
            pa.field("timezone", nullable=False, type=pa.int32(), metadata=metadata),
        ]
        pa_type = pa.struct(timestamp_fields)
        epoch = pa.compute.divide(value.cast(pa.int64()), 1_000_000_000).combine_chunks()

        value = pa.StructArray.from_arrays(
            arrays=[epoch,
                    # TODO: modulos 1_000_000_000 to get the fraction of a second, pyarrow doesn't support the operator yet
                    pa.nulls(len(value), type=pa.int32()),
                    # TODO: reverse engineer the timezone conversion
                    pa.nulls(len(value), type=pa.int32()),
                    ],
            fields=timestamp_fields)
        metadata["logicalType"] = "TIMESTAMP_TZ"
        metadata["precision"] = "0"
        metadata["scale"] = "9"
        metadata["physicalType"] = "SB16"
    elif field_type == "JSON":
        pa_type = pa.utf8()
        metadata["logicalType"] = "OBJECT"
        metadata["charLength"] = "16777216"
        metadata["byteLength"] = "16777216"
        metadata["scale"] = "0"
        metadata["precision"] = "38"
        metadata["finalType"] = "T"
    elif pa_type == pa.bool_():
        metadata["logicalType"] = "BOOLEAN"
    elif field_type == 'list':
        pa_type = pa.utf8()
        arrow_to_project = duckdb.from_arrow(arrow_table.select([field_name]))
        metadata["logicalType"] = "ARRAY"
        metadata["charLength"] = "16777216"
        metadata["byteLength"] = "16777216"
        metadata["scale"] = "0"
        metadata["precision"] = "38"
        metadata["finalType"] = "T"
        value = (arrow_to_project.project(f"to_json({field_name})").arrow())[0]
    elif pa_type == pa.string():
        metadata["logicalType"] = "TEXT"
        metadata["charLength"] = "16777216"
        metadata["byteLength"] = "16777216"
    else:
        raise Exception()

    field = pa.field(field_name, type=pa_type, nullable=True, metadata=metadata)
    return value, field
