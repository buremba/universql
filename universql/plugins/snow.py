import ast
import configparser
import os
import platform
import re

import boto3
import sqlglot
from sqlglot import Expression
from sqlglot.expressions import CopyParameter, Var, Literal, From, Table, Anonymous, Array, Select, Star, Insert, EQ, Column, Identifier

from universql.plugin import UniversqlPlugin, register
from universql.util import full_qualifier, QueryError
from universql.warehouse.duckdb import DuckDBExecutor
from universql.warehouse.snowflake import SnowflakeExecutor, SnowflakeCatalog
from pprint import pp

DEFAULT_CREDENTIALS_LOCATIONS = {
    'Darwin': "~/.aws/credentials",
    'Linux': "~/.aws/credentials",
    'Windows': "%USERPROFILE%\.aws\credentials",
}

DISALLOWED_PARAMS_BY_FORMAT = {
    "JSON": {
        "ignore_errors": ["ALWAYS_REMOVE"],
        "nullstr": ["ALWAYS_REMOVE"],
        "timestampformat": ["AUTO"]
    }
}

REQUIRED_PARAMS_BY_FORMAT = {
    "JSON": {
        "auto_detect": {
            "duckdb_property_type": "BOOL",
            "duckdb_property_value": "TRUE"
        }
    }
}

SNOWFLAKE_TO_DUCKDB_DATETIME_MAPPINGS = {
    'YYYY': '%Y',
    'YY': '%y',
    "MMMM": "%B",
    'MM': '%m',
    'MON': "%b",  # in snowflake this means full or abbreviated; duckdb doesn't have an either or option
    "DD": "%d",
    "DY": "%a",
    "HH24": "%24",
    "HH12": "%I",
    "AM": "%p",
    "PM": "%p",
    "MI": "%M",
    "SS": "%S",
    "FF0": "",
    "FF1": "%g",
    "FF2": "%g",
    "FF3": "%g",
    "FF4": "%f",
    "FF5": "%f",
    "FF6": "%f",
    "FF7": "%n",
    "FF8": "%n",
    "FF9": "%n",
    "TZH:TZM": "%z",
    "TZHTZM": "%z",
    "TZH": "%z",
}

REQUIRED_PARAMS_BY_FORMAT = {
    "JSON": {
        "auto_detect": {
            "duckdb_property_type": "BOOL",
            "duckdb_property_value": "TRUE"
        }
    },
    "PARQUET": {
        "hive_partitioning": {
            "duckdb_property_type": "BOOL",
            "duckdb_property_value": "TRUE"
        },
        "union_by_name": {
            "duckdb_property_type": "BOOL",
            "duckdb_property_value": "TRUE"
        },
    }
}

SNOWFLAKE_TO_DUCKDB_PROPERTY_MAPPINGS = {
    "TYPE": {
        "duckdb_property_name": "format",
        "duckdb_property_type": "VARCHAR"
    },
    "RECORD_DELIMITER": {
        "duckdb_property_name": 'new_line',
        "duckdb_property_type": "VARCHAR"
    },
    "FIELD_DELIMITER": {
        "duckdb_property_name": "delim",
        "duckdb_property_type": "VARCHAR"
    },
    "FILE_EXTENSION": {
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "SKIP_HEADER": {
        "duckdb_property_name": "skip",
        "duckdb_property_type": "BIGINT"
    },
    "PARSE_HEADER": {
        "duckdb_property_name": "header",
        "duckdb_property_type": "BOOL"
    },
    "DATE_FORMAT": {
        "duckdb_property_name": "dateformat",
        "duckdb_property_type": "VARCHAR"
    },
    "TIME_FORMAT": {
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "TIMESTAMP_FORMAT": {
        "duckdb_property_name": "timestampformat",
        "duckdb_property_type": "VARCHAR"
    },
    "BINARY_FORMAT": {
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "ESCAPE": {
        "duckdb_property_name": "escape",
        "duckdb_property_type": "VARCHAR"
    },
    "ESCAPE_UNENCLOSED_FIELD": {
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "TRIM_SPACE": {
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "FIELD_OPTIONALLY_ENCLOSED_BY": {
        "duckdb_property_name": "quote",
        "duckdb_property_type": "VARCHAR"
    },
    "NULL_IF": {
        "duckdb_property_name": "nullstr",
        "duckdb_property_type": "VARCHAR[]"
    },
    "COMPRESSION": {
        "duckdb_property_name": "compression",
        "duckdb_property_type": "VARCHAR"
    },
    "ERROR_ON_COLUMN_COUNT_MISMATCH": {
        "duckdb_property_name": "null_padding",
        "duckdb_property_type": "BOOL"
    },
    "VALIDATE_UTF8": {
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "SKIP_BLANK_LINES": {
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "REPLACE_INVALID_CHARACTERS": {
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "EMPTY_FIELD_AS_NULL": {
        "duckdb_property_name": "EMPTY_FIELD_AS_NULL",
        "duckdb_property_type": "SPECIAL_HANDLING"
    },
    "SKIP_BYTE_ORDER_MARK": {
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "ENCODING": {
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "ON_ERROR": {
        "duckdb_property_name": "ignore_errors",
        "duckdb_property_type": "BOOL"
    },
    "SIZE_LIMIT": {
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "PURGE": {
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "RETURN_FAILED_ONLY": {
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "ENFORCE_LENGTH": {
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "TRUNCATECOLUMNS": {
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "FORCE": {
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "URL": {
        "duckdb_property_name": "URL",
        "duckdb_property_type": "METADATA"
    },
    "STORAGE_INTEGRATION": {
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "AWS_ROLE": {
        "duckdb_property_name": "AWS_ROLE",
        "duckdb_property_type": "METADATA"
    },
    "AWS_EXTERNAL_ID": {
        "duckdb_property_name": "AWS_EXTERNAL_ID",
        "duckdb_property_type": "METADATA"
    },
    "SNOWFLAKE_IAM_USER": {
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "ENABLE": {
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "AUTO_REFRESH": {
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "ALLOW_DUPLICATE": {  # duckdb only takes the last value
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "ENABLE_OCTAL": {
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "IGNORE_UTF8_ERRORS": {
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "STRIP_NULL_VALUES": {  # would need to be handled after a successful copy
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "STRIP_OUTER_ARRAY": {  # needs to use json_array_elements() after loading
        "duckdb_property_name": None,
        "duckdb_property_type": None
    }
}

SNOWFLAKE_DEFAULT_COPY_PARAMETERS = {
    'TYPE': {"snowflake_property_value": "String", "snowflake_property_type": "CSV"},
    'RECORD_DELIMITER': {"snowflake_property_value": "String", "snowflake_property_type": "\n"},
    'FIELD_DELIMITER': {"snowflake_property_value": "String", "snowflake_property_type": ","},
    'SKIP_HEADER': {"snowflake_property_value": "Integer", "snowflake_property_type": 0},
    'PARSE_HEADER': {"snowflake_property_value": "Boolean", "snowflake_property_type": "FALSE"},
    'DATE_FORMAT': {"snowflake_property_value": "String", "snowflake_property_type": "AUTO"},
    'TIME_FORMAT': {"snowflake_property_value": "String", "snowflake_property_type": "AUTO"},
    'TIMESTAMP_FORMAT': {"snowflake_property_value": "String", "snowflake_property_type": "AUTO"},
    'BINARY_FORMAT': {"snowflake_property_value": "String", "snowflake_property_type": "HEX"},
    'ESCAPE': {"snowflake_property_value": "String", "snowflake_property_type": "NONE"},
    'ESCAPE_UNENCLOSED_FIELD': {"snowflake_property_value": "String", "snowflake_property_type": "\\"},
    'TRIM_SPACE': {"snowflake_property_value": "Boolean", "snowflake_property_type": "FALSE"},
    'FIELD_OPTIONALLY_ENCLOSED_BY': {"snowflake_property_value": "String", "snowflake_property_type": "NONE"},
    'NULL_IF': {"snowflake_property_value": "List", "snowflake_property_type": "[\\N]"},
    'COMPRESSION': {"snowflake_property_value": "String", "snowflake_property_type": "AUTO"},
    'ERROR_ON_COLUMN_COUNT_MISMATCH': {"snowflake_property_value": "Boolean", "snowflake_property_type": "TRUE"},
    'VALIDATE_UTF8': {"snowflake_property_value": "Boolean", "snowflake_property_type": "TRUE"},
    'SKIP_BLANK_LINES': {"snowflake_property_value": "Boolean", "snowflake_property_type": "FALSE"},
    'REPLACE_INVALID_CHARACTERS': {"snowflake_property_value": "Boolean", "snowflake_property_type": "FALSE"},
    'EMPTY_FIELD_AS_NULL': {"snowflake_property_value": "Boolean", "snowflake_property_type": "TRUE"},
    'SKIP_BYTE_ORDER_MARK': {"snowflake_property_value": "Boolean", "snowflake_property_type": "TRUE"},
    'ENCODING': {"snowflake_property_value": "String", "snowflake_property_type": "UTF8"},
    'ON_ERROR': {"snowflake_property_value": "String", "snowflake_property_type": "ABORT_STATEMENT"},
    'PURGE': {"snowflake_property_value": "Boolean", "snowflake_property_type": "FALSE"},
    'RETURN_FAILED_ONLY': {"snowflake_property_value": "Boolean", "snowflake_property_type": "FALSE"},
    'ENFORCE_LENGTH': {"snowflake_property_value": "Boolean", "snowflake_property_type": "TRUE"},
    'TRUNCATECOLUMNS': {"snowflake_property_value": "Boolean", "snowflake_property_type": "FALSE"},
    'FORCE': {"snowflake_property_value": "Boolean", "snowflake_property_type": "FALSE"},
    'ENABLE': {"snowflake_property_value": "Boolean", "snowflake_property_type": "FALSE"},
    'AUTO_REFRESH': {"snowflake_property_value": "Boolean", "snowflake_property_type": "FALSE"},
    'USE_PRIVATELINK_ENDPOINT': {"snowflake_property_value": "Boolean", "snowflake_property_type": "FALSE"},
    'ENABLE_OCTAL': {"snowflake_property_value": "Boolean", "snowflake_property_type": "FALSE"},
    'ALLOW_DUPLICATE': {"snowflake_property_value": "Boolean", "snowflake_property_type": "FALSE"},
    'STRIP_OUTER_ARRAY': {"snowflake_property_value": "Boolean", "snowflake_property_type": "FALSE"},
    'STRIP_NULL_VALUES': {"snowflake_property_value": "Boolean", "snowflake_property_type": "FALSE"},
    'IGNORE_UTF8_ERRORS': {"snowflake_property_value": "Boolean", "snowflake_property_type": "FALSE"},
    'BINARY_AS_TEXT': {"snowflake_property_value": "Boolean", "snowflake_property_type": "TRUE"},
    'USE_LOGICAL_TYPE': {"snowflake_property_value": "Boolean", "snowflake_property_type": "FALSE"},
    'USE_VECTORIZED_SCANNER': {"snowflake_property_value": "Boolean", "snowflake_property_type": "FALSE"}
}

DUCKDB_SUPPORTED_FILE_TYPES = ['CSV', 'JSON', 'AVRO', 'PARQUET']

DISALLOWED_PARAMS_BY_FORMAT_COPY = {
    "JSON": {
        "ignore_errors": ["ALWAYS_REMOVE"],
        "nullstr": ["ALWAYS_REMOVE"],
        "timestampformat": ["AUTO"],
        "binary_as_string": ["ALWAYS_REMOVE"],
        "null_padding": ["ALWAYS_REMOVE"],
        "quote": ["ALWAYS_REMOVE"],
        "header": ["ALWAYS_REMOVE"],
        # "new_line": ["ALWAYS_REMOVE"],
        "skip": ["ALWAYS_REMOVE"],
        "escape": ["ALWAYS_REMOVE"],
        "delim": ["ALWAYS_REMOVE"],
    },
    "AVRO": {
        "ignore_errors": ["ALWAYS_REMOVE"],
        "nullstr": ["ALWAYS_REMOVE"],
        "timestampformat": ["AUTO"],
        "compression": ["ALWAYS_REMOVE"]
    },
    "CSV": {
        "binary_as_string": ["ALWAYS_REMOVE"]
    },
    "PARQUET": {
        "ignore_errors": ["ALWAYS_REMOVE"],
        "nullstr": ["ALWAYS_REMOVE"],
        "timestampformat": ["ALWAYS_REMOVE"],
        "null_padding": ["ALWAYS_REMOVE"],
        "quote": ["ALWAYS_REMOVE"],
        "header": ["ALWAYS_REMOVE"],
        "dateformat": ["ALWAYS_REMOVE"],
        "delim": ["ALWAYS_REMOVE"],
        "escape": ["ALWAYS_REMOVE"],
        "skip": ["ALWAYS_REMOVE"],
        # "new_line": ["ALWAYS_REMOVE"],
    },
    "ALL_FORMATS": {
        "format": ["ALWAYS_REMOVE"],
        "new_line": ["ALWAYS_REMOVE"],
    }
}

@register()
class SnowflakeStageUniversqlPlugin(UniversqlPlugin):
    def __init__(self, source_executor: SnowflakeExecutor):
        super().__init__(source_executor)

    def transform_sql(self, expression: Expression, target_executor: DuckDBExecutor) -> Expression:
        # Ensure the root node is a Copy node
        if not isinstance(expression, sqlglot.exp.Copy):
            return expression

        files_list = self._find_files(expression)

        processed_file_data_copy = self.get_file_info_copy(files_list, expression)

        return self.transform_copy_into_insert_into_select(expression, processed_file_data_copy)

    def transform_copy_into_insert_into_select(self, expression, copy_data):
        """
        Transforms Snowflake COPY commands to DuckDB-compatible file reading operations.

        Converts Snowflake stage references (@stage) into direct file paths and 
        maps Snowflake COPY parameters to their DuckDB equivalents.

        Args:
            expression: COPY command AST
            file_data: Stage and file metadata

        Returns:
            Modified AST with direct file paths and mapped parameters

        Raises:
            Exception: If file format is not supported by DuckDB
        """

        if not expression.args.get('files'):
            return expression

        files = expression.args['files']
        new_files = []
        file_type = copy_data["file_parameters"]['format']["duckdb_property_value"]
        if file_type.upper() not in DUCKDB_SUPPORTED_FILE_TYPES:
            raise Exception(f"DuckDB currently does not support reading from {
                            file_type} files.")

        for table in files:
            if isinstance(table, sqlglot.exp.Table) and str(table.this).startswith('@'):
                stage_name = self.get_stage_name(table)
                stage_data = copy_data["files"].get(stage_name)

                url = stage_data["URL"][0]
                full_path = url + self.get_file_path(table)

                if full_path[-1] == "/":
                    full_path = full_path + "*"

                # Create new function node for read_csv with the S3 path
                new_files.append(Literal.string(full_path))
            else:
                new_files.append(table)
        
        file_parameters = self.convert_copy_params_to_read_datatype_params(
            copy_data["file_parameters"])
        function_name = "read_" + file_type.lower()
        from_ast = From(
            this=Table(
                this=Anonymous(
                    this=function_name,
                    expressions=[
                        Array(
                            expressions=new_files
                        )
                    ] + file_parameters
                )
            )
        )

        select_ast = Select().select(Star()).from_(from_ast)
        insert_into_select_ast = Insert(
            this=expression.this,
            expression=select_ast
        )
        return insert_into_select_ast


    def convert_copy_params_to_read_datatype_params(self, params):
        """
        Converts Snowflake COPY parameters to DuckDB-compatible options.

        Maps and transforms file reading parameters between Snowflake and DuckDB,
        handling special cases and default values.

        Args:
            params: Dictionary of Snowflake parameters

        Returns:
            List of DuckDB CopyParameter nodes
        """
        # print(f"params INCOMING:")
        # pp(params)
        converted_params = []
        params = self.apply_param_post_processing_copy(params)

        for property_name, property_info in params.items():
            if property_name == "dateformat" and property_info["duckdb_property_value"] == 'AUTO':
                continue
            # handle arrays
            elif property_info["duckdb_property_type"][-2:] == '[]':
                literal_array = []
                for array_value in property_info["duckdb_property_value"]:
                    literal_array.append(Literal(this=array_value, is_string=True))
                converted_param = EQ(
                    this=Column(
                        this=Identifier(this=property_name, quoted=False)
                    ),
                    expression=Array(
                        expressions=literal_array
                    )
                )
            else:
                converted_param = EQ(
                    this=Column(
                        this=Identifier(this=property_name, quoted=False)),
                    expression=Literal(this=property_info["duckdb_property_value"], is_string=True))

            converted_params.append(converted_param)
        return converted_params

    def apply_param_post_processing_copy(self, params):
        format = self.get_file_format(params)
        params = self._remove_problematic_params_copy(params, format)
        params = self._add_required_params(params, format)
        params = self._add_empty_field_as_null_to_nullstr(params)
        return params

    def _add_empty_field_as_null_to_nullstr(self, params):
        empty_field_as_null = params.get("EMPTY_FIELD_AS_NULL")
        if empty_field_as_null is None:
            return params

        del params["EMPTY_FIELD_AS_NULL"]
        snowflake_value = empty_field_as_null.get("snowflake_property_value")
        if snowflake_value.lower() == 'true':
            nullstr = params.get("nullstr")
            if nullstr is None:
                return params

            nullstr_values = nullstr.get("duckdb_property_value")
            nullstr_values.append("")
            params["nullstr"]["duckdb_property_value"] = nullstr_values

        return params

    def _remove_problematic_params_copy(self, params, format):

        disallowed_params = DISALLOWED_PARAMS_BY_FORMAT_COPY.get(
            format.upper(), {}) | DISALLOWED_PARAMS_BY_FORMAT_COPY.get("ALL_FORMATS", {})

        for disallowed_param, disallowed_values in disallowed_params.items():
            if disallowed_values[0] in ("ALWAYS_REMOVE"):
                params = {k: v for k, v in params.items() if k.lower() != disallowed_param.lower()}
        return params

    # fix this?
    def _add_required_params(self, params, format):
        # required_params = REQUIRED_PARAMS_BY_FORMAT.get(format, {})
        # for required_param, required_values in required_params.items():
        #     params[required_param] = required_values
        return params

    def get_file_format(self, params):
        return params["format"]["duckdb_property_value"]

    def _add_empty_field_as_null_to_nullstr(self, params):
        empty_field_as_null = params.get("EMPTY_FIELD_AS_NULL")
        if empty_field_as_null is None:
            return params

        del params["EMPTY_FIELD_AS_NULL"]
        snowflake_value = empty_field_as_null.get("snowflake_property_value")
        if snowflake_value.lower() == 'true':
            nullstr = params.get("nullstr")
            if nullstr is None:
                return params

            nullstr_values = nullstr.get("duckdb_property_value")
            nullstr_values.append("")
            params["nullstr"]["duckdb_property_value"] = nullstr_values

        return params

    def convert_copy_params(self, params):
        """
        Converts Snowflake COPY parameters to DuckDB-compatible options.

        Maps and transforms file reading parameters between Snowflake and DuckDB,
        handling special cases and default values.

        Args:
            params: Dictionary of Snowflake parameters

        Returns:
            List of DuckDB CopyParameter nodes
        """

        copy_params = []
        params = self.apply_param_post_processing(params)

        for property_name, property_info in params.items():
            if property_name == "METADATA":
                continue
            if property_name == "dateformat" and property_info["duckdb_property_value"] == 'AUTO':
                continue
            copy_params.append(
                CopyParameter(
                    this=Var(this=property_name),
                    expression=Literal.string(property_info["duckdb_property_value"]) if isinstance(
                        property_info["duckdb_property_value"], str)
                    else Literal(this=property_info["duckdb_property_value"], is_string=False)
                )
            )
        return copy_params
    
    def get_file_info_copy(self, files, ast):
        copy_data = {
            "files": {},
            "file_parameters": {}
        }


        if len(files) == 0:
            return {}
        specified_copy_params = self._extract_copy_params(ast).get("FILE_FORMAT", {})
        print("specified_copy_params INCOMING")
        pp(specified_copy_params)
        cursor = self.source_executor.catalog.cursor()
        for file in files:
                
            raw_duckdb_copy_params = self.get_duckdb_copy_params_copy(file, specified_copy_params, cursor, "SNOWFLAKE")
            raw_duckdb_copy_params["METADATA"] = raw_duckdb_copy_params["METADATA"] | file
            copy_data["files"][file["object_name"]] = raw_duckdb_copy_params["METADATA"]
            if copy_data["file_parameters"] == {}:
                del raw_duckdb_copy_params["METADATA"]
                copy_data["file_parameters"] = raw_duckdb_copy_params
                
        return copy_data
    
    
    def get_duckdb_copy_params_copy(self, file, file_format_params, cursor, catalog_type):
        """
        Retrieves and processes Snowflake file metadata.

        Gets file configuration from Snowflake and converts it to 
        DuckDB-compatible format options, handling parameter overrides.

        Args:
            file: file reference info
            file_format_params: Optional format parameter overrides
            cursor: Database cursor for file queries

        Returns:
            Dictionary of processed DuckDB parameters
        """

        if catalog_type == "SNOWFLAKE":
            copy_params = SNOWFLAKE_DEFAULT_COPY_PARAMETERS
        else:
            raise Exception("Universql currently only supports Snowflake")
        
        if file_format_params is None:
            file_format_params = {}    
        
        if file.get("type").upper() == "STAGE":
            try:
                cursor.execute(f"DESCRIBE STAGE {file["object_name"]}")
                stage_info = cursor.fetchall()
                if not stage_info:
                    raise Exception(f"No metadata returned for stage {
                                    file["object_name"]}")
            except Exception as e:
                raise Exception(f"Failed to get stage metadata: {str(e)}")
            stage_info_dict = {}            

            for row in stage_info:
                column_name = row[1]
                data_type = row[2]
                # checks to see if the parameter is overriden.  If yes, it replaces the value with the overriden value.
                value = file_format_params.get(column_name, row[3])
                copy_params[column_name] = {
                    "snowflake_property_value": value,
                    "snowflake_property_type": data_type
                }
                print(f"{column_name}: {ascii(value)}")
        else:
            for property_name, property_value in file_format_params.items():
                copy_params[property_name] = property_value
                
        duckdb_data = self.convert_to_duckdb_properties_copy(copy_params)

        return duckdb_data

    def convert_to_duckdb_properties_copy(self, copy_properties):
        """
        Maps Snowflake stage properties to DuckDB file reading options.

        Processes stage configuration and converts each property to its 
        DuckDB equivalent, handling special cases and metadata extraction.

        Args:
            copy_properties: Dictionary of Snowflake stage properties

        Returns:
            Dictionary of mapped DuckDB properties and metadata
        """
        file_format = copy_properties['TYPE']['snowflake_property_value']
        all_converted_properties = {}
        metadata = {}

        for snowflake_property_name, snowflake_property_info in copy_properties.items():
            converted_properties = self.convert_properties_copy(
                file_format, snowflake_property_name, snowflake_property_info)
            duckdb_property_name, property_values = next(
                iter(converted_properties.items()))
            if property_values["duckdb_property_type"] == 'METADATA':
                metadata[duckdb_property_name] = property_values["duckdb_property_value"]
            elif property_values["duckdb_property_type"] is None:
                continue
            else:
                all_converted_properties = all_converted_properties | converted_properties
        first_url = metadata["URL"][0]
        if first_url.startswith("s3:"):
            metadata["storage_provider"] = "Amazon S3"
        else:
            raise Exception(
                "Universql currently only supports Amazon S3 for stages locations.")
        all_converted_properties["METADATA"] = metadata
        return all_converted_properties

    def convert_properties_copy(self, file_format, snowflake_property_name, snowflake_property_info):

        no_match = {
            "duckdb_property_name": None,
            "duckdb_property_type": None
        }
        duckdb_property_info = SNOWFLAKE_TO_DUCKDB_PROPERTY_MAPPINGS.get(
            snowflake_property_name, no_match)
        duckdb_property_name = duckdb_property_info["duckdb_property_name"]
        duckdb_property_type = duckdb_property_info["duckdb_property_type"]
        properties = {
            "duckdb_property_type": duckdb_property_type
        } | snowflake_property_info | {"snowflake_property_name": snowflake_property_name}
        if duckdb_property_name is not None:
            value = self._format_value_for_duckdb_copy(
                file_format, snowflake_property_name, properties)
            properties["duckdb_property_value"] = value
        else:
            properties["duckdb_property_value"] = None
        return {duckdb_property_name: properties}

    def _format_value_for_duckdb_copy(self, file_format, snowflake_property_name, data):
        snowflake_type = data["snowflake_property_type"]
        duckdb_type = data["duckdb_property_type"]
        snowflake_value = data["snowflake_property_value"]
        if snowflake_property_name in ["date_format", "timestamp_format"]:
            duckdb_value = snowflake_value
            for snowflake_datetime_component, duckdb_datetime_component in SNOWFLAKE_TO_DUCKDB_DATETIME_MAPPINGS.items():
                duckdb_value.replace(
                    snowflake_datetime_component, duckdb_datetime_component)
            return duckdb_value
        elif snowflake_type == 'String' and duckdb_type == 'VARCHAR':
            if file_format.upper() == 'JSON' and snowflake_property_name.lower() == 'compression' and snowflake_value.lower() == 'auto':
                return "auto_detect"
            return self._format_string_for_duckdb(snowflake_value)
        elif snowflake_type == "Boolean" and duckdb_type == 'BOOL':
            return snowflake_value.lower()
        elif snowflake_type == 'Integer' and duckdb_type == 'BIGINT':
            return snowflake_value
        elif snowflake_type == 'List' and duckdb_type == 'VARCHAR[]':
            new_list = []
            for string in snowflake_value[1:len(snowflake_value)-1].split(","):
                new_list.append(self._format_string_for_duckdb(string))
            return new_list
        elif snowflake_type == 'String' and duckdb_type == 'BOOL':
            if snowflake_property_name == 'ON_ERROR':
                if snowflake_value == 'CONTINUE':
                    return 'TRUE'
                else:
                    return 'FALSE'
        elif duckdb_type == 'METADATA':
            if snowflake_property_name == 'URL':
                return ast.literal_eval(snowflake_value)
            elif snowflake_property_name in ["TYPE", "AWS_ROLE", "AWS_EXTERNAL_ID"]:
                return snowflake_value
            else:
                return "NO MATCH"
        else:
            return "NO MATCH"

    def _extract_copy_params(self, ast):
        params = {}
        for param in ast.args.get('params', []):
            param_name = param.args['this'].args['this']

            # Handle single expression case
            if 'expression' in param.args:
                expr = param.args['expression']
                params[param_name] = expr.args['this'].args['this']

            # Handle multiple expressions case
            elif 'expressions' in param.args:
                params[param_name] = {}
                for expr in param.args['expressions']:
                    property_name = expr.args['this'].args['this']
                    property_value = expr.args['value'].args['this']
                    params[param_name][property_name] = property_value

        return params

    def get_region(self, profile, url, storage_provider):
        """
        Resolves AWS region for an S3 bucket.

        Takes a bucket URL and retrieves its region using AWS SDK.
        Handles the special case where a null LocationConstraint indicates us-east-1.
        Additionally, this sets the profile to use for the copy operation.

        Args:
            profile: AWS credentials profile
            url: S3 URL containing bucket name
            storage_provider: Must be 'Amazon S3'

        Returns:
            str: AWS region identifier (e.g., 'us-east-1')
        """
        if storage_provider == 'Amazon S3':
            bucket_name = url[5:].split("/")[0]
            session = boto3.Session(profile_name=profile)
            s3 = session.client('s3')
            region_dict = s3.get_bucket_location(Bucket=bucket_name)
            return region_dict.get('LocationConstraint') or 'us-east-1'

    def get_stage_info(self, file, file_format_params):
        """
        Retrieves and processes Snowflake stage metadata.

        Gets stage configuration from Snowflake and converts it to
        DuckDB-compatible format options, handling parameter overrides.

        Args:
            file: Stage reference info
            file_format_params: Optional format parameter overrides
            cursor: Database cursor for stage queries

        Returns:
            Dictionary of processed DuckDB parameters
        """
        if file.get("type") != "STAGE" and file.get("source_catalog") != "SNOWFLAKE":
            raise Exception("There was an issue processing your file data.")
        if file_format_params is None:
            file_format_params = {}
        stage_name = file["object_name"]
        cursor = self.source_executor.catalog.cursor()
        cursor.execute(f"DESCRIBE STAGE {stage_name}")
        stage_info = cursor.fetchall()
        stage_info_dict = {}

        # file_format_overrides = None
        # if file_format_params is not None:
        #     file_format_overrides = file_format_params.keys()
        for row in stage_info:
            column_name = row[1]
            data_type = row[2]
            # checks to see if the parameter is overriden.  If yes, it replaces the value with the overriden value.
            value = file_format_params.get(column_name, row[3])
            stage_info_dict[column_name] = {
                "snowflake_property_value": value,
                "snowflake_property_type": data_type
            }
        duckdb_data = self.convert_to_duckdb_properties(stage_info_dict)

        return duckdb_data

    def _format_string_for_duckdb(self, str):
        if str == 'NONE':
            return ""
        remove_snowflake_escape_characters = re.sub(r'\\\\', r'\\', str)
        
        # add_duckdb_escape_characters = remove_snowflake_escape_characters.replace("'", "''")
        print(f"INCOMING: {ascii(str)} to {ascii(remove_snowflake_escape_characters)}")
        return f"{remove_snowflake_escape_characters}"

    def get_file_path(self, file: sqlglot.exp.Table):
        full_string = file.this.name
        in_quotes = False
        for i, char in enumerate(full_string):
            if char == '"':
                in_quotes = not in_quotes
            elif char == '/' and not in_quotes:
                return full_string[i + 1:]
        return ""

    def _get_cache_snapshot(self, monitored_dirs):
        """
        Creates a snapshot of all files currently in the specified directories.
        Files found are stored as absolute paths in a set for quick comparison.

        Args:
            monitored_dirs: List of directory paths to check for files

        Returns:
            set: Absolute paths of all files found in monitored directories

        Note:
            Silently skips directories that don't exist
        """
        files = set()
        for directory in monitored_dirs:
            try:
                with os.scandir(directory) as entries:
                    for entry in entries:
                        if entry.is_file():
                            files.add(entry.path)
            except FileNotFoundError:
                pass
        return files

    def _load_file_format(self, file_format):
        file_format_queries = {
            "JSON": ["INSTALL json;", "LOAD json;"],
            "AVRO": ["INSTALL avro FROM community;", "LOAD avro;"]
        }

    def get_stage_name(self, file: sqlglot.exp.Table):
        full_string = file.this.name
        in_quotes = False
        for i, char in enumerate(full_string):
            if char == '"':
                in_quotes = not in_quotes
            elif char == '/' and not in_quotes:
                return full_string[1:i]
        return full_string[1:i]

    def _find_files(self, ast: sqlglot.exp.Copy):
        """
        Extracts file information from a Snowflake COPY command's AST.

        This function specifically handles Snowflake stage references (prefixed with @)
        in COPY commands. It processes the 'files' argument of the COPY command and
        returns structured information about each file source.
        Args:
            ast (sqlglot.exp.Expression): The Abstract Syntax Tree of the SQL query
        Returns:
            List[Dict] | None: A list of dictionaries containing file information with keys:
                - file_qualifier: Full path/identifier of the file
                - type: Type of source (e.g., 'STAGE')
                - source_catalog: Origin catalog (e.g., 'SNOWFLAKE')
                - stage_name: Name of the stage (for stage-based files)
            Returns None if the AST is not a COPY command.
        Notes:
            Currently only supports Snowflake stage references (@stage_name).
            Direct file ingestion is not yet implemented.
        """

        # Access the files property
        file_nodes = ast.args.get("files", [])
        files = []

        for file_node in file_nodes:
            match = False
            if isinstance(file_node, sqlglot.exp.Table) and isinstance(self.source_executor.catalog, SnowflakeCatalog):
                if file_node.this.name[0] == '@':
                    files.append({
                        'file_qualifier': full_qualifier(file_node, self.source_executor.catalog.credentials),
                        'type': 'STAGE',
                        'source_catalog': 'SNOWFLAKE',
                        'object_name': self.get_stage_name(file_node)
                    })
                    match = True

            if isinstance(file_node, sqlglot.exp.Var):
                print("Ingesting data directly from files is not yet supported.")
                match = True

            if not match:
                print("Unknown node type in files:", file_node)

        return files

    def get_profile_for_role(self, aws_role_arn):
        # Get credentials file location and read TOML file
        if aws_role_arn is None:
            return "default"
        creds_file = self.get_credentials_file_location()
        try:
            config = configparser.ConfigParser()
            config.read(creds_file)
        except Exception as e:
            raise Exception(f"Failed to read credentials file: {str(e)}")

        # Find which profile has our target role_arn
        target_profile = None
        for profile_name in config.sections():
            profile_data = config[profile_name]

            if profile_data.get('role_arn') == aws_role_arn:
                target_profile = profile_name
                break

        if not target_profile:
            if not target_profile:
                # Try default profile if target role not found
                if config.has_section('default'):
                    return self._find_profile_with_credentials('default', config, creds_file)
            raise Exception(
                f"We were unable to find credentials for {aws_role_arn} in {creds_file}."
                "Please make sure you have this role_arn in your credentials file or have a default profile configured."
                "You can set the environment variable AWS_SHARED_CREDENTIALS_FILE to a different credentials file location."
            )

        return self._find_profile_with_credentials(target_profile, config, creds_file)

    def _find_profile_with_credentials(self, profile_name, config, creds_file, visited=None):
        """
        Recursive function to find the appropriate profile to use, following source_profile references if needed.

        Args:
            profile_name: Name of profile to check
            config: Configuration dictionary from config file
            creds_file: Path to credentials file
            visited: Set of profiles already checked (prevents infinite loops)

        Returns:
            str: The name of the profile to use with boto3.Session
        """
        credentials_not_found_message = f"""The profile {profile_name} cannot be found in your credentials file located at {creds_file}.
        Please update your credentials and try again."""

        # Initialize visited profiles set on first call
        if visited is None:
            visited = set()

        # Check for circular dependencies
        if profile_name in visited:
            join = ", ".join(visited)
            raise Exception(
                f"You have a circular dependency in your credentials file between the following profiles that you need to correct:"
                f"{join}"
            )
        visited.add(profile_name)

        # Get profile data
        if not config.has_section(profile_name):
            raise Exception(credentials_not_found_message)

        # If profile has source_profile, return that instead
        if config.has_option(profile_name, 'source_profile'):
            source_profile = config.get(profile_name, 'source_profile')
            if not config.has_section(source_profile):
                raise Exception(f"Source profile {source_profile} referenced by {profile_name} does not exist")
            return source_profile

        # Otherwise return the profile itself
        return profile_name

    def get_credentials_file_location(self):
        # Check for environment variable
        credentials_file_location = os.environ.get("AWS_SHARED_CREDENTIALS_FILE")
        if credentials_file_location is not None:
            return os.path.expandvars(os.path.expanduser(credentials_file_location))

        # fallback to default if it's not set
        operating_system = platform.system()
        credentials_file_location = DEFAULT_CREDENTIALS_LOCATIONS.get(operating_system)
        if credentials_file_location is not None:
            return os.path.expandvars(os.path.expanduser(credentials_file_location))

        raise Exception(
            "Universql is unable to determine your credentials file location."
            "Please set the environment variable AWS_SHARED_CREDENTIALS_FILE to your credentials file location and try again."
        )

    def get_role_credentials(self, profile_name):
        """
        Gets credentials directly from the profile
        """
        session = boto3.Session(profile_name=profile_name)

        credentials = session.get_credentials().get_frozen_credentials()

        return {
            'AccessKeyId': credentials.access_key,
            'SecretAccessKey': credentials.secret_key,
            'SessionToken': credentials.token if hasattr(credentials, 'token') else None
        }
