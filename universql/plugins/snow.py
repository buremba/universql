import ast
import re

import typing
from typing import List
import pyarrow
import sqlglot
from sqlglot import Expression
from sqlglot.expressions import TableSample, CopyParameter, Var, Literal, From, Table, Anonymous, Array, Select, Star, Insert, EQ, Column, Identifier, Tuple, Property

from universql.plugin import UniversqlPlugin, register, UQuery, Locations
from universql.warehouse.duckdb import DuckDBExecutor
from universql.warehouse.snowflake import SnowflakeExecutor, SnowflakeCatalog
from universql.util import full_qualifier
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
    'TYPE': {"snowflake_property_type": "String", "snowflake_property_value": "CSV"},
    'RECORD_DELIMITER': {"snowflake_property_type": "String", "snowflake_property_value": "\n"},
    'FIELD_DELIMITER': {"snowflake_property_type": "String", "snowflake_property_value": ","},
    'SKIP_HEADER': {"snowflake_property_type": "Integer", "snowflake_property_value": 0},
    'PARSE_HEADER': {"snowflake_property_type": "Boolean", "snowflake_property_value": "FALSE"},
    'DATE_FORMAT': {"snowflake_property_type": "String", "snowflake_property_value": "AUTO"},
    'TIME_FORMAT': {"snowflake_property_type": "String", "snowflake_property_value": "AUTO"},
    'TIMESTAMP_FORMAT': {"snowflake_property_type": "String", "snowflake_property_value": "AUTO"},
    'BINARY_FORMAT': {"snowflake_property_type": "String", "snowflake_property_value": "HEX"},
    'ESCAPE': {"snowflake_property_type": "String", "snowflake_property_value": "NONE"},
    'ESCAPE_UNENCLOSED_FIELD': {"snowflake_property_type": "String", "snowflake_property_value": "\\"},
    'TRIM_SPACE': {"snowflake_property_type": "Boolean", "snowflake_property_value": "FALSE"},
    'FIELD_OPTIONALLY_ENCLOSED_BY': {"snowflake_property_type": "String", "snowflake_property_value": "NONE"},
    'NULL_IF': {"snowflake_property_type": "List", "snowflake_property_value": "[\\N]"},
    'COMPRESSION': {"snowflake_property_type": "String", "snowflake_property_value": "AUTO"},
    'ERROR_ON_COLUMN_COUNT_MISMATCH': {"snowflake_property_type": "Boolean", "snowflake_property_value": "TRUE"},
    'VALIDATE_UTF8': {"snowflake_property_type": "Boolean", "snowflake_property_value": "TRUE"},
    'SKIP_BLANK_LINES': {"snowflake_property_type": "Boolean", "snowflake_property_value": "FALSE"},
    'REPLACE_INVALID_CHARACTERS': {"snowflake_property_type": "Boolean", "snowflake_property_value": "FALSE"},
    'EMPTY_FIELD_AS_NULL': {"snowflake_property_type": "Boolean", "snowflake_property_value": "TRUE"},
    'SKIP_BYTE_ORDER_MARK': {"snowflake_property_type": "Boolean", "snowflake_property_value": "TRUE"},
    'ENCODING': {"snowflake_property_type": "String", "snowflake_property_value": "UTF8"},
    'ON_ERROR': {"snowflake_property_type": "String", "snowflake_property_value": "ABORT_STATEMENT"},
    'PURGE': {"snowflake_property_type": "Boolean", "snowflake_property_value": "FALSE"},
    'RETURN_FAILED_ONLY': {"snowflake_property_type": "Boolean", "snowflake_property_value": "FALSE"},
    'ENFORCE_LENGTH': {"snowflake_property_type": "Boolean", "snowflake_property_value": "TRUE"},
    'TRUNCATECOLUMNS': {"snowflake_property_type": "Boolean", "snowflake_property_value": "FALSE"},
    'FORCE': {"snowflake_property_type": "Boolean", "snowflake_property_value": "FALSE"},
    'ENABLE': {"snowflake_property_type": "Boolean", "snowflake_property_value": "FALSE"},
    'AUTO_REFRESH': {"snowflake_property_type": "Boolean", "snowflake_property_value": "FALSE"},
    'USE_PRIVATELINK_ENDPOINT': {"snowflake_property_type": "Boolean", "snowflake_property_value": "FALSE"},
    'ENABLE_OCTAL': {"snowflake_property_type": "Boolean", "snowflake_property_value": "FALSE"},
    'ALLOW_DUPLICATE': {"snowflake_property_type": "Boolean", "snowflake_property_value": "FALSE"},
    'STRIP_OUTER_ARRAY': {"snowflake_property_type": "Boolean", "snowflake_property_value": "FALSE"},
    'STRIP_NULL_VALUES': {"snowflake_property_type": "Boolean", "snowflake_property_value": "FALSE"},
    'IGNORE_UTF8_ERRORS': {"snowflake_property_type": "Boolean", "snowflake_property_value": "FALSE"},
    'BINARY_AS_TEXT': {"snowflake_property_type": "Boolean", "snowflake_property_value": "TRUE"},
    'USE_LOGICAL_TYPE': {"snowflake_property_type": "Boolean", "snowflake_property_value": "FALSE"},
    'USE_VECTORIZED_SCANNER': {"snowflake_property_type": "Boolean", "snowflake_property_value": "FALSE"}
}

DUCKDB_SUPPORTED_FILE_TYPES = ['CSV', 'JSON', 'AVRO', 'PARQUET']

DISALLOWED_PARAMS_BY_FORMAT = {
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


# when FILES is not defined
# COPY INTO stg_device_metadata FROM 's3:/test/initial_objects/*' (TYPE = CSV SKIP_HEADER = 1)
# when FILES is specified:
# COPY INTO stg_device_metadata FROM 's3:/test/initial_objects/device_metadata.csv' (TYPE = CSV SKIP_HEADER = 1)
# COPY INTO stg_device_metadata FROM 's3:/test/initial_objects/file2.csv' (TYPE = CSV SKIP_HEADER = 1)

class SnowflakeQueryTransformer(UQuery):
    
    
    def source_executor(self):
        return self.session.catalog_executor
    
    def transform_ast(self, expression: Expression, target_executor: DuckDBExecutor) -> Expression:

        if not isinstance(expression, sqlglot.exp.Copy):
            return expression

        files_list = self._find_files(expression)
        processed_file_data = self.get_file_info(files_list, expression)
        
        credentials = self._get_credentials_for(processed_file_data, target_executor)
        final_ast = self.transform_copy_into_insert_into_select(expression, processed_file_data)
        return final_ast

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

        # print("copy_data INCOMING")
        # pp(copy_data)
        if not expression.args.get('files'):
            return expression

        files = expression.args['files']
        new_files = []
        file_type = copy_data["file_parameters"]['format']["duckdb_property_value"]
        if file_type.upper() not in DUCKDB_SUPPORTED_FILE_TYPES:
            raise Exception(f"DuckDB currently does not support reading from {file_type} files.")

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

    def get_file_path(self, file: sqlglot.exp.Table):
        full_string = file.this.name
        in_quotes = False
        for i, char in enumerate(full_string):
            if char == '"':
                in_quotes = not in_quotes
            elif char == '/' and not in_quotes:
                return full_string[i + 1:]
        return ""

    def _find_files(self, ast: sqlglot.exp.Copy):
        """
        Extracts file information from a Snowflake COPY command's AST.  Returns an array of file dictionaries with the following:
        
        file_qualifier: full name of the object or one if it is an ad hoc URL query
        type: STAGE or URL
        source_catalog: will always be snowflake
        object_name: the objects name that can be referenced in metadata queries
        """

        # Access the files property
        file_nodes = ast.args.get("files", [])
        files = []

        for file_node in file_nodes:
            match = False
            if isinstance(file_node, sqlglot.exp.Table) and isinstance(self.source_executor().catalog, SnowflakeCatalog):
                if file_node.this.name[0] == '@':
                    files.append({
                        'file_qualifier': full_qualifier(file_node, self.source_executor().catalog.credentials),
                        'type': 'STAGE',
                        'source_catalog': 'SNOWFLAKE',
                        'object_name': self.get_stage_name(file_node)
                    })
                    match = True
                else:
                    files.append({
                        'file_qualifier': None,
                        'type': 'URL',
                        'source_catalog': 'SNOWFLAKE',
                        'object_name': file_node.this.name
                    })
                    match = True
            if isinstance(file_node, sqlglot.exp.Var):
                print("Ingesting data directly from files is not yet supported.")
                match = True

            if not match:
                print("Unknown node type in files:", file_node)

        return files


    def _get_stage(self, table: sqlglot.exp.Table):
        # self.source_executor.execute_raw("DESCRIBE STAGE {}", self.source_executor)
        return
    
    def get_stage_name(self, file: sqlglot.exp.Table):
        full_string = file.this.name
        in_quotes = False
        for i, char in enumerate(full_string):
            if char == '"':
                in_quotes = not in_quotes
            elif char == '/' and not in_quotes:
                return full_string[1:i]
        return full_string[1:i]
    
    def get_file_info(self, files, ast):
        copy_data = {
            "files": {},
            "file_parameters": {}
        }

        specified_copy_params = self._extract_copy_params(ast)
        s3_access_key_id = specified_copy_params.get("AWS_KEY_ID")
        s3_secret_access_key = specified_copy_params.get("AWS_SECRET_KEY")
        cursor = self.source_executor().catalog.cursor()
        
        for file in files:        
            raw_duckdb_copy_params = self.get_duckdb_copy_params(file, specified_copy_params, cursor, "SNOWFLAKE")
            raw_duckdb_copy_params["METADATA"] = raw_duckdb_copy_params["METADATA"] | file
            copy_data["files"][file["object_name"]] = raw_duckdb_copy_params["METADATA"]
            if copy_data["file_parameters"] == {}:
                del raw_duckdb_copy_params["METADATA"]
                copy_data["file_parameters"] = raw_duckdb_copy_params
                copy_data["s3_access_key_id"] = s3_access_key_id
                copy_data["s3_secret_access_key"] = s3_secret_access_key
        return copy_data
    
    def _extract_copy_params(self, ast):
        params = {}
        for param in ast.args.get('params', []):
            param_name = param.args['this'].args['this']            
            
            # Handle single expression case
            if 'expression' in param.args:
                expr = param.args['expression']
                if isinstance(expr, Literal):
                    params[param_name] = expr.args['this']
                    print(f"{param_name} expression Literal")
                # handle multiple expressions within a single
                elif isinstance(expr, Tuple):
                    expressions = expr.args["expressions"]
                    param_pair = self._iterate_through_expressions(expressions)
                    params.update(param_pair)
                    print(f"{param_name} expression Tuple")
            # Handle multiple expressions case
            elif 'expressions' in param.args:
                expressions = param.args['expressions']
                param_pair = self._iterate_through_expressions(expressions)
                params.update(param_pair)
                print(f"{param_name} expressions")
        return params

    def _get_credentials_for(self, copy_data, target_executor):
        s3_access_key_id = copy_data.get("s3_access_key_id")
        s3_secret_access_key = copy_data.get("s3_secret_access_key")
        if s3_access_key_id is None or s3_secret_access_key is None:
            return None
        return {
            's3_access_key_id': s3_access_key_id,
            's3_secret_access_key':s3_secret_access_key
        }

    def _iterate_through_expressions(self, expressions):
        result = {}
        for expr in expressions:
            if isinstance(expr, Property):
                property_name = expr.args['this'].args['this']
                property_value = expr.args['value'].args['this'] 
            elif isinstance(expr, EQ):
                property_name = expr.args["this"].args["this"].args["this"]
                property_value = expr.args["expression"].args["this"]
            result[property_name] = property_value
        return result
    
    def get_duckdb_copy_params(self, file, file_format_params, cursor, catalog_type):
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
        # print("file_format_params INCOMING")
        # pp(file_format_params)
        if catalog_type == "SNOWFLAKE":
            copy_params = SNOWFLAKE_DEFAULT_COPY_PARAMETERS
        else:
            raise Exception("Universql currently only supports Snowflake")
        
        if file_format_params is None:
            file_format_params = {}    
        
        if file.get("type").upper() == "STAGE":
            try:
                cursor.execute(f"DESCRIBE STAGE {file['object_name']}")
                stage_info = cursor.fetchall()
                if not stage_info:
                    raise Exception(f"No metadata returned for stage {file['object_name']}")
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
        else:
            for property_name, property_value in file_format_params.items():
                property_info = copy_params.get(property_name.upper())
                if property_info is None:
                    continue
                property_info["snowflake_property_value"] = property_value
                copy_params[property_name] = property_info
                
        duckdb_data = self.convert_to_duckdb_properties(copy_params)

        return duckdb_data
    
    def convert_to_duckdb_properties(self, copy_properties):
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
        # print("copy_properties INCOMING")
        # pp(copy_properties)

        for snowflake_property_name, snowflake_property_info in copy_properties.items():
            converted_properties = self.convert_properties(
                file_format, snowflake_property_name, snowflake_property_info)
            duckdb_property_name, property_values = next(
                iter(converted_properties.items()))
            if property_values["duckdb_property_type"] == 'METADATA':
                metadata[duckdb_property_name] = property_values["duckdb_property_value"]
            elif property_values["duckdb_property_type"] is None:
                continue
            else:
                all_converted_properties = all_converted_properties | converted_properties
        # need to check for storage provider somewhere else

        # first_url = metadata.get("URL",[""])
        # if first_url.startswith("s3:"):
        #     metadata["storage_provider"] = "Amazon S3"
        # else:
        #     raise Exception(
        #         "Universql currently only supports Amazon S3 for stages locations.")
        all_converted_properties["METADATA"] = metadata
        return all_converted_properties    

    def convert_properties(self, file_format, snowflake_property_name, snowflake_property_info):

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
            value = self._format_value_for_duckdb(
                file_format, snowflake_property_name, properties)
            properties["duckdb_property_value"] = value
        else:
            properties["duckdb_property_value"] = None
        return {duckdb_property_name: properties}

    def _format_value_for_duckdb(self, file_format, snowflake_property_name, data):
        snowflake_type = data["snowflake_property_type"]
        duckdb_type = data["duckdb_property_type"]
        snowflake_value = str(data["snowflake_property_value"])
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

    def _format_string_for_duckdb(self, str):
        if str == 'NONE':
            return ""
        remove_snowflake_escape_characters = re.sub(r'\\\\', r'\\', str)
        
        # add_duckdb_escape_characters = remove_snowflake_escape_characters.replace("'", "''")
        return f"{remove_snowflake_escape_characters}"



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

        converted_params = []
        params = self.apply_param_post_processing(params)

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
    
    def apply_param_post_processing(self, params):
        format = self.get_file_format(params)
        params = self._remove_problematic_params(params, format)
        params = self._add_required_params(params, format)
        params = self._add_empty_field_as_null_to_nullstr(params)
        return params
    
    def get_file_format(self, params):
        return params["format"]["duckdb_property_value"]

    def _remove_problematic_params(self, params, format):

        disallowed_params = DISALLOWED_PARAMS_BY_FORMAT.get(
            format.upper(), {}) | DISALLOWED_PARAMS_BY_FORMAT.get("ALL_FORMATS", {})

        for disallowed_param, disallowed_values in disallowed_params.items():
            if disallowed_values[0] in ("ALWAYS_REMOVE"):
                params = {k: v for k, v in params.items() if k.lower() != disallowed_param.lower()}
        return params

    def _add_required_params(self, params, format):
        # required_params = REQUIRED_PARAMS_BY_FORMAT.get(format, {})
        # for required_param, required_values in required_params.items():
        #     params[required_param] = required_values
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

    def post_execute(self, locations: typing.Optional[Locations], target_executor: DuckDBExecutor):
        pass

    def end(self, table : pyarrow.Table):
        pass  
@register()
class SnowflakeUniversqlPlugin(UniversqlPlugin):
    def __init__(self, session: "universql.protocol.session.UniverSQLSession"):
        super().__init__(session)

    def start_query(self, ast: typing.Optional[List[sqlglot.exp.Expression]], raw_query: str) -> UQuery:
        return SnowflakeQueryTransformer(self.session, ast, raw_query)


# @register()
class TableSampleUniversqlPlugin(UniversqlPlugin):
    def __init__(self, source_executor: SnowflakeExecutor):
        super().__init__(source_executor)

    def transform_sql(self, expression: Expression, target_executor: DuckDBExecutor,
                      sample="reservoir(1000)") -> Expression:
        if not isinstance(target_executor, DuckDBExecutor):
            raise NotImplementedError
        if isinstance(expression, sqlglot.exp.Select) and 'sample' not in expression.args:
            sample_value = sqlglot.exp.maybe_parse(sample)
            expression.args['sample'] = sqlglot.exp.TableSample(
                method=sqlglot.exp.Var(this=sample_value.this),
                size=sample_value.expressions[0])
        return expression
