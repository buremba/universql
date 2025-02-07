import typing
from typing import List

import pyarrow
import sqlglot
from sqlglot import Expression
from sqlglot.expressions import TableSample

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


# when FILES is not defined
# COPY INTO stg_device_metadata FROM 's3:/test/initial_objects/*' (TYPE = CSV SKIP_HEADER = 1)
# when FILES is specified:
# COPY INTO stg_device_metadata FROM 's3:/test/initial_objects/device_metadata.csv' (TYPE = CSV SKIP_HEADER = 1)
# COPY INTO stg_device_metadata FROM 's3:/test/initial_objects/file2.csv' (TYPE = CSV SKIP_HEADER = 1)

class SnowflakeQueryTransformer(UQuery):
    
    
    def transform_ast(self, expression: Expression, target_executor: DuckDBExecutor) -> Expression:

        if not isinstance(expression, sqlglot.exp.Copy):
            return expression

        files_list = self._find_files(expression)
        print("files_list INCOMING")
        pp(files_list)
        # processed_file_data_copy = self.get_file_info_copy(files_list, expression)
        
        # credentials = self._get_credentials_for_copy(processed_file_data_copy, target_executor)
        # final_ast = self.transform_copy_into_insert_into_select(expression, processed_file_data_copy)
        return final_ast

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
            if isinstance(file_node, sqlglot.exp.Table) and isinstance(self.source_executor.catalog, SnowflakeCatalog):
                if file_node.this.name[0] == '@':
                    files.append({
                        'file_qualifier': full_qualifier(file_node, self.source_executor.catalog.credentials),
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

    def post_execute(self, locations: typing.Optional[Locations], target_executor: DuckDBExecutor):
        pass

    def end(self, table : pyarrow.Table):
        pass

@register()
class SnowflakeUniversqlPlugin(UniversqlPlugin):
    def __init__(self, session: "universql.protocol.session.UniverSQLSession"):
        super().__init__(session)

    def start_query(self, ast: typing.Optional[List[sqlglot.exp.Expression]], raw_query: str) -> UQuery:
        return SnowflakeQueryTransformer(ast, raw_query)


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
