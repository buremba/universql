import ast
import configparser
import os
import platform

import boto3
import sqlglot
from sqlglot import Expression
from sqlglot.expressions import CopyParameter, Var, Literal

from universql.plugin import UniversqlPlugin, register
from universql.util import full_qualifier, QueryError
from universql.warehouse.duckdb import DuckDBExecutor
from universql.warehouse.snowflake import SnowflakeExecutor, SnowflakeCatalog

# when FILES is not defined
# COPY INTO stg_device_metadata FROM 's3:/test/initial_objects/*' (TYPE = CSV SKIP_HEADER = 1)
# when FILES is specified:
# COPY INTO stg_device_metadata FROM 's3:/test/initial_objects/device_metadata.csv' (TYPE = CSV SKIP_HEADER = 1)
# COPY INTO stg_device_metadata FROM 's3:/test/initial_objects/file2.csv' (TYPE = CSV SKIP_HEADER = 1)
# @register()
# class SnowflakeStageUniversqlPlugin(UniversqlPlugin):
#     def __init__(self, source_executor: SnowflakeExecutor):
#         super().__init__(source_executor)
#
#     def transform_sql(self, expression: Expression, target_executor: DuckDBExecutor) -> Expression:
#         if isinstance(expression, sqlglot.exp.Var) and expression.name.startswith('@'):
#             expression.args['name'] = 'myname'
#             return expression
#
#         # referenced from copy
#         if isinstance(expression, sqlglot.exp.Table) and expression.alias_or_name.startswith('@'):
#             self._get_stage(expression)
#
#         return expression
#
#     def _get_stage(self, table: sqlglot.exp.Table):
#         # self.source_executor.execute_raw("DESCRIBE STAGE {}", self.source_executor)
#         return
# @register()
# class TableSampleUniversqlPlugin(UniversqlPlugin):
#     def __init__(self, source_executor: SnowflakeExecutor):
#         super().__init__(source_executor)
#
#     def transform_sql(self, expression: Expression, target_executor: DuckDBExecutor,
#                       sample="reservoir(1000)") -> Expression:
#         if not isinstance(target_executor, DuckDBExecutor):
#             raise NotImplementedError
#         if isinstance(expression, sqlglot.exp.Select) and 'sample' not in expression.args:
#             sample_value = sqlglot.exp.maybe_parse(sample)
#             expression.args['sample'] = sqlglot.exp.TableSample(
#                 method=sqlglot.exp.Var(this=sample_value.this),
#                 size=sample_value.expressions[0])
#         return expression

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
        "duckdb_property_name": "delimiter",
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


DUCKDB_SUPPORTED_FILE_TYPES = ['CSV', 'JSON', 'AVRO', 'PARQUET']

@register()
class SnowflakeStageUniversqlPlugin(UniversqlPlugin):
    def __init__(self, source_executor: SnowflakeExecutor):
        super().__init__(source_executor)

    def transform_sql(self, expression: Expression, target_executor: DuckDBExecutor) -> Expression:
        # Ensure the root node is a Copy node
        if not isinstance(expression, sqlglot.exp.Copy):
            return expression

        cache_directory = self.source_executor.catalog.context.get('cache_directory')
        file_cache_directories = []

        files_list = self._find_files(expression)

        processed_file_data = self.get_file_info(files_list, expression)
        # for file_name, file_config in processed_file_data.items():
        #     metadata = file_config["METADATA"]
        #     if metadata["storage_provider"] != "Amazon S3":
        #         raise Exception("Universql currently only supports Amazon S3 stages.")
        #     aws_role = metadata.get("AWS_ROLE")
        #     profile_to_use = self.get_profile_for_role(aws_role)
        #     metadata["profile"] = profile_to_use
            # urls = file_config["METADATA"]["URL"]
            # profile = file_config["METADATA"]["profile"]
            # stage_name_length = len(file_config["METADATA"]["stage_name"])
            # file_nodes = expression.args.get("files", [])
            # primary_folder = urls[0].replace('://', '/')
            # for file in file_nodes:
            #     sub_path = file.this.name[stage_name_length + 2:].rsplit('/', 1)[0] + "/"
            #     full_path = cache_directory + "/" + primary_folder + sub_path
            #     file_cache_directories.append(full_path)
            # try:
            #     region = self.get_region(profile, urls[0], file_config["METADATA"]["storage_provider"])
            # except Exception as e:
            #     print(f"There was a problem accessing data for {file_name}:\n{e}")

        return self.transform_copy(expression, processed_file_data)

    def transform_copy(self, expression, file_data):
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
        copy_params = []
        for table in files:
            if isinstance(table, sqlglot.exp.Table) and str(table.this).startswith('@'):
                stage_name = self.get_stage_name(table)
                stage_file_data = file_data.get(stage_name)
                metadata = stage_file_data["METADATA"]
                file_type = self.get_file_format(stage_file_data)
                if file_type not in DUCKDB_SUPPORTED_FILE_TYPES:
                    raise Exception(f"DuckDB currently does not support reading from {file_type} files.")
                url = metadata["URL"][0]
                full_path = url + self.get_file_path(table)

                # Create new function node for read_csv with the S3 path
                new_files.append(Literal.string(full_path))
                if not copy_params:
                    copy_params = self.convert_copy_params(stage_file_data)
                    existing_params = expression.args.get('params', [])
                    # remove existing CopyParameter from params
                    filtered_params = [p for p in existing_params if not isinstance(p, sqlglot.exp.CopyParameter)]
                    expression.args['params'] = copy_params + filtered_params
            else:
                new_files.append(table)

        expression.args['files'] = new_files
        return expression

    def _remove_problematic_params(self, params, format):
        disallowed_params = DISALLOWED_PARAMS_BY_FORMAT.get(format, {})
        for disallowed_param, disallowed_values in disallowed_params.items():
            if params.get(disallowed_param) is not None:
                if disallowed_values[0] in ("ALWAYS_REMOVE"):
                    del params[disallowed_param]
                    continue
                param_current_value = params[disallowed_param]["duckdb_property_value"]
                if param_current_value in disallowed_values:
                    del params[disallowed_param]
        return params

    def apply_param_post_processing(self, params):
        format = self.get_file_format(params)
        params = self._remove_problematic_params(params, format)
        params = self._add_empty_field_as_null_to_nullstr(params)
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

    def get_file_info(self, files, ast):
        copy_data = {}
        if len(files) == 0:
            return {}

        copy_params = self._extract_copy_params(ast)
        file_format_params = copy_params.get("FILE_FORMAT")
        with self.source_executor.catalog.cursor() as cursor:
            for file in files:
                if file.get("type") == 'STAGE':
                    stage_info = self.get_stage_info(file, file_format_params, cursor)
                    stage_info["METADATA"] = stage_info["METADATA"] | file
                    copy_data[file["stage_name"]] = stage_info
                else:
                    raise QueryError("Unable to find type")
        return copy_data

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

    def get_stage_info(self, file, file_format_params, cursor):
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
        stage_name = file["stage_name"]
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

        for snowflake_property_name, snowflake_property_info in copy_properties.items():
            converted_properties = self.convert_properties(file_format, snowflake_property_name,
                                                           snowflake_property_info)
            duckdb_property_name, property_values = next(iter(converted_properties.items()))
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
            raise Exception("Universql currently only supports Amazon S3 for stages locations.")
        all_converted_properties["METADATA"] = metadata
        return all_converted_properties

    def convert_properties(self, file_format, snowflake_property_name, snowflake_property_info):
        no_match = {
            "duckdb_property_name": None,
            "duckdb_property_type": None
        }
        duckdb_property_info = SNOWFLAKE_TO_DUCKDB_PROPERTY_MAPPINGS.get(snowflake_property_name, no_match)
        duckdb_property_name = duckdb_property_info["duckdb_property_name"]
        duckdb_property_type = duckdb_property_info["duckdb_property_type"]
        properties = {
                         "duckdb_property_type": duckdb_property_type
                     } | snowflake_property_info | {"snowflake_property_name": snowflake_property_name}
        if duckdb_property_name is not None:
            value = self._format_value_for_duckdb(file_format, snowflake_property_name, properties)
            properties["duckdb_property_value"] = value
        else:
            properties["duckdb_property_value"] = None
        return {duckdb_property_name: properties}

    def _format_value_for_duckdb(self, file_format, snowflake_property_name, data):
        snowflake_type = data["snowflake_property_type"]
        duckdb_type = data["duckdb_property_type"]
        snowflake_value = data["snowflake_property_value"]
        if snowflake_property_name in ["date_format", "timestamp_format"]:
            duckdb_value = snowflake_value
            for snowflake_datetime_component, duckdb_datetime_component in SNOWFLAKE_TO_DUCKDB_DATETIME_MAPPINGS.items():
                duckdb_value.replace(snowflake_datetime_component, duckdb_datetime_component)
            return duckdb_value
        elif snowflake_type == 'String' and duckdb_type == 'VARCHAR':
            if file_format == 'JSON' and snowflake_property_name.lower() == 'compression' and snowflake_value.lower() == 'auto':
                return "auto_detect"
            return self._format_string_for_duckdb(snowflake_value)
        elif snowflake_type == "Boolean" and duckdb_type == 'BOOL':
            return snowflake_value.lower()
        elif snowflake_type == 'Integer' and duckdb_type == 'BIGINT':
            return snowflake_value
        elif snowflake_type == 'List' and duckdb_type == 'VARCHAR[]':
            new_list = []
            for string in snowflake_value[1:len(snowflake_value) - 1].split(","):
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
        remove_snowflake_escape_characters = str.replace('\\\\', '\\')
        # add_duckdb_escape_characters = remove_snowflake_escape_characters.replace("'", "''")
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
                        'stage_name': self.get_stage_name(file_node)
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
