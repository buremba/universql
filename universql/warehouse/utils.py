import ast
import sqlglot
from pprint import pp
from sqlglot.expressions import Literal, CopyParameter, Var

DUCKDB_SUPPORTED_FILE_TYPES = ['CSV', 'JSON', 'AVRO', 'Parquet']

FILE_FORMAT_LOAD_QUERIES = {
    "JSON": ["INSTALL json;", "LOAD json;"],
    "AVRO": ["INSTALL avro FROM community;", "LOAD avro;"]
}

def get_load_file_format_queries(file_format):
    return FILE_FORMAT_LOAD_QUERIES.get(file_format, [])
    
def transform_copy(expression, file_data):
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
            stage_name = get_stage_name(table)
            stage_file_data = file_data.get(stage_name)
            metadata = stage_file_data["METADATA"]
            file_type = get_file_format(stage_file_data)
            if file_type not in DUCKDB_SUPPORTED_FILE_TYPES:
                raise Exception(f"DuckDB currently does not support reading from {file_type} files.")
            url = metadata["URL"][0]
            full_path = url + get_file_path(table)
            
            # Create new function node for read_csv with the S3 path
            new_files.append(Literal.string(full_path))
            if copy_params == []:
                copy_params = convert_copy_params(stage_file_data)
                existing_params = expression.args.get('params', [])
                # remove existing CopyParameter from params
                filtered_params = [p for p in existing_params if not isinstance(p, sqlglot.exp.CopyParameter)]            
                expression.args['params'] = copy_params + filtered_params
        else:
            new_files.append(table)
            
    expression.args['files'] = new_files
    return expression

def convert_copy_params(params):
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
    params = apply_param_post_processing(params)

    for property_name, property_info in params.items():
        if property_name == "METADATA":
            continue
        if property_name == "dateformat" and property_info["duckdb_property_value"] == 'AUTO':
            continue
        copy_params.append(
            CopyParameter(
                this=Var(this=property_name),
                expression=Literal.string(property_info["duckdb_property_value"]) if isinstance(property_info["duckdb_property_value"], str) 
                    else Literal(this=property_info["duckdb_property_value"], is_string=False)
            )
        )    
    return copy_params

def get_file_format(params):
    return params["format"]["duckdb_property_value"]

def apply_param_post_processing(params):
    format = get_file_format(params)
    params = _remove_problematic_params(params, format)
    params = _add_required_params(params, format)
    params = _add_empty_field_as_null_to_nullstr(params)
    return params

def _add_empty_field_as_null_to_nullstr(params):
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

def _remove_problematic_params(params, format):
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

def _add_required_params(params, format):
    # required_params = REQUIRED_PARAMS_BY_FORMAT.get(format, {})
    # for required_param, required_values in required_params.items():
    #     params[required_param] = required_values
    return params

def get_stage_info(file, file_format_params, cursor):
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
    cursor.execute(f"DESCRIBE STAGE {file["stage_name"]}")
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
    duckdb_data = convert_to_duckdb_properties(stage_info_dict)

    return duckdb_data

def convert_to_duckdb_properties(copy_properties):
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
        converted_properties = convert_properties(file_format, snowflake_property_name, snowflake_property_info)
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

def convert_properties(file_format, snowflake_property_name, snowflake_property_info):
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
        value = _format_value_for_duckdb(file_format, snowflake_property_name, properties)
        properties["duckdb_property_value"] = value
    else:
        properties["duckdb_property_value"] = None
    return {duckdb_property_name: properties}

def _format_value_for_duckdb(file_format, snowflake_property_name, data):
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
        return _format_string_for_duckdb(snowflake_value)
    elif snowflake_type == "Boolean" and duckdb_type == 'BOOL':
        return snowflake_value.lower()
    elif snowflake_type == 'Integer' and duckdb_type == 'BIGINT':
        return snowflake_value
    elif snowflake_type == 'List' and duckdb_type == 'VARCHAR[]':
        new_list = []
        for string in snowflake_value[1:len(snowflake_value)-1].split(","):
            new_list.append(_format_string_for_duckdb(string))
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
        
def _format_string_for_duckdb(str):
    if str == 'NONE':
        return ""
    remove_snowflake_escape_characters = str.replace('\\\\', '\\')
    # add_duckdb_escape_characters = remove_snowflake_escape_characters.replace("'", "''")
    return f"{remove_snowflake_escape_characters}"

def get_stage_name(file: sqlglot.exp.Table):
    full_string = file.this.name
    in_quotes = False
    for i, char in enumerate(full_string):
        if char == '"':
            in_quotes = not in_quotes
        elif char == '/' and not in_quotes:
            return full_string[1:i]
    return full_string[1:i]

def get_file_path(file: sqlglot.exp.Table):
    full_string = file.this.name
    in_quotes = False
    for i, char in enumerate(full_string):
        if char == '"':
            in_quotes = not in_quotes
        elif char == '/' and not in_quotes:
            return full_string[i + 1:]
    return ""

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
    'MON': "%b", #in snowflake this means full or abbreviated; duckdb doesn't have an either or option
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
    "ALLOW_DUPLICATE": { # duckdb only takes the last value
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
    "STRIP_NULL_VALUES": { # would need to be handled after a successful copy
        "duckdb_property_name": None,
        "duckdb_property_type": None
    },
    "STRIP_OUTER_ARRAY": { # needs to use json_array_elements() after loading
        "duckdb_property_name": None,
        "duckdb_property_type": None
    }
}

    # def convert_stage_params()