import ast
import sqlglot
from pprint import pp

def get_stage_info(file, cursor):
    if file.get("type") != "STAGE" and file.get("source_catalog") != "SNOWFLAKE":
        raise Exception("There was an issue processing your file data.")
    cursor.execute(f"DESCRIBE STAGE {file["stage_name"]}")
    stage_info = cursor.fetchall()
    stage_info_dict = {}

    for row in stage_info:
        column_name = row[1]
        data_type = row[2]
        value = row[3]
        stage_info_dict[column_name] = {
            "snowflake_property_value": value,
            "snowflake_property_type": data_type
        }



    duckdb_data = convert_to_duckdb_properties(stage_info_dict)

    return duckdb_data

def convert_to_duckdb_properties(copy_properties):
    all_converted_properties = {}
    metadata = {}
    for snowflake_property_name, snowflake_property_info in copy_properties.items():
        converted_properties = convert_properties(snowflake_property_name, snowflake_property_info)
        duckdb_property_name, property_values = next(iter(converted_properties.items()))
        if property_values["duckdb_property_type"] == 'METADATA':
            metadata[duckdb_property_name] = property_values["duckdb_property_value"]
        else:
            all_converted_properties = all_converted_properties | converted_properties
    all_converted_properties["METADATA"] = metadata    
    return all_converted_properties

def convert_properties(snowflake_property_name, snowflake_property_info):
    duckdb_property_info = PROPERTY_MAPPINGS[snowflake_property_name]
    duckdb_property_name = duckdb_property_info["duckdb_property_name"]
    duckdb_property_type = duckdb_property_info["duckdb_property_type"]
    properties = {
        "duckdb_property_type": duckdb_property_type
    } | snowflake_property_info | {"snowflake_property_name": snowflake_property_name}
    if duckdb_property_name is not None:
        value = _format_value_for_duckdb(snowflake_property_name, properties)
        properties["duckdb_property_value"] = value
    else:
        properties["duckdb_property_value"] = None
    return {duckdb_property_name: properties}

def _format_value_for_duckdb(snowflake_property_name, data):
    snowflake_type = data["snowflake_property_type"]
    duckdb_type = data["duckdb_property_type"]
    snowflake_value = data["snowflake_property_value"]
    if snowflake_type == 'String' and duckdb_type == 'VARCHAR':
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
        elif snowflake_property_name in ["TYPE", "AWS_ROLE"]:
            return snowflake_value
        else:
            return "NO MATCH"
    else:
        return "NO MATCH"
        
def _format_string_for_duckdb(str):
    remove_snowflake_escape_characters = str.replace('\\\\', '\\')
    add_duckdb_escape_characters = remove_snowflake_escape_characters.replace("'", "''")
    return f"'{add_duckdb_escape_characters}'"

def get_stage_name(file: sqlglot.exp.Table):
    full_string = file.this.name
    in_quotes = False
    for i, char in enumerate(full_string):
        if char == '"':
            in_quotes = not in_quotes
        elif char == '/' and not in_quotes:
            return full_string[1:i]
    return full_string[1:i]

PROPERTY_MAPPINGS = {
    "TYPE": {
        "duckdb_property_name": "file_type",
        "duckdb_property_type": "METADATA" 
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
        "duckdb_property_name": None,
        "duckdb_property_type": None 
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
        "duckdb_property_name": None,
        "duckdb_property_type": None
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
    }
}

    # def convert_stage_params()