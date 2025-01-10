import ast
import sqlglot

def convert_file_properties(file_properties):
    for property, property_value in file_properties:
        pass
    return {}

def get_stage_info(file, cursor):
    cursor.execute(f"DESCRIBE STAGE {file["stage_name"]}")
    stage_info = cursor.fetchall()

    filtered_stage_info = {}

    for row in stage_info:
        column_name = row[1]
        value = _format_stage_value(row)
        filtered_stage_info[column_name] = value
    return filtered_stage_info

def _format_stage_value(row):
    def _format_snowflake_type(value, data_type):
        if data_type == 'String':
            return _escape_backslashes(value)
        elif data_type == 'Boolean':
            return True if row[2] == 'true' else False
        elif data_type == 'Integer':
            return int(value)
        
    data_type = row[2]
    value = row[3]

    if value == '':
        return None
    
    # will fail if a list item contain ", " within it
    # because SF does not use quotes around strings for list types
    elif data_type == 'List':
        formatted_value = _escape_backslashes( value[1:len(value)-1])
        return formatted_value.split(", ")
    elif value[0] == '[':
        raw_array = ast.literal_eval(value)
        formatted_array = []
        for value in raw_array:
            formatted_array.append(_format_snowflake_type(value, data_type))
        return formatted_array
    else:
        return _format_snowflake_type(value, data_type)

def _format_stage_properties(stage_row):
    return {
        "property": stage_row["property"],
        "value": stage_row["property"]
    }

def _escape_backslashes(str):
    return str.replace('\\\\', '\\')

def get_stage_name(file: sqlglot.exp.Table):
    full_string = file.this.name
    in_quotes = False
    for i, char in enumerate(full_string):
        if char == '"':
            in_quotes = not in_quotes
        elif char == '/' and not in_quotes:
            return full_string[1:i]
    return full_string[1:i]



SNOWFLAKE_TO_DUCKDB_MAPPING = {
    "TYPE": "type",
    "RECORD_DELIMITER": 'new_line',
    "FIELD_DELIMITER": "delimiter",
    "FILE_EXTENSION": "file_extension",
    "SKIP_HEADER": "skip",
    "PARSE_HEADER": "header",
    "DATE_FORMAT": "dateformat",
    "TIME_FORMAT": None,
    "TIMESTAMP_FORMAT": "timestampformat",
    "BINARY_FORMAT": None,
    "ESCAPE": "escape",
    "ESCAPE_UNENCLOSED_FIELD": None,
    "TRIM_SPACE": "trim_space",
    "FIELD_OPTIONALLY_ENCLOSED_BY": "quote",
    "NULL_IF": "nullstr",
    "COMPRESSION": "compression",
    "ERROR_ON_COLUMN_COUNT_MISMATCH": "null_padding",
    "VALIDATE_UTF8": None,
    "SKIP_BLANK_LINES": None,
    "REPLACE_INVALID_CHARACTERS": None,
    "EMPTY_FIELD_AS_NULL": None,
    "SKIP_BYTE_ORDER_MARK": "skip_byte_order_mark",
    "ENCODING": None
}

    # def convert_stage_params()