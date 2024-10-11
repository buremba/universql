from jinja2 import Environment, DictLoader


# Mock transformation functions
def sample_data(size):
    return {'name': 'sample_data', 'args': {'size': size}}


def duckdb(machine):
    return {'name': 'duckdb', 'args': {'machine': machine}}


def default_create_table_as_iceberg(external_volume, catalog, base_location):
    return {
        'name': 'default_create_table_as_iceberg',
        'args': {
            'external_volume': external_volume,
            'catalog': catalog,
            'base_location': base_location,
        }
    }


def snowflake(warehouse):
    return {'name': 'snowflake', 'args': {'warehouse': warehouse}}


# Jinja2 environment with mock functions
env = Environment()

# Add mock functions to the environment
env.globals['sample_data'] = sample_data
env.globals['duckdb'] = duckdb
env.globals['default_create_table_as_iceberg'] = default_create_table_as_iceberg
env.globals['snowflake'] = snowflake


# Parsing function to extract transformations
def parse_jinja_template(template_str):
    # Remove the surrounding {{ and }}
    template_str = template_str.strip("{}").strip()

    # Create the template in Jinja2
    template = env.from_string(template_str)

    # Render the template and capture the transformations
    result = template.render()

    return result


# Example usage:
input_str = "{{ transpile.sample_data('1000 rows') | execute.duckdb(machine='local'), transpile.default_create_table_as_iceberg(external_volume='iceberg_jinjat', catalog='snowflake', base_location='') | snowflake(warehouse='COMPUTE_WH') }}"
parsed_transforms = parse_jinja_template(input_str)
print(parsed_transforms)