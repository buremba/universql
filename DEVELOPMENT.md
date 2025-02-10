# Running Github Actions locally

```shell
act push --container-architecture linux/amd64 --secret-file .env.act --workflows ./.github/workflows/test.yml --insecure-secrets
```

# Update certs

1. Start [the instance](https://console.cloud.google.com/compute/instancesDetail/zones/us-central1-a/instances/instance-20240709-162937?inv=1&invt=AbpNfQ&project=jinjat-demo)

2. Connect and update the certs
```bash
gcloud compute ssh --zone "us-central1-a" "instance-20240709-162937" --project "jinjat-demo"
```

Update IP on DNS: https://dash.cloudflare.com/09f0f68ebdbed47d203725997d4cfbdb/localhostcomputing.com/dns/records

```bash
sudo certbot certonly --standalone --domains localhostcomputing.com
```

3. Update the [util.py](/universql/util.py)

# Plugins

If you would like to enable custom logic inside Universql,  to support transition in between different dialects or just 
```bash
> poetry add "universql[rewrite_create_table_as_iceberg]"
> universql snowflake myaccount.aws.us-east-1 --module-paths rewrite_create_table_as_iceberg
```

universql snowflake --module-paths rewrite_create_table_as_iceberg

Then, implement your custom logic later on if you would like.

You can inject modules into Universql at start time, by passing `--module-paths ./my_transformers` or via:



Here is the abstract class for transformer:

```python
class Transformer:
    def __init__(self,
                 # allows us to call this transformer when base catalog is snowflake
                 source_engine: SnowflakeCatalog,
                 # if this is generic as Catalog, can automatically invoke
                 # .transform() no matter where it's running on
                 target_engine: Catalog
                 ):
        self.source_engine = source_engine
        self.target_engine = target_engine

    def transform_sql(self, expression: Expression) -> Expression:
        return expression

    def transform_result(self, response: Response):
        return response

    def transform_request(self, request: Request):
        return request
```

Here is an example:

```python
# Rewrites Snowflake timestamp time to DuckDB
class FixTimestampTypes(Transformer):
    def __init__(self, source_engine: SnowflakeCatalog, target_engine: DuckDBCatalog):
        super().__init__(source_engine, target_engine)
        
    def transform_sql(self, expression):
        if isinstance(expression, sqlglot.exp.DataType):
            if expression.this.value in ["TIMESTAMPLTZ", "TIMESTAMPTZ"]:
                return sqlglot.exp.DataType.build("TIMESTAMPTZ")
            if expression.this.value in ["VARIANT"]:
                return sqlglot.exp.DataType.build("JSON")

        return expression
```

The engine will look into the `__init__` and call the transformer only when the catalogs type match.

Things we can move to transformers:

```python
class RewriteCreateAsIceberg(Transformer):

    def transform_sql(self, expression: Expression) -> Expression:
        # re-write CREATE TABLE as CREATE ICEBERG TABLE
        return expression
```

For stage integration, something like:

```python
class SnowflakeStageTransformer(Transformer):
    def __init__(self, source_engine: SnowflakeCatalog, target_engine: DuckDBCatalog):
        super().__init__(source_engine, target_engine)

    def transform_sql(self, ast: Expression) -> Expression:
        if isinstance(ast, sqlglot.exp.Var) and ast.name.startswith('@'):
            # transform into full path and create secret on duckdb
            self.target_engine.duckdb.sql("select from stage information_schema.stages where ..")
            if not_exists:
                self.source_engine.executor().execute_raw(, "get stage info from fs"
                self.target_engine.duckdb.sql("INSERT INTO information_schema.stages ...")
            return new_ast_with_full_path
        return ast
```