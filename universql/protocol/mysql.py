import asyncio

from mysql_mimic import MysqlServer, Session


class MySession(Session):
    async def query(self, expression, sql, attrs):
        print(f"Parsed abstract syntax tree: {expression}")
        print(f"Original SQL string: {sql}")
        print(f"Query attributes: {sql}")
        print(f"Currently authenticated user: {self.username}")
        print(f"Currently selected database: {self.database}")
        return [("a", 1), ("b", 2)], ["col1", "col2"]

    async def schema(self):
        # Optionally provide the database schema.
        # This is used to serve INFORMATION_SCHEMA and SHOW queries.
        return {
            "table": {
                "col1": "TEXT",
                "col2": "INT",
            }
        }

if __name__ == "__main__":
    server = MysqlServer(session_factory=MySession)
    asyncio.run(server.serve_forever())