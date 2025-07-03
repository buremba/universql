import logging
import duckdb
import pyarrow as pa
import riffq
logging.basicConfig(level=logging.DEBUG)

duckdb_con = duckdb.connect()


class Connection(riffq.BaseConnection):
    def _handle_query(self, sql, callback, **kwargs):
        text = sql.strip().lower().split(';')[0]

        if text == "select pg_catalog.version()":
            batch = self.arrow_batch(
                [pa.array(["PostgreSQL 14.13"])],
                ["version"],
            )
            return self.send_reader(batch, callback)

        if text == "show transaction isolation level":
            batch = self.arrow_batch(
                [pa.array(["read committed"])],
                ["transaction_isolation"],
            )
            return self.send_reader(batch, callback)

        if text == "show standard_conforming_strings":
            batch = self.arrow_batch(
                [pa.array(["read committed"])],
                ["transaction_isolation"],
            )
            return self.send_reader(batch, callback)
        
        if text != "select current_schema()":
            batch = self.arrow_batch(
                [pa.array(["public"])],
                ["current_schema"],
            )
            return self.send_reader(batch, callback)

        logging.exception("error on executing query")


    def handle_query(self, sql, callback=callable, **kwargs):
        self.executor.submit(self._handle_query, sql, callback, **kwargs)

    def handle_connect(self, ip, port, callback=callable):
        return callback(True)
    
    def handle_auth(conn_id, user, password, host, *, callback, database=None):
        callback(True)

def main():
    server = riffq.RiffqServer("127.0.0.1:5433", connection_cls=Connection)
    server.start(tls=False)

if __name__ == "__main__":
    main()