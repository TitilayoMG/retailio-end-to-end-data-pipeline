import gzip
import logging
from contextlib import contextmanager
from airflow.hooks.base import BaseHook
import psycopg2
import gzip
import csv
import io
import logging



logger = logging.getLogger(__name__)


class PostgresClient:
    def __init__(self, conn_id: str):
        self.conn_id = conn_id
        self.conn = self._get_connection()

    def _get_connection(self):
        conn = BaseHook.get_connection(self.conn_id)

        return psycopg2.connect(
            host=conn.host,
            port=conn.port,
            dbname=conn.schema,
            user=conn.login,
            password=conn.password
        )

    @contextmanager
    def session(self):
        """
        Generic DB session (for queries)
        """
        cur = self.conn.cursor()
        try:
            yield cur
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Query failed: {e}")
            raise
        finally:
            cur.close()

    # =========================
    # 🔹 QUERY EXECUTION
    # =========================
    def execute(self, query: str, params: tuple = None):
        with self.session() as cur:
            cur.execute(query, params)

    def execute_many(self, query: str, params_list: list):
        with self.session() as cur:
            cur.executemany(query, params_list)

    def fetch_one(self, query: str, params: tuple = None):
        with self.session() as cur:
            cur.execute(query, params)
            return cur.fetchone()

    def fetch_all(self, query: str, params: tuple = None):
        with self.session() as cur:
            cur.execute(query, params)
            return cur.fetchall()

    # =========================
    # 🔹 SQL FILE EXECUTION
    # =========================
    def execute_file(self, file_path: str):
        """
        Execute SQL from a .sql file
        """
        with open(file_path, "r") as f:
            sql = f.read()

        with self.session() as cur:
            cur.execute(sql)

        logger.info(f"Executed SQL file: {file_path}")

    # =========================
    # 🔹 TABLE MANAGEMENT
    # =========================
    def truncate(self, table: str):
        self.execute(f"TRUNCATE TABLE {table}")

    def drop_table(self, table: str):
        self.execute(f"DROP TABLE IF EXISTS {table}")

    def create_table(self, ddl: str):
        """
        Pass full CREATE TABLE statement
        """
        self.execute(ddl)

    # =========================
    # 🔹 BULK LOAD (COPY)
    # =========================
    @contextmanager
    def copy_session(self):
        cur = self.conn.cursor()
        try:
            yield cur
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            logger.error(f"COPY failed: {e}")
            raise
        finally:
            cur.close()

    def copy_from_buffer(self, table: str, buffer, columns: list = None):
        cols = f"({', '.join(columns)})" if columns else ""

        query = f"""
            COPY {table} {cols}
            FROM STDIN
            WITH (FORMAT csv, HEADER TRUE)
        """

        with self.copy_session() as cur:
            cur.copy_expert(query, buffer)

        logger.info(f"Loaded data into {table}")

    # def load_gzip_csv(self, table: str, gzip_stream, columns: list = None):
    #     with gzip.open(gzip_stream, mode="rt", encoding="utf-8") as f:
    #         self.copy_from_buffer(table, f, columns)

    def load_gzip_csv(self, table: str, gzip_stream, columns: list = None):
        try:
            with gzip.GzipFile(fileobj=gzip_stream) as gz:
                decoded = io.TextIOWrapper(gz, encoding="utf-8")

                self.copy_from_buffer(table, decoded, columns)

        except Exception as e:
            logger.error(f"Error loading gzip CSV: {e}")
            raise

        finally:
            if hasattr(gzip_stream, "close"):
                gzip_stream.close()

                
    def read_gzip_csv(self, gzip_stream, columns: list):
        """
        Read a gzip CSV stream from MinIO and return rows as list of tuples
        (for UPSERT operations)

        Args:
            gzip_stream: MinIO object response stream
            columns (list): Ordered list of columns expected in DB

        Returns:
            List[Tuple]: Data ready for execute_many / upsert
        """

        data = []

        try:
            # Decompress stream
            with gzip.GzipFile(fileobj=gzip_stream) as gz:
                # Convert bytes → text
                decoded = io.TextIOWrapper(gz, encoding="utf-8")

                reader = csv.DictReader(decoded)

                for row in reader:
                    # Ensure order matches DB columns
                    record = tuple(row[col] for col in columns)
                    data.append(record)

            logger.info(f"Read {len(data)} records from gzip file")

        except Exception as e:
            logger.error(f"Error reading gzip CSV: {str(e)}")
            raise

        finally:
            if hasattr(gzip_stream, "close"):
                gzip_stream.close()

        return data

    # =========================
    # 🔹 ADVANCED (OPTIONAL)
    # =========================
    def upsert(self, table: str, data: list, columns: list, conflict_cols: list):
        """
        Generic UPSERT
        """
        cols = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))

        # update_clause = ", ".join(
        #     [f"{col}=EXCLUDED.{col}" for col in columns]
        # )
        update_clause = ", ".join(
            [f"{col}=EXCLUDED.{col}" for col in columns if col not in conflict_cols]
        )

        conflict = ", ".join(conflict_cols)

        query = f"""
            INSERT INTO {table} ({cols})
            VALUES ({placeholders})
            ON CONFLICT ({conflict})
            DO UPDATE SET {update_clause}
        """

        self.execute_many(query, data)


    def is_file_loaded(self, file_name: str) -> bool:
        result = self.fetch_one(
            "SELECT 1 FROM ingestion_tracker WHERE file_name = %s",
            (file_name,)
        )
        return result is not None


    def track_loaded_file(self, file_name: str, dataset: str):
        self.execute(
            """
            INSERT INTO ingestion_tracker (file_name, dataset)
            VALUES (%s, %s)
            ON CONFLICT (file_name) DO NOTHING
            """,
            (file_name, dataset)
        )