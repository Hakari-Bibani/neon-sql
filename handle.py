import psycopg2
import pandas as pd
from psycopg2 import sql
from psycopg2.extras import execute_batch

class DatabaseHandler:
    def __init__(self, connection_string):
        self.conn = None
        self.connection_string = connection_string
        self._initialize_database()

    def _initialize_database(self):
        """Initialize database connection and ensure table exists"""
        try:
            self.conn = psycopg2.connect(self.connection_string)
            with self.conn.cursor() as cur:
                # Create table if not exists
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS table1 (
                        name TEXT,
                        age INTEGER,
                        status TEXT
                    )
                """)
                self.conn.commit()
        except Exception as e:
            raise Exception(f"Database initialization failed: {e}")

    def insert_record(self, name, age, status):
        """Insert a single record into the database"""
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO table1 (name, age, status) VALUES (%s, %s, %s)",
                    (name, age, status)
                )
                self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Failed to insert record: {e}")

    def bulk_insert(self, df):
        """Insert multiple records from a pandas DataFrame"""
        try:
            records = df[["name", "age", "status"]].to_records(index=False)
            with self.conn.cursor() as cur:
                execute_batch(
                    cur,
                    "INSERT INTO table1 (name, age, status) VALUES (%s, %s, %s)",
                    records
                )
                self.conn.commit()
            return len(records)
        except Exception as e:
            self.conn.rollback()
            raise Exception(f"Bulk insert failed: {e}")

    def fetch_all(self):
        """Fetch all records from the database"""
        try:
            with self.conn.cursor() as cur:
                cur.execute("SELECT name, age, status FROM table1")
                return cur.fetchall()
        except Exception as e:
            raise Exception(f"Failed to fetch records: {e}")

    def __del__(self):
        """Close database connection when object is destroyed"""
        if self.conn:
            self.conn.close()
