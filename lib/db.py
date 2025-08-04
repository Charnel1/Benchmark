
from typing import Optional, Tuple, Any, Dict, List
import mysql.connector
from mysql.connector import Error
import logging
import streamlit as st


class DatabaseConnection:
    def __init__(self, host: str, database: str, user: str, password: str, port: int = 3306):
        """Initialize database connection parameters."""
        self.host = host
        self.database = database
        self.user = user
        self.password = password
        self.port = port
        self.connection = None
        self.cursor = None

        # Set up logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def connect(self) -> bool:
        """Establish connection to the database."""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )

            if self.connection.is_connected():
                self.cursor = self.connection.cursor(
                    dictionary=True, buffered=True)  # Changed to dictionary cursor
                db_info = self.connection.get_server_info()

                return True

        except Error as e:
            self.logger.error(f"Error connecting to MySQL database: {e}")
            return False

    def disconnect(self) -> None:
        """Close database connection and cursor."""
        if self.connection and self.connection.is_connected():
            if self.cursor:
                self.cursor.close()
            self.connection.close()

    def execute_query(self, query: str, params: Optional[Tuple] = None) -> Optional[List[Dict]]:
        """Execute a SQL query and return results with column headers."""
        try:
            if not self.connection or not self.connection.is_connected():
                self.connect()

            self.cursor.execute(query, params or ())

            if query.lower().startswith('select') or query.lower().startswith('SHOW'):
                results = self.cursor.fetchall()
                return results  # Returns list of dictionaries
            else:
                self.connection.commit()
                return {"affected_rows": self.cursor.rowcount}

        except Error as e:
            self.logger.error(f"Error executing query: {e}")
            return None

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()


@st.cache_data(ttl=6000)
def fetch_data(querry: str):
    db = DatabaseConnection(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        password="vv_webapp_2025",
        port="3306",
    )
    db.connect()
    result = db.execute_query(querry)

    db.disconnect()
    return result


def _fetch_data(querry: str):
    db = DatabaseConnection(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        password="vv_webapp_2025",
        port="3306",
    )
    db.connect()
    result = db.execute_query(querry)

    db.disconnect()
    return result


@st.cache_data(ttl=6000)
def query_data(querry: str):
    db = DatabaseConnection(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        password="vv_webapp_2025",
        port="3306",
    )
    db.connect()
    db.execute_query(querry)

    db.disconnect()


def _query_data(querry: str):
    db = DatabaseConnection(
        host="85.215.198.141",
        database="mysql",
        user="webapp",
        password="vv_webapp_2025",
        port="3306",
    )
    db.connect()
    db.execute_query(querry)

    db.disconnect()
