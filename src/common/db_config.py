import os, pyodbc
from dotenv import load_dotenv

load_dotenv()

def get_sql_connection():
    conn_str = (
        f"DRIVER={{{os.getenv('SQL_DRIVER')}}};"
        f"SERVER={os.getenv('SQL_SERVER')};"
        f"DATABASE={os.getenv('SQL_DATABASE')};"
        f"Trusted_Connection={os.getenv('SQL_TRUSTED_CONNECTION')};"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)

if __name__ == "__main__":
    print(get_sql_connection().cursor().execute("SELECT @@VERSION").fetchone()[0])