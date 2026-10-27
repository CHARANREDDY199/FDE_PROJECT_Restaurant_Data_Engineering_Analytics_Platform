"""
Loads cleaned CSV files from data/cleaned/
into PostgreSQL database: restaurant_analytics

Run:
    python etl\load_to_postgres.py
"""

import os
import pandas as pd
from sqlalchemy import create_engine


# --------------------------------------------------
# 1. CLEANED DATA LOCATION
# --------------------------------------------------

CLEAN_DIR = os.path.join(
    os.path.dirname(__file__),
    "..",
    "data",
    "cleaned"
)


# --------------------------------------------------
# 2. POSTGRESQL DATABASE CONFIGURATION
# --------------------------------------------------

DB_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": os.getenv("PG_PORT", "5034"),   # IMPORTANT
    "db": os.getenv("PG_DB", "restaurant_analytics"),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgres"),
}


# --------------------------------------------------
# 3. DATABASE CONNECTION URL
# --------------------------------------------------

ENGINE_URL = (
    f"postgresql+psycopg2://"
    f"{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}"
    f"/{DB_CONFIG['db']}"
)


# --------------------------------------------------
# 4. TABLE LOAD ORDER
# --------------------------------------------------

LOAD_ORDER = [
    ("customers.csv", "customers"),
    ("menu.csv", "menu"),
    ("restaurant_tables.csv", "restaurant_tables"),
    ("reservations.csv", "reservations"),
    ("orders.csv", "orders"),
    ("order_items.csv", "order_items"),
    ("payments.csv", "payments"),
    ("deliveries.csv", "deliveries"),
]


# --------------------------------------------------
# 5. MAIN FUNCTION
# --------------------------------------------------

def main():

    print("----------------------------------------")
    print("Restaurant Data Loading Started")
    print("----------------------------------------")

    print(f"Host     : {DB_CONFIG['host']}")
    print(f"Port     : {DB_CONFIG['port']}")
    print(f"Database : {DB_CONFIG['db']}")
    print(f"User     : {DB_CONFIG['user']}")
    print("----------------------------------------")

    # Create database connection
    engine = create_engine(ENGINE_URL)

    # Test and load data
    with engine.begin() as conn:

        for filename, table in LOAD_ORDER:

            path = os.path.join(CLEAN_DIR, filename)

            # Check whether file exists
            if not os.path.exists(path):
                print(f"File not found: {path}")
                continue

            # Read cleaned CSV
            df = pd.read_csv(path)

            # Load into PostgreSQL
            df.to_sql(
                table,
                conn,
                if_exists="append",
                index=False,
                method="multi",
                chunksize=500
            )

            print(
                f"Loaded {len(df):>5} rows -> {table}"
            )

    print("----------------------------------------")
    print("Load complete.")
    print("----------------------------------------")


# --------------------------------------------------
# 6. PROGRAM START
# --------------------------------------------------

if __name__ == "__main__":
    main()