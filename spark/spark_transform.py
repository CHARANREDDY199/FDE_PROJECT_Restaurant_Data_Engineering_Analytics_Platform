"""
PySpark batch job:
Reads orders, order_items, and menu directly from PostgreSQL,
computes KPIs:
    - Daily revenue
    - Top dishes
    - Peak ordering hours
    - Preparation performance
    - Cancelled orders

Writes results back to the analytics_* tables
for Power BI to consume.

Run:
    python spark/spark_transform.py

Make sure:
    postgresql-42.7.3.jar
is present in the project root.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ============================================================
# PostgreSQL Configuration
# ============================================================

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "restaurant_analytics")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")


# JDBC connection URL
JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"


# JDBC connection properties
CONN_PROPS = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver"
}


# ============================================================
# Read table from PostgreSQL
# ============================================================

def read_table(spark, table):

    return spark.read.jdbc(
        url=JDBC_URL,
        table=table,
        properties=CONN_PROPS
    )


# ============================================================
# Write DataFrame to PostgreSQL
# ============================================================

def write_table(df, table, mode="overwrite"):

    df.write.jdbc(
        url=JDBC_URL,
        table=table,
        mode=mode,
        properties=CONN_PROPS
    )


# ============================================================
# Main Spark Job
# ============================================================

def main():

    # --------------------------------------------------------
    # Create Spark Session
    # --------------------------------------------------------

    spark = (
        SparkSession.builder
        .appName("RestaurantAnalyticsETL")
        .getOrCreate()
    )

    print("Spark session started.")
    print("Reading data from PostgreSQL...")


    # --------------------------------------------------------
    # Read source tables
    # --------------------------------------------------------

    orders = read_table(
        spark,
        "orders"
    )

    order_items = read_table(
        spark,
        "order_items"
    )

    menu = read_table(
        spark,
        "menu"
    )


    print("PostgreSQL tables loaded successfully.")


    # ========================================================
    # Filter completed orders
    # ========================================================

    completed_orders = orders.filter(
        F.col("status") == "completed"
    )


    # ========================================================
    # 1. DAILY REVENUE
    # ========================================================

    print("Calculating daily revenue...")

    daily_revenue = (
        completed_orders

        # Extract date from order_time
        .withColumn(
            "order_date",
            F.to_date("order_time")
        )

        # Group by date
        .groupBy("order_date")

        # Calculate KPIs
        .agg(

            F.sum("total_amount")
            .alias("total_revenue"),

            F.count("order_id")
            .alias("total_orders"),

            F.avg("total_amount")
            .alias("avg_order_value")
        )
    )


    # Write to PostgreSQL
    write_table(
        daily_revenue,
        "analytics_daily_revenue"
    )

    print(
        "analytics_daily_revenue created."
    )


    # ========================================================
    # 2. TOP DISHES
    # ========================================================

    print("Calculating top dishes...")


    # Join order_items with menu
    item_sales = (
        order_items

        .join(
            menu,
            "menu_item_id"
        )

        .groupBy(
            "menu_item_id",
            "item_name"
        )

        .agg(

            # Total quantity sold
            F.sum("quantity")
            .alias("total_quantity"),

            # Total revenue generated
            F.sum(
                F.col("quantity")
                * F.col("unit_price")
            )
            .alias("total_revenue")
        )
    )


    # --------------------------------------------------------
    # Rank dishes according to quantity sold
    # --------------------------------------------------------

    rank_window = Window.orderBy(
        F.desc("total_quantity")
    )


    top_dishes = (
        item_sales

        .withColumn(
            "rank_position",
            F.row_number().over(rank_window)
        )
    )


    # Write to PostgreSQL
    write_table(
        top_dishes,
        "analytics_top_dishes"
    )


    print(
        "analytics_top_dishes created."
    )


    # ========================================================
    # 3. PEAK ORDERING HOURS
    # ========================================================

    print("Calculating peak ordering hours...")


    peak_hours = (
        completed_orders

        # Extract hour from order_time
        .withColumn(
            "hour_of_day",
            F.hour("order_time")
        )

        # Group by hour
        .groupBy(
            "hour_of_day"
        )

        .agg(

            # Number of orders
            F.count("order_id")
            .alias("total_orders"),

            # Revenue
            F.sum("total_amount")
            .alias("total_revenue")
        )
    )


    # Write to PostgreSQL
    write_table(
        peak_hours,
        "analytics_peak_hours"
    )


    print(
        "analytics_peak_hours created."
    )


    # ========================================================
    # 4. PREPARATION PERFORMANCE
    # ========================================================

    print(
        "Calculating preparation performance..."
    )


    # Add order_date
    prep_df = (
        orders

        .withColumn(
            "order_date",
            F.to_date("order_time")
        )
    )


    # --------------------------------------------------------
    # Completed orders preparation time
    # --------------------------------------------------------

    prep_completed = (
        prep_df

        .filter(
            F.col("status") == "completed"
        )

        .withColumn(
            "prep_minutes",

            (
                F.col("prep_end_time")
                .cast("long")

                -

                F.col("prep_start_time")
                .cast("long")
            )

            / 60.0
        )
    )


    # --------------------------------------------------------
    # Aggregate preparation performance
    # --------------------------------------------------------

    prep_agg = (
        prep_completed

        .groupBy(
            "order_date"
        )

        .agg(

            # Average preparation time
            F.avg("prep_minutes")
            .alias("avg_prep_minutes"),

            # Maximum preparation time
            F.max("prep_minutes")
            .alias("max_prep_minutes")
        )
    )


    # ========================================================
    # 5. CANCELLED ORDERS
    # ========================================================

    cancelled_agg = (
        prep_df

        .filter(
            F.col("status") == "cancelled"
        )

        .groupBy(
            "order_date"
        )

        .agg(

            F.count("order_id")
            .alias("cancelled_orders")
        )
    )


    # ========================================================
    # Combine preparation + cancellation data
    # ========================================================

    prep_performance = (
        prep_agg

        .join(
            cancelled_agg,
            "order_date",
            "outer"
        )

        .fillna(0)
    )


    # Write to PostgreSQL
    write_table(
        prep_performance,
        "analytics_prep_performance"
    )


    print(
        "analytics_prep_performance created."
    )


    # ========================================================
    # Job Complete
    # ========================================================

    print()
    print("========================================")
    print("Spark transformation job complete.")
    print("========================================")


    # Stop Spark
    spark.stop()


# ============================================================
# Python Entry Point
# ============================================================

if __name__ == "__main__":
    main()