"""
Data cleaning & preprocessing step.
Reads raw CSVs from ../data, applies cleaning rules, writes cleaned CSVs
to ../data/cleaned/. Run this before load_to_postgres.py.
"""

import os
import pandas as pd

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
CLEAN_DIR = os.path.join(DATA_DIR, "cleaned")
os.makedirs(CLEAN_DIR, exist_ok=True)


def load(name):
    return pd.read_csv(os.path.join(DATA_DIR, name), parse_dates=True)


def clean_customers(df):
    df = df.drop_duplicates(subset="customer_id")
    df["email"] = df["email"].fillna("unknown@example.com")
    df["phone"] = df["phone"].fillna("N/A")
    df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce")
    return df.dropna(subset=["customer_id", "name"])


def clean_menu(df):
    df = df.drop_duplicates(subset="menu_item_id")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df[df["price"] > 0]
    return df.dropna(subset=["item_name", "price"])


def clean_orders(df):
    df = df.drop_duplicates(subset="order_id")
    for col in ["order_time", "prep_start_time", "prep_end_time"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce").fillna(0)
    df["status"] = df["status"].fillna("placed")
    df = df.dropna(subset=["order_id", "order_time"])
    return df


def clean_order_items(df, valid_order_ids, valid_menu_ids):
    df = df.drop_duplicates(subset="order_item_id")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(1).astype(int)
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df = df[df["order_id"].isin(valid_order_ids)]
    df = df[df["menu_item_id"].isin(valid_menu_ids)]
    return df.dropna(subset=["unit_price"])


def clean_payments(df, valid_order_ids):
    df = df.drop_duplicates(subset="payment_id")
    df["amount_paid"] = pd.to_numeric(df["amount_paid"], errors="coerce")
    df["payment_time"] = pd.to_datetime(df["payment_time"], errors="coerce")
    df = df[df["order_id"].isin(valid_order_ids)]
    return df.dropna(subset=["amount_paid"])


def clean_deliveries(df, valid_order_ids):
    df = df.drop_duplicates(subset="delivery_id")
    for col in ["dispatch_time", "delivered_time"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")
    df = df[df["order_id"].isin(valid_order_ids)]
    return df


def clean_reservations(df):
    df = df.drop_duplicates(subset="reservation_id")
    df["reservation_time"] = pd.to_datetime(df["reservation_time"], errors="coerce")
    df["party_size"] = pd.to_numeric(df["party_size"], errors="coerce").fillna(1).astype(int)
    return df.dropna(subset=["reservation_time"])


def main():
    customers = clean_customers(load("customers.csv"))
    menu = clean_menu(load("menu.csv"))
    tables = load("restaurant_tables.csv").drop_duplicates(subset="table_id")
    orders = clean_orders(load("orders.csv"))
    order_items = clean_order_items(load("order_items.csv"), set(orders["order_id"]), set(menu["menu_item_id"]))
    payments = clean_payments(load("payments.csv"), set(orders["order_id"]))
    deliveries = clean_deliveries(load("deliveries.csv"), set(orders["order_id"]))
    reservations = clean_reservations(load("reservations.csv"))

    customers.to_csv(f"{CLEAN_DIR}/customers.csv", index=False)
    menu.to_csv(f"{CLEAN_DIR}/menu.csv", index=False)
    tables.to_csv(f"{CLEAN_DIR}/restaurant_tables.csv", index=False)
    orders.to_csv(f"{CLEAN_DIR}/orders.csv", index=False)
    order_items.to_csv(f"{CLEAN_DIR}/order_items.csv", index=False)
    payments.to_csv(f"{CLEAN_DIR}/payments.csv", index=False)
    deliveries.to_csv(f"{CLEAN_DIR}/deliveries.csv", index=False)
    reservations.to_csv(f"{CLEAN_DIR}/reservations.csv", index=False)

    print("Cleaning complete. Cleaned files written to", CLEAN_DIR)


if __name__ == "__main__":
    main()
