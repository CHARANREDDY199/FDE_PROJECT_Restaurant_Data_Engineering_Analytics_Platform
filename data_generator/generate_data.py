"""
Generates synthetic restaurant data (customers, menu, tables, reservations,
orders, order_items, payments, deliveries) and writes them as CSV files
under ../data/. Used to seed the pipeline when no real POS export exists.
"""

import os
import random
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
os.makedirs(OUT_DIR, exist_ok=True)

N_CUSTOMERS = 500
N_MENU_ITEMS = 40
N_TABLES = 20
N_ORDERS = 5000

CATEGORIES = ["Starters", "Main Course", "Desserts", "Beverages", "Sides"]
ORDER_TYPES = ["dine_in", "takeaway", "delivery"]
PAYMENT_METHODS = ["card", "cash", "upi", "wallet"]


def gen_customers():
    rows = []
    for cid in range(1, N_CUSTOMERS + 1):
        rows.append({
            "customer_id": cid,
            "name": fake.name(),
            "email": fake.unique.email(),
            "phone": fake.phone_number(),
            "signup_date": fake.date_between(start_date="-2y", end_date="-1d"),
            "city": fake.city(),
        })
    return pd.DataFrame(rows)


def gen_menu():
    rows = []
    for mid in range(1, N_MENU_ITEMS + 1):
        rows.append({
            "menu_item_id": mid,
            "item_name": fake.unique.word().capitalize() + " " + random.choice(
                ["Curry", "Pizza", "Burger", "Salad", "Pasta", "Soup", "Cake", "Juice"]
            ),
            "category": random.choice(CATEGORIES),
            "price": round(random.uniform(3, 25), 2),
            "is_active": True,
        })
    return pd.DataFrame(rows)


def gen_tables():
    rows = [{"table_id": i, "table_number": i, "capacity": random.choice([2, 4, 6, 8])}
            for i in range(1, N_TABLES + 1)]
    return pd.DataFrame(rows)


def gen_orders_and_children(menu_df, n_orders=N_ORDERS):
    orders, items, payments, deliveries = [], [], [], []
    order_item_id = 1
    payment_id = 1
    delivery_id = 1

    for oid in range(1, n_orders + 1):
        order_time = fake.date_time_between(start_date="-90d", end_date="now")
        order_type = random.choice(ORDER_TYPES)
        status = random.choices(
            ["completed", "cancelled", "preparing"], weights=[0.85, 0.08, 0.07]
        )[0]

        prep_start = order_time + timedelta(minutes=random.randint(1, 5))
        prep_minutes = random.randint(5, 40)
        prep_end = prep_start + timedelta(minutes=prep_minutes)

        n_items = random.randint(1, 5)
        chosen_items = menu_df.sample(n_items)
        total_amount = 0.0

        for _, item in chosen_items.iterrows():
            qty = random.randint(1, 3)
            line_total = round(qty * float(item["price"]), 2)
            total_amount += line_total
            items.append({
                "order_item_id": order_item_id,
                "order_id": oid,
                "menu_item_id": int(item["menu_item_id"]),
                "quantity": qty,
                "unit_price": item["price"],
            })
            order_item_id += 1

        orders.append({
            "order_id": oid,
            "customer_id": random.randint(1, N_CUSTOMERS),
            "table_id": random.randint(1, N_TABLES) if order_type == "dine_in" else None,
            "order_time": order_time,
            "order_type": order_type,
            "prep_start_time": prep_start,
            "prep_end_time": prep_end if status != "cancelled" else None,
            "status": status,
            "total_amount": round(total_amount, 2),
        })

        if status != "cancelled":
            payments.append({
                "payment_id": payment_id,
                "order_id": oid,
                "payment_method": random.choice(PAYMENT_METHODS),
                "amount_paid": round(total_amount, 2),
                "payment_time": prep_end + timedelta(minutes=random.randint(1, 10)),
            })
            payment_id += 1

        if order_type == "delivery" and status != "cancelled":
            dispatch = prep_end + timedelta(minutes=random.randint(2, 10))
            delivered = dispatch + timedelta(minutes=random.randint(10, 45))
            deliveries.append({
                "delivery_id": delivery_id,
                "order_id": oid,
                "dispatch_time": dispatch,
                "delivered_time": delivered,
                "delivery_status": "delivered",
            })
            delivery_id += 1

    return (
        pd.DataFrame(orders),
        pd.DataFrame(items),
        pd.DataFrame(payments),
        pd.DataFrame(deliveries),
    )


def gen_reservations():
    rows = []
    for rid in range(1, 300):
        rows.append({
            "reservation_id": rid,
            "customer_id": random.randint(1, N_CUSTOMERS),
            "table_id": random.randint(1, N_TABLES),
            "reservation_time": fake.date_time_between(start_date="-60d", end_date="+10d"),
            "party_size": random.randint(1, 8),
            "status": random.choice(["confirmed", "cancelled", "completed"]),
        })
    return pd.DataFrame(rows)


def main():
    customers = gen_customers()
    menu = gen_menu()
    tables = gen_tables()
    orders, order_items, payments, deliveries = gen_orders_and_children(menu)
    reservations = gen_reservations()

    customers.to_csv(f"{OUT_DIR}/customers.csv", index=False)
    menu.to_csv(f"{OUT_DIR}/menu.csv", index=False)
    tables.to_csv(f"{OUT_DIR}/restaurant_tables.csv", index=False)
    orders.to_csv(f"{OUT_DIR}/orders.csv", index=False)
    order_items.to_csv(f"{OUT_DIR}/order_items.csv", index=False)
    payments.to_csv(f"{OUT_DIR}/payments.csv", index=False)
    deliveries.to_csv(f"{OUT_DIR}/deliveries.csv", index=False)
    reservations.to_csv(f"{OUT_DIR}/reservations.csv", index=False)

    print(f"Generated data written to {OUT_DIR}")
    print(f"  customers={len(customers)} menu={len(menu)} tables={len(tables)} "
          f"orders={len(orders)} order_items={len(order_items)} "
          f"payments={len(payments)} deliveries={len(deliveries)} "
          f"reservations={len(reservations)}")


if __name__ == "__main__":
    main()
