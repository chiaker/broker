import os
import time
from decimal import Decimal

import psycopg2
from psycopg2 import sql


def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "db"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "shop"),
        user=os.getenv("DB_USER", "shop_user"),
        password=os.getenv("DB_PASSWORD", "shop_password"),
    )


def wait_for_db(max_attempts=30, delay_seconds=2):
    for _ in range(max_attempts):
        try:
            conn = get_connection()
            conn.close()
            return
        except psycopg2.OperationalError:
            time.sleep(delay_seconds)
    raise RuntimeError("Database is not available")


def scenario_1_place_order(conn, customer_id, items):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into orders (customerid, orderdate, totalamount)
                values (%s, now(), 0)
                returning orderid
                """,
                (customer_id,),
            )
            order_id = cur.fetchone()[0]

            for product_id, quantity in items:
                cur.execute("select price from products where productid = %s", (product_id,))
                row = cur.fetchone()
                if row is None:
                    raise ValueError(f"Product {product_id} not found")
                price = row[0]
                subtotal = (price * Decimal(quantity)).quantize(Decimal("0.01"))
                cur.execute(
                    """
                    insert into orderitems (orderid, productid, quantity, subtotal)
                    values (%s, %s, %s, %s)
                    """,
                    (order_id, product_id, quantity, subtotal),
                )

            cur.execute(
                """
                update orders
                set totalamount = (
                    select coalesce(sum(subtotal), 0)
                    from orderitems
                    where orderid = %s
                )
                where orderid = %s
                returning totalamount
                """,
                (order_id, order_id),
            )
            total_amount = cur.fetchone()[0]
            return order_id, total_amount


def scenario_2_update_customer_email(conn, customer_id, new_email):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update customers
                set email = %s
                where customerid = %s
                returning customerid, firstname, lastname, email
                """,
                (new_email, customer_id),
            )
            updated = cur.fetchone()
            if updated is None:
                raise ValueError(f"Customer {customer_id} not found")
            return updated


def scenario_3_add_product(conn, product_name, price):
    with conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into products (productname, price)
                values (%s, %s)
                returning productid, productname, price
                """,
                (product_name, price),
            )
            return cur.fetchone()


def print_table(conn, table_name):
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("select * from {} order by 1").format(sql.Identifier(table_name.lower()))
        )
        rows = cur.fetchall()
        print(f"\n{table_name}:")
        for row in rows:
            print(row)


def main():
    wait_for_db()
    conn = get_connection()
    try:
        order_id, total_amount = scenario_1_place_order(
            conn,
            customer_id=1,
            items=[(1, 1), (2, 2), (3, 1)],
        )
        print(f"Scenario 1: order {order_id} created with total {total_amount}")

        updated_customer = scenario_2_update_customer_email(
            conn,
            customer_id=1,
            new_email="ivan.petrov.updated@example.com",
        )
        print(f"Scenario 2: customer updated -> {updated_customer}")

        new_product = scenario_3_add_product(
            conn,
            product_name="Webcam",
            price=Decimal("150.00"),
        )
        print(f"Scenario 3: product added -> {new_product}")

        print_table(conn, "Customers")
        print_table(conn, "Products")
        print_table(conn, "Orders")
        print_table(conn, "OrderItems")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
