import sqlite3
import time
from datetime import datetime
from pathlib import Path

import streamlit as st


DB_PATH = Path("shop.db")


def read_products() -> list:
    if not DB_PATH.exists():
        return []

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT id, name, category, price, stock
        FROM products
        ORDER BY id
        """
    ).fetchall()
    conn.close()

    return [dict(row) for row in rows]


def read_sales() -> list:
    if not DB_PATH.exists():
        return []

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT id, product_name, category, quantity, unit_price, total, sold_at
        FROM sales
        ORDER BY id DESC
        """
    ).fetchall()
    conn.close()

    return [dict(row) for row in rows]


def read_partners() -> list:
    if not DB_PATH.exists():
        return []

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        """
        SELECT id, name, category, lead_time_days, min_order_units, discount_percent
        FROM partners
        ORDER BY id
        """
    ).fetchall()
    conn.close()

    return [dict(row) for row in rows]


st.set_page_config(page_title="Shop DB Monitor", layout="wide")
st.title("Shop DB Monitor")

refresh_seconds = st.sidebar.number_input(
    "Refresh seconds",
    min_value=1,
    max_value=60,
    value=2,
)
auto_refresh = st.sidebar.toggle("Auto refresh", value=True)

if st.sidebar.button("Refresh now"):
    st.rerun()

if not DB_PATH.exists():
    st.warning("shop.db does not exist yet. Run shop-ops-agent.py once to create it.")
else:
    products = read_products()
    sales = read_sales()
    partners = read_partners()
    total_stock = sum(row["stock"] for row in products)
    total_value = sum(row["price"] * row["stock"] for row in products)
    total_revenue = sum(row["total"] for row in sales)
    categories = len({row["category"] for row in products})

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Products", len(products))
    col2.metric("Categories", categories)
    col3.metric("Inventory value", f"${total_value:,.2f}")
    col4.metric("Sales revenue", f"${total_revenue:,.2f}")

    products_tab, sales_tab, partners_tab = st.tabs(["Products", "Sales", "Partners"])

    with products_tab:
        st.dataframe(products, use_container_width=True, hide_index=True)

    with sales_tab:
        st.dataframe(sales, use_container_width=True, hide_index=True)

    with partners_tab:
        st.dataframe(partners, use_container_width=True, hide_index=True)

    st.caption(
        f"Total stock: {total_stock} | Last refresh: "
        f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )

if auto_refresh:
    time.sleep(refresh_seconds)
    st.rerun()
