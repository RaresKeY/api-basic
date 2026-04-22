import argparse
import json
import sqlite3
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from google import genai
from google.genai import types
from google.genai.errors import ServerError


MODEL = "gemma-4-26b-a4b-it"
EXAMPLE_DB_PATH = Path("examples/shop-example.db")
DB_PATH = Path("shop.db")
LOG_PATH = Path("db-fetch-log.md")
REPORT_PATH = Path("final-report.md")
RETRY_STATUS_CODES = {500, 502, 503, 504}


def rows_to_dicts(rows) -> list:
    return [dict(row) for row in rows]


def create_shop_db(db_path: Path = DB_PATH) -> None:
    if db_path.exists():
        return

    if EXAMPLE_DB_PATH.exists():
        db_path.write_bytes(EXAMPLE_DB_PATH.read_bytes())
        return

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            category TEXT NOT NULL,
            price REAL NOT NULL,
            stock INTEGER NOT NULL
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE sales (
            id INTEGER PRIMARY KEY,
            product_id INTEGER NOT NULL,
            product_name TEXT NOT NULL,
            category TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            total REAL NOT NULL,
            sold_at TEXT NOT NULL,
            note TEXT NOT NULL,
            FOREIGN KEY (product_id) REFERENCES products (id)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE partners (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            lead_time_days INTEGER NOT NULL,
            min_order_units INTEGER NOT NULL,
            discount_percent REAL NOT NULL
        )
        """
    )
    cursor.executemany(
        """
        INSERT INTO products (name, category, price, stock)
        VALUES (?, ?, ?, ?)
        """,
        [
            ("Wireless Mouse", "electronics", 24.99, 35),
            ("USB-C Hub", "electronics", 49.99, 18),
            ("Noise Cancelling Headphones", "electronics", 129.99, 8),
            ("Desk Lamp", "home", 34.5, 21),
            ("Ceramic Mug", "home", 12.0, 50),
            ("Cotton T-Shirt", "clothing", 18.99, 42),
            ("Rain Jacket", "clothing", 79.99, 13),
            ("Python Pocket Guide", "books", 15.99, 27),
            ("Notebook", "books", 6.5, 80),
            ("Organic Coffee", "grocery", 14.25, 24),
            ("NovaTech TV", "electronics", 499.99, 25),
            ("NovaTech Fridge", "electronics", 899.99, 80),
            ("NovaTech Washing Machine", "electronics", 699.99, 45),
            ("NovaTech Microwave", "electronics", 149.99, 12),
        ],
    )
    cursor.execute(
        """
        INSERT INTO partners
            (name, category, lead_time_days, min_order_units, discount_percent)
        VALUES (?, ?, ?, ?, ?)
        """,
        ("NovaTech", "electronics", 14, 50, 8.0),
    )
    conn.commit()
    conn.close()


def append_log(path: Path, title: str, value=None) -> None:
    with path.open("a", encoding="utf-8") as log_file:
        log_file.write(f"\n### {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {title}\n")

        if value is None:
            return

        if isinstance(value, (dict, list)):
            log_file.write("```json\n")
            log_file.write(json.dumps(value, indent=2))
            log_file.write("\n```\n")
        else:
            log_file.write("```text\n")
            log_file.write(str(value))
            log_file.write("\n```\n")


def debug_print(title: str, value, enabled: bool) -> None:
    if not enabled:
        return

    print(f"\n[debug] {title}")
    if isinstance(value, (dict, list)):
        print(json.dumps(value, indent=2))
    else:
        print(value)


def get_status_code(error: ServerError):
    status_code = getattr(error, "status_code", None) or getattr(error, "code", None)
    if status_code is not None:
        return int(status_code)

    error_text = str(error)
    for code in RETRY_STATUS_CODES:
        if str(code) in error_text:
            return code

    return None


def get_text(response) -> str:
    parts = []
    for candidate in response.candidates or []:
        if not candidate.content:
            continue
        for part in candidate.content.parts or []:
            if part.text:
                parts.append(part.text)

    return "\n".join(parts).strip()


def get_function_calls(response) -> list:
    calls = []
    for candidate in response.candidates or []:
        if not candidate.content:
            continue
        for part in candidate.content.parts or []:
            if part.function_call:
                calls.append(part.function_call)

    return calls


def fetch_inventory(db_path: Path, category: str = "all") -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    params = []
    sql = """
        SELECT name, category, price, stock
        FROM products
        WHERE 1 = 1
    """
    if category != "all":
        sql += " AND category = ?"
        params.append(category)
    sql += " ORDER BY category, name"

    rows = conn.execute(sql, params).fetchall()
    conn.close()

    return {"products": rows_to_dicts(rows)}


def record_sales(db_path: Path, sales: list[dict], note: str) -> dict:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    updates = []

    for item in sales:
        product_name = item["product_name"]
        quantity = int(item["quantity"])
        product = conn.execute(
            """
            SELECT id, name, category, price, stock
            FROM products
            WHERE lower(name) = lower(?)
            LIMIT 1
            """,
            (product_name,),
        ).fetchone()

        if not product:
            updates.append({"product_name": product_name, "error": "not found"})
            continue

        sold = max(0, min(quantity, product["stock"]))
        total = round(sold * product["price"], 2)
        new_stock = product["stock"] - sold
        sold_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("UPDATE products SET stock = ? WHERE id = ?", (new_stock, product["id"]))
        conn.execute(
            """
            INSERT INTO sales
                (product_id, product_name, category, quantity, unit_price, total, sold_at, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                product["id"],
                product["name"],
                product["category"],
                sold,
                product["price"],
                total,
                sold_at,
                note,
            ),
        )
        updates.append(
            {
                "product_name": product["name"],
                "requested_quantity": quantity,
                "quantity_sold": sold,
                "stock_before": product["stock"],
                "stock_after": new_stock,
                "revenue": total,
            }
        )

    conn.commit()
    conn.close()

    return {"updates": updates}


def get_sales_stats(db_path: Path, month: str, category: str) -> dict:
    if month == "current":
        month = datetime.now().strftime("%Y-%m")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    params = [f"{month}%"]
    category_filter = ""
    if category != "all":
        category_filter = "AND category = ?"
        params.append(category)

    summary = conn.execute(
        f"""
        SELECT
            COALESCE(SUM(quantity), 0) AS units_sold,
            COALESCE(SUM(total), 0) AS revenue,
            COUNT(*) AS sale_lines
        FROM sales
        WHERE sold_at LIKE ?
        {category_filter}
        """,
        params,
    ).fetchone()
    by_product = conn.execute(
        f"""
        SELECT product_name, category, SUM(quantity) AS units_sold, SUM(total) AS revenue
        FROM sales
        WHERE sold_at LIKE ?
        {category_filter}
        GROUP BY product_name, category
        ORDER BY revenue DESC
        """,
        params,
    ).fetchall()
    conn.close()

    return {
        "month": month,
        "category": category,
        "units_sold": int(summary["units_sold"]),
        "revenue": round(float(summary["revenue"]), 2),
        "sale_lines": int(summary["sale_lines"]),
        "by_product": rows_to_dicts(by_product),
    }


def plan_partner_order(db_path: Path, partner_name: str, category: str) -> dict:
    month = datetime.now().strftime("%Y-%m")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    partner = conn.execute(
        """
        SELECT name, category, lead_time_days, min_order_units, discount_percent
        FROM partners
        WHERE lower(name) = lower(?) AND category = ?
        LIMIT 1
        """,
        (partner_name, category),
    ).fetchone()
    rows = conn.execute(
        """
        SELECT p.name, p.category, p.price, p.stock,
            COALESCE(SUM(s.quantity), 0) AS monthly_units_sold
        FROM products p
        LEFT JOIN sales s
            ON s.product_id = p.id
            AND s.sold_at LIKE ?
        WHERE p.category = ?
        GROUP BY p.id
        ORDER BY p.stock ASC, monthly_units_sold DESC
        """,
        (f"{month}%", category),
    ).fetchall()
    conn.close()

    recommendations = []
    for row in rows:
        monthly_units_sold = int(row["monthly_units_sold"])
        target_stock = max(20, monthly_units_sold * 2)
        order_units = max(0, target_stock - row["stock"])
        if order_units == 0:
            continue

        recommendations.append(
            {
                "product_name": row["name"],
                "current_stock": row["stock"],
                "monthly_units_sold": monthly_units_sold,
                "suggested_order_units": order_units,
                "estimated_cost": round(order_units * row["price"], 2),
                "priority": "urgent" if row["stock"] == 0 else "normal",
            }
        )

    total_units = sum(item["suggested_order_units"] for item in recommendations)
    total_cost = round(sum(item["estimated_cost"] for item in recommendations), 2)

    return {
        "partner": dict(partner) if partner else {"name": partner_name},
        "category": category,
        "recommendations": recommendations,
        "total_suggested_units": total_units,
        "estimated_cost_before_partner_discount": total_cost,
    }


def build_tool() -> types.Tool:
    return types.Tool(
        function_declarations=[
            types.FunctionDeclaration(
                name="fetch_inventory",
                description="Fetch current product inventory from the local SQLite shop database.",
                parameters=types.Schema(
                    type=types.Type.OBJECT,
                    properties={
                        "category": types.Schema(
                            type=types.Type.STRING,
                            enum=["all", "electronics", "home", "clothing", "books", "grocery"],
                        )
                    },
                    required=["category"],
                ),
            ),
            types.FunctionDeclaration(
                name="record_sales",
                description=(
                    "Record product sales and reduce inventory. Use exact product names "
                    "from the inventory, and pass one item for each sold product."
                ),
                parameters=types.Schema(
                    type=types.Type.OBJECT,
                    properties={
                        "sales": types.Schema(
                            type=types.Type.ARRAY,
                            items=types.Schema(
                                type=types.Type.OBJECT,
                                properties={
                                    "product_name": types.Schema(type=types.Type.STRING),
                                    "quantity": types.Schema(
                                        type=types.Type.INTEGER,
                                        minimum=0,
                                    ),
                                },
                                required=["product_name", "quantity"],
                            ),
                        ),
                        "note": types.Schema(type=types.Type.STRING),
                    },
                    required=["sales", "note"],
                ),
            ),
            types.FunctionDeclaration(
                name="get_sales_stats",
                description="Get monthly sales statistics from the local SQLite shop database.",
                parameters=types.Schema(
                    type=types.Type.OBJECT,
                    properties={
                        "month": types.Schema(
                            type=types.Type.STRING,
                            description="Month in YYYY-MM format, or current.",
                        ),
                        "category": types.Schema(
                            type=types.Type.STRING,
                            enum=["all", "electronics", "home", "clothing", "books", "grocery"],
                        ),
                    },
                    required=["month", "category"],
                ),
            ),
            types.FunctionDeclaration(
                name="plan_partner_order",
                description="Plan a partner restock order using current stock and sales data.",
                parameters=types.Schema(
                    type=types.Type.OBJECT,
                    properties={
                        "partner_name": types.Schema(type=types.Type.STRING),
                        "category": types.Schema(
                            type=types.Type.STRING,
                            enum=["electronics", "home", "clothing", "books", "grocery"],
                        ),
                    },
                    required=["partner_name", "category"],
                ),
            ),
        ]
    )


def run_tool(db_path: Path, function_call) -> dict:
    args = dict(function_call.args)

    if function_call.name == "fetch_inventory":
        result = fetch_inventory(db_path, **args)
    elif function_call.name == "record_sales":
        result = record_sales(db_path, **args)
    elif function_call.name == "get_sales_stats":
        result = get_sales_stats(db_path, **args)
    elif function_call.name == "plan_partner_order":
        result = plan_partner_order(db_path, **args)
    else:
        raise ValueError(f"Unknown tool: {function_call.name}")

    return {"name": function_call.name, "args": args, "result": result}


def make_function_response(function_call, result: dict) -> types.Content:
    return types.Content(
        role="user",
        parts=[
            types.Part(
                function_response=types.FunctionResponse(
                    id=getattr(function_call, "id", None),
                    name=function_call.name,
                    response={"result": result},
                )
            )
        ],
    )


def generate_with_retries(client, model: str, contents: list, config, retries: int = 3):
    for attempt in range(retries + 1):
        try:
            return client.models.generate_content(
                model=model,
                contents=contents,
                config=config,
            )
        except ServerError as error:
            status_code = get_status_code(error)
            if status_code not in RETRY_STATUS_CODES or attempt == retries:
                raise
            time.sleep(2**attempt)


def run_agent(prompt: str, model: str, debug: bool) -> str:
    load_dotenv()
    create_shop_db(DB_PATH)
    client = genai.Client()
    tool = build_tool()
    contents = [
        types.Content(
            role="user",
            parts=[
                types.Part.from_text(
                    text=(
                        "You are a shop operations agent. Use tools to inspect inventory, "
                        "record sales, compute monthly statistics, and plan partner orders. "
                        "Use exact product names from fetched inventory. For stockouts, first "
                        "fetch inventory, then record the available stock quantity as sold. "
                        "After all needed tools are complete, provide a concise Markdown report.\n\n"
                        f"User request: {prompt}"
                    )
                )
            ],
        )
    ]

    with LOG_PATH.open("a", encoding="utf-8") as log_file:
        log_file.write(f"\n\n## Run - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    append_log(LOG_PATH, "User prompt", prompt)
    for step in range(8):
        append_log(LOG_PATH, "Model request", {"model": model, "step": step + 1})
        response = generate_with_retries(
            client,
            model,
            contents,
            types.GenerateContentConfig(
                tools=[tool],
                tool_config=types.ToolConfig(
                    function_calling_config=types.FunctionCallingConfig(mode="AUTO")
                ),
            ),
        )
        function_calls = get_function_calls(response)

        if not function_calls:
            final_text = get_text(response)
            append_log(LOG_PATH, "Model final response", final_text)
            break

        contents.append(response.candidates[0].content)
        for function_call in function_calls:
            tool_result = run_tool(DB_PATH, function_call)
            append_log(LOG_PATH, "Tool call", {"name": tool_result["name"], "args": tool_result["args"]})
            append_log(LOG_PATH, "Tool result", tool_result["result"])
            debug_print("Tool call", {"name": tool_result["name"], "args": tool_result["args"]}, debug)
            debug_print("Tool result", tool_result["result"], debug)
            contents.append(make_function_response(function_call, tool_result["result"]))
    else:
        final_text = "Stopped after reaching the experiment step limit."
        append_log(LOG_PATH, "Step limit reached", final_text)

    REPORT_PATH.write_text(final_text.strip() + "\n", encoding="utf-8")
    append_log(LOG_PATH, "Final report path", str(REPORT_PATH))

    return final_text


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the shop operations tool-calling agent.")
    parser.add_argument("prompt", nargs="*", help="Prompt to send to the model.")
    parser.add_argument("--debug", action="store_true", help="Print tool calls and results.")
    parser.add_argument("--model", default=MODEL, help="Model to use.")
    args = parser.parse_args()

    prompt = " ".join(args.prompt) or (
        "We sold 100 electronics today - 15 TVs, 50 fridges, 35 washing machines - "
        "and we sold-out ALL microwaves. Update stock, give me monthly statistics - "
        "and plan the next bulk-buy electronics from our NovaTech partner."
    )
    final_text = run_agent(prompt, args.model, args.debug)

    print(f"DB: {DB_PATH}")
    print(f"Log: {LOG_PATH}")
    print(f"Final report: {REPORT_PATH}")
    print("\nFinal response:")
    print(final_text)


if __name__ == "__main__":
    main()
