import json
import re
import sqlite3
import sys
import time
from datetime import datetime

from dotenv import load_dotenv
from google import genai
from google.genai.errors import ClientError
from google.genai.errors import ServerError
from google.genai import types


DB_PATH = "shop.db"
LOG_PATH = "db-fetch-log.md"
REPORT_PATH = "final-report.md"
MODEL = "gemma-4-26b-a4b-it"
RETRY_STATUS_CODES = {500, 502, 503, 504}
PRODUCT_CATEGORIES = ["all", "electronics", "home", "clothing", "books", "grocery"]
TOOL_NAMES = [
    "fetch_products",
    "update_inventory_from_sales_report",
    "get_monthly_sales_stats",
    "plan_bulk_buy",
]
SAMSUNG_PRODUCTS = [
    ("Samsung TV", "electronics", 499.99, 25),
    ("Samsung Fridge", "electronics", 899.99, 80),
    ("Samsung Washing Machine", "electronics", 699.99, 45),
    ("Samsung Microwave", "electronics", 149.99, 12),
]


def create_shop_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            price REAL NOT NULL,
            stock INTEGER NOT NULL
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS sales (
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
        CREATE TABLE IF NOT EXISTS partners (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            category TEXT NOT NULL,
            lead_time_days INTEGER NOT NULL,
            min_order_units INTEGER NOT NULL,
            discount_percent REAL NOT NULL
        )
        """
    )
    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_products_name ON products(name)")
    cursor.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_partners_name_category "
        "ON partners(name, category)"
    )

    cursor.execute("SELECT COUNT(*) FROM products")
    if cursor.fetchone()[0] == 0:
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
            ],
        )

    cursor.executemany(
        """
        INSERT OR IGNORE INTO products (name, category, price, stock)
        VALUES (?, ?, ?, ?)
        """,
        SAMSUNG_PRODUCTS,
    )
    cursor.execute(
        """
        INSERT OR IGNORE INTO partners
            (name, category, lead_time_days, min_order_units, discount_percent)
        VALUES (?, ?, ?, ?, ?)
        """,
        ("Samsung", "electronics", 14, 50, 8.0),
    )

    conn.commit()
    conn.close()


def fetch_products(category: str, max_price: float) -> list:
    """Fetch products from the local shop database.

    Args:
        category: Product category, such as electronics, home, clothing,
            books, grocery, or all.
        max_price: Maximum price in dollars. Use 0 for no price limit.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    sql = "SELECT name, category, price, stock FROM products WHERE 1 = 1"
    params = []

    if category.lower() != "all":
        sql += " AND category = ?"
        params.append(category.lower())

    if max_price > 0:
        sql += " AND price <= ?"
        params.append(max_price)

    sql += " ORDER BY price"

    rows = cursor.execute(sql, params).fetchall()
    conn.close()

    return [dict(row) for row in rows]


def find_product(cursor, name_pattern: str):
    return cursor.execute(
        """
        SELECT id, name, category, price, stock
        FROM products
        WHERE lower(name) LIKE ?
        ORDER BY id
        LIMIT 1
        """,
        (f"%{name_pattern.lower()}%",),
    ).fetchone()


def record_sale(cursor, product, quantity: int, note: str) -> dict:
    quantity = max(0, min(quantity, product["stock"]))
    total = round(quantity * product["price"], 2)
    new_stock = product["stock"] - quantity
    sold_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cursor.execute(
        "UPDATE products SET stock = ? WHERE id = ?",
        (new_stock, product["id"]),
    )
    cursor.execute(
        """
        INSERT INTO sales
            (product_id, product_name, category, quantity, unit_price, total, sold_at, note)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            product["id"],
            product["name"],
            product["category"],
            quantity,
            product["price"],
            total,
            sold_at,
            note,
        ),
    )

    return {
        "product": product["name"],
        "quantity_sold": quantity,
        "unit_price": product["price"],
        "revenue": total,
        "stock_before": product["stock"],
        "stock_after": new_stock,
    }


def parse_sales_report(report: str) -> list:
    text = report.lower()
    parsed = []
    product_patterns = [
        ("Samsung TV", r"(\d+)\s*(?:samsung\s+)?tvs?\b"),
        ("Samsung Fridge", r"(\d+)\s*(?:samsung\s+)?fridges?\b"),
        ("Samsung Washing Machine", r"(\d+)\s*(?:samsung\s+)?washing machines?\b"),
        ("Samsung Microwave", r"(\d+)\s*(?:samsung\s+)?microwaves?\b"),
    ]

    for product_name, pattern in product_patterns:
        match = re.search(pattern, text)
        if match:
            parsed.append({"product_name": product_name, "quantity": int(match.group(1))})

    if re.search(r"sold[- ]?out\s+(?:all\s+)?(?:samsung\s+)?microwaves?", text):
        parsed.append({"product_name": "Samsung Microwave", "quantity": "all"})

    return parsed


def update_inventory_from_sales_report(
    tv_sold: int,
    fridge_sold: int,
    washing_machine_sold: int,
    microwave_sold: int,
    microwave_sold_out: bool,
) -> dict:
    """Update inventory and sales records from structured electronics sales data."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    parsed_sales = [
        {"product_name": "Samsung TV", "quantity": tv_sold},
        {"product_name": "Samsung Fridge", "quantity": fridge_sold},
        {"product_name": "Samsung Washing Machine", "quantity": washing_machine_sold},
    ]
    if microwave_sold_out:
        parsed_sales.append({"product_name": "Samsung Microwave", "quantity": "all"})
    elif microwave_sold > 0:
        parsed_sales.append({"product_name": "Samsung Microwave", "quantity": microwave_sold})

    updates = []
    note = (
        "Structured sales update: "
        f"tv_sold={tv_sold}, fridge_sold={fridge_sold}, "
        f"washing_machine_sold={washing_machine_sold}, microwave_sold={microwave_sold}, "
        f"microwave_sold_out={microwave_sold_out}"
    )

    for sale in parsed_sales:
        product = find_product(cursor, sale["product_name"])
        if not product:
            updates.append(
                {
                    "product": sale["product_name"],
                    "error": "Product not found in database.",
                }
            )
            continue

        quantity = product["stock"] if sale["quantity"] == "all" else sale["quantity"]
        updates.append(record_sale(cursor, product, quantity, note))

    conn.commit()
    conn.close()

    return {
        "parsed_sales": parsed_sales,
        "updates": updates,
        "message": "Inventory and sales records updated.",
    }


def get_monthly_sales_stats(month: str, category: str) -> dict:
    """Get monthly sales statistics from the local shop database.

    Args:
        month: Month in YYYY-MM format, or current for the current month.
        category: Product category, such as electronics, home, clothing,
            books, grocery, or all.
    """
    if month.lower() == "current":
        month = datetime.now().strftime("%Y-%m")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    params = [f"{month}%"]
    category_filter = ""

    if category.lower() != "all":
        category_filter = "AND category = ?"
        params.append(category.lower())

    summary = cursor.execute(
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
    by_product = cursor.execute(
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
        "category": category.lower(),
        "units_sold": int(summary["units_sold"]),
        "revenue": round(float(summary["revenue"]), 2),
        "sale_lines": int(summary["sale_lines"]),
        "by_product": [dict(row) for row in by_product],
    }


def plan_bulk_buy(category: str, partner: str) -> dict:
    """Plan the next bulk buy using stock and sales data."""
    month = datetime.now().strftime("%Y-%m")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    products = cursor.execute(
        """
        SELECT p.id, p.name, p.category, p.price, p.stock,
            COALESCE(SUM(s.quantity), 0) AS monthly_units_sold
        FROM products p
        LEFT JOIN sales s
            ON s.product_id = p.id
            AND s.sold_at LIKE ?
        WHERE p.category = ?
        GROUP BY p.id
        ORDER BY p.stock ASC, monthly_units_sold DESC
        """,
        (f"{month}%", category.lower()),
    ).fetchall()
    partner_row = cursor.execute(
        """
        SELECT name, category, lead_time_days, min_order_units, discount_percent
        FROM partners
        WHERE lower(name) = ? AND category = ?
        LIMIT 1
        """,
        (partner.lower(), category.lower()),
    ).fetchone()
    conn.close()

    recommendations = []
    for product in products:
        monthly_units_sold = int(product["monthly_units_sold"])
        target_stock = max(20, monthly_units_sold * 2)
        suggested_order = max(0, target_stock - product["stock"])
        if product["stock"] == 0:
            priority = "urgent"
        elif suggested_order > 0:
            priority = "normal"
        else:
            priority = "hold"

        if suggested_order > 0:
            recommendations.append(
                {
                    "product": product["name"],
                    "current_stock": product["stock"],
                    "monthly_units_sold": monthly_units_sold,
                    "suggested_order_units": suggested_order,
                    "estimated_cost": round(suggested_order * product["price"], 2),
                    "priority": priority,
                }
            )

    total_units = sum(item["suggested_order_units"] for item in recommendations)
    total_cost = round(sum(item["estimated_cost"] for item in recommendations), 2)

    if partner_row and 0 < total_units < partner_row["min_order_units"]:
        recommendations.append(
            {
                "product": "Assorted Samsung electronics",
                "current_stock": None,
                "monthly_units_sold": None,
                "suggested_order_units": partner_row["min_order_units"] - total_units,
                "estimated_cost": None,
                "priority": "fill minimum order",
            }
        )
        total_units = partner_row["min_order_units"]

    return {
        "partner": dict(partner_row) if partner_row else {"name": partner},
        "category": category.lower(),
        "month": month,
        "recommendations": recommendations,
        "total_suggested_units": total_units,
        "estimated_cost_before_partner_discount": total_cost,
        "notes": [
            "Priority is highest for sold-out products.",
            "Suggested order targets about two months of current-month sales or at least 20 units.",
        ],
    }


def append_log(title: str, value=None) -> None:
    with open(LOG_PATH, "a", encoding="utf-8") as log_file:
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


def start_run_log(prompt: str) -> None:
    with open(LOG_PATH, "a", encoding="utf-8") as log_file:
        log_file.write(f"\n\n## Run - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    append_log("User prompt", prompt)


def save_final_report(prompt: str, answer: str) -> None:
    with open(REPORT_PATH, "w", encoding="utf-8") as report_file:
        report_file.write("# Final Report\n\n")
        report_file.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        report_file.write("## Prompt\n\n")
        report_file.write("```text\n")
        report_file.write(prompt)
        report_file.write("\n```\n\n")
        report_file.write("## Response\n\n")
        report_file.write(answer.strip())
        report_file.write("\n")


def debug_print(title: str, value, enabled: bool) -> None:
    if not enabled:
        return

    print(f"\n[debug] {title}", file=sys.stderr)
    if isinstance(value, (dict, list)):
        print(json.dumps(value, indent=2), file=sys.stderr)
    else:
        print(value, file=sys.stderr)


def get_status_code(error: ServerError):
    status_code = getattr(error, "status_code", None) or getattr(error, "code", None)
    if status_code is not None:
        return int(status_code)

    error_text = str(error)
    for code in RETRY_STATUS_CODES:
        if str(code) in error_text:
            return code

    return None


def build_prompt(prompt: str) -> str:
    return (
        "You are a shop operations assistant. Use tools for product lookup, "
        "inventory updates, sales statistics, and bulk-buy planning. "
        "For a request that asks to update sales, get statistics, and plan a "
        "bulk buy, call tools in this order: update_inventory_from_sales_report, "
        "get_monthly_sales_stats, plan_bulk_buy. Do not invent DB values.\n\n"
        f"User request: {prompt}"
    )


def build_tool() -> types.Tool:
    return types.Tool(
        function_declarations=[
            types.FunctionDeclaration(
                name="fetch_products",
                description=(
                    "Fetch products from the local SQLite shop database. "
                    "Use category='all' for all product categories. "
                    "Use max_price=0 only when there is no price limit."
                ),
                parameters=types.Schema(
                    type=types.Type.OBJECT,
                    properties={
                        "category": types.Schema(
                            type=types.Type.STRING,
                            enum=PRODUCT_CATEGORIES,
                            description="Product category to search.",
                        ),
                        "max_price": types.Schema(
                            type=types.Type.NUMBER,
                            minimum=0,
                            description=(
                                "Maximum product price in dollars. "
                                "Use 0 only for no price limit."
                            ),
                        ),
                    },
                    required=["category", "max_price"],
                ),
            ),
            types.FunctionDeclaration(
                name="update_inventory_from_sales_report",
                description=(
                    "Record structured electronics sales and decrease product stock "
                    "in the local SQLite shop database."
                ),
                parameters=types.Schema(
                    type=types.Type.OBJECT,
                    properties={
                        "tv_sold": types.Schema(
                            type=types.Type.INTEGER,
                            minimum=0,
                            description="Number of Samsung TVs sold.",
                        ),
                        "fridge_sold": types.Schema(
                            type=types.Type.INTEGER,
                            minimum=0,
                            description="Number of Samsung fridges sold.",
                        ),
                        "washing_machine_sold": types.Schema(
                            type=types.Type.INTEGER,
                            minimum=0,
                            description="Number of Samsung washing machines sold.",
                        ),
                        "microwave_sold": types.Schema(
                            type=types.Type.INTEGER,
                            minimum=0,
                            description=(
                                "Number of Samsung microwaves sold. Use 0 when "
                                "microwave_sold_out is true and no exact count is given."
                            ),
                        ),
                        "microwave_sold_out": types.Schema(
                            type=types.Type.BOOLEAN,
                            description=(
                                "True when the user says all microwaves were sold out."
                            ),
                        ),
                    },
                    required=[
                        "tv_sold",
                        "fridge_sold",
                        "washing_machine_sold",
                        "microwave_sold",
                        "microwave_sold_out",
                    ],
                ),
            ),
            types.FunctionDeclaration(
                name="get_monthly_sales_stats",
                description=(
                    "Get monthly sales totals, revenue, and per-product breakdowns "
                    "from the local SQLite database."
                ),
                parameters=types.Schema(
                    type=types.Type.OBJECT,
                    properties={
                        "month": types.Schema(
                            type=types.Type.STRING,
                            description="Month in YYYY-MM format, or current.",
                        ),
                        "category": types.Schema(
                            type=types.Type.STRING,
                            enum=PRODUCT_CATEGORIES,
                            description="Product category for the stats.",
                        ),
                    },
                    required=["month", "category"],
                ),
            ),
            types.FunctionDeclaration(
                name="plan_bulk_buy",
                description=(
                    "Plan the next bulk-buy order from a partner using current stock "
                    "and monthly sales data."
                ),
                parameters=types.Schema(
                    type=types.Type.OBJECT,
                    properties={
                        "category": types.Schema(
                            type=types.Type.STRING,
                            enum=PRODUCT_CATEGORIES,
                            description="Product category to restock.",
                        ),
                        "partner": types.Schema(
                            type=types.Type.STRING,
                            description="Partner or vendor name, such as Samsung.",
                        ),
                    },
                    required=["category", "partner"],
                ),
            ),
        ]
    )


def build_tool_config(mode: str = "ANY", allowed_names=None) -> types.ToolConfig:
    allowed_function_names = allowed_names if mode == "ANY" else None

    return types.ToolConfig(
        function_calling_config=types.FunctionCallingConfig(
            mode=mode,
            allowed_function_names=allowed_function_names,
        )
    )


def get_function_call(response):
    for candidate in response.candidates or []:
        if not candidate.content:
            continue

        for part in candidate.content.parts or []:
            if part.function_call:
                return part.function_call

    return None


def get_function_calls(response) -> list:
    calls = []

    for candidate in response.candidates or []:
        if not candidate.content:
            continue

        for part in candidate.content.parts or []:
            if part.function_call:
                calls.append(part.function_call)

    return calls


def get_text(response) -> str:
    text_parts = []

    for candidate in response.candidates or []:
        if not candidate.content:
            continue

        for part in candidate.content.parts or []:
            if part.text:
                text_parts.append(part.text)

    return "\n".join(text_parts)


def clean_tool_args(function_call, prompt: str) -> dict:
    args = dict(function_call.args)

    if function_call.name == "fetch_products":
        args["category"] = str(args.get("category", "all")).lower()
        if args["category"] not in PRODUCT_CATEGORIES:
            args["category"] = "all"

        args["max_price"] = float(args.get("max_price", 0) or 0)
        price_match = re.search(r"\$(\d+(?:\.\d+)?)", prompt)
        if not price_match:
            price_match = re.search(
                r"(?:under|below|less than)\s+(\d+(?:\.\d+)?)",
                prompt.lower(),
            )

        if price_match:
            args["max_price"] = float(price_match.group(1))

    elif function_call.name == "update_inventory_from_sales_report":
        prompt_sales = extract_structured_sales(prompt)
        args["tv_sold"] = prompt_sales["tv_sold"] or int(args.get("tv_sold") or 0)
        args["fridge_sold"] = prompt_sales["fridge_sold"] or int(
            args.get("fridge_sold") or 0
        )
        args["washing_machine_sold"] = prompt_sales["washing_machine_sold"] or int(
            args.get("washing_machine_sold") or 0
        )
        args["microwave_sold_out"] = prompt_sales["microwave_sold_out"] or bool(
            args.get("microwave_sold_out", False)
        )
        args["microwave_sold"] = (
            0
            if args["microwave_sold_out"]
            else prompt_sales["microwave_sold"] or int(args.get("microwave_sold") or 0)
        )

    elif function_call.name == "get_monthly_sales_stats":
        args["month"] = str(args.get("month") or "current")
        args["category"] = str(args.get("category") or infer_category(prompt)).lower()
        if args["category"] not in PRODUCT_CATEGORIES:
            args["category"] = "all"

    elif function_call.name == "plan_bulk_buy":
        args["category"] = str(args.get("category") or infer_category(prompt)).lower()
        if args["category"] not in PRODUCT_CATEGORIES or args["category"] == "all":
            args["category"] = "electronics"
        args["partner"] = str(args.get("partner") or infer_partner(prompt))

    return args


def extract_structured_sales(prompt: str) -> dict:
    text = prompt.lower()

    def find_count(pattern: str) -> int:
        match = re.search(pattern, text)
        return int(match.group(1)) if match else 0

    return {
        "tv_sold": find_count(r"(\d+)\s*(?:samsung\s+)?tvs?\b"),
        "fridge_sold": find_count(r"(\d+)\s*(?:samsung\s+)?fridges?\b"),
        "washing_machine_sold": find_count(
            r"(\d+)\s*(?:samsung\s+)?washing machines?\b"
        ),
        "microwave_sold": find_count(r"(\d+)\s*(?:samsung\s+)?microwaves?\b"),
        "microwave_sold_out": bool(
            re.search(r"sold[- ]?out\s+(?:all\s+)?(?:samsung\s+)?microwaves?", text)
        ),
    }


def infer_category(prompt: str) -> str:
    text = prompt.lower()
    for category in PRODUCT_CATEGORIES:
        if category != "all" and category in text:
            return category

    return "all"


def infer_partner(prompt: str) -> str:
    if "samsung" in prompt.lower():
        return "Samsung"

    return "Samsung"


def run_function_call(function_call, prompt: str, debug: bool):
    if function_call.name not in TOOL_NAMES:
        raise ValueError(f"Unknown function call: {function_call.name}")

    raw_args = dict(function_call.args)
    clean_args = clean_tool_args(function_call, prompt)
    append_log(
        "Tool call",
        {
            "name": function_call.name,
            "raw_args_from_model": raw_args,
            "args_used_by_python": clean_args,
        },
    )
    debug_print(
        "Tool call",
        {
            "name": function_call.name,
            "raw_args_from_model": raw_args,
            "args_used_by_python": clean_args,
        },
        debug,
    )

    if function_call.name == "fetch_products":
        result = fetch_products(**clean_args)
    elif function_call.name == "update_inventory_from_sales_report":
        result = update_inventory_from_sales_report(**clean_args)
    elif function_call.name == "get_monthly_sales_stats":
        result = get_monthly_sales_stats(**clean_args)
    elif function_call.name == "plan_bulk_buy":
        result = plan_bulk_buy(**clean_args)

    append_log("Tool result", result)
    debug_print("Tool result", result, debug)

    return result


def required_tools_for_prompt(prompt: str) -> list:
    text = prompt.lower()
    required = []

    if any(word in text for word in ["sold", "sales", "update stock", "sold-out"]):
        required.append("update_inventory_from_sales_report")

    if any(word in text for word in ["statistics", "stats", "monthly", "revenue"]):
        required.append("get_monthly_sales_stats")

    if any(word in text for word in ["plan", "bulk-buy", "bulk buy", "partner"]):
        required.append("plan_bulk_buy")

    if not required:
        required.append("fetch_products")

    return required


def format_products(products: list) -> str:
    if not products:
        return "No matching products found."

    lines = ["Matching products:"]
    for product in products:
        lines.append(
            f"- {product['name']}: ${product['price']:.2f} "
            f"({product['category']}, stock: {product['stock']})"
        )

    return "\n".join(lines)


def format_operations_summary(tool_results: list) -> str:
    lines = ["Operation summary:"]

    for item in tool_results:
        name = item["name"]
        result = item["result"]
        if name == "update_inventory_from_sales_report":
            lines.append("\nInventory updates:")
            for update in result.get("updates", []):
                if "error" in update:
                    lines.append(f"- {update['product']}: {update['error']}")
                else:
                    lines.append(
                        f"- {update['product']}: sold {update['quantity_sold']}, "
                        f"stock {update['stock_before']} -> {update['stock_after']}, "
                        f"revenue ${update['revenue']:.2f}"
                    )

        elif name == "get_monthly_sales_stats":
            lines.append("\nMonthly statistics:")
            lines.append(
                f"- {result['month']} {result['category']}: "
                f"{result['units_sold']} units, ${result['revenue']:.2f} revenue"
            )
            for row in result.get("by_product", []):
                lines.append(
                    f"- {row['product_name']}: {row['units_sold']} units, "
                    f"${row['revenue']:.2f}"
                )

        elif name == "plan_bulk_buy":
            lines.append("\nBulk-buy plan:")
            for recommendation in result.get("recommendations", []):
                units = recommendation["suggested_order_units"]
                lines.append(
                    f"- {recommendation['product']}: order {units} units "
                    f"({recommendation['priority']})"
                )

    return "\n".join(lines)


def make_function_response(function_call, result) -> types.Content:
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


def generate_once(client, prompt: str, debug: bool) -> str:
    model_prompt = build_prompt(prompt)
    tool = build_tool()
    contents = [
        types.Content(
            role="user",
            parts=[types.Part.from_text(text=model_prompt)],
        )
    ]
    tool_results = []
    required_tools = required_tools_for_prompt(prompt)

    for tool_name in required_tools:
        model_request = {
            "model": MODEL,
            "allowed_tool": tool_name,
            "prompt": model_prompt,
        }
        append_log("Model request", model_request)
        debug_print("Model request", model_request, debug)

        response = client.models.generate_content(
            model=MODEL,
            contents=contents,
            config=types.GenerateContentConfig(
                tools=[tool],
                tool_config=build_tool_config(mode="ANY", allowed_names=[tool_name]),
            ),
        )
        function_calls = get_function_calls(response)

        if not function_calls:
            text = get_text(response)
            append_log("Model text response", text)
            debug_print("Model text response", text, debug)
            continue

        function_call = function_calls[0]
        result = run_function_call(function_call, prompt, debug)
        tool_results.append({"name": function_call.name, "result": result})
        function_response = make_function_response(function_call, result)
        append_log(
            "Function response sent",
            {
                "id": getattr(function_call, "id", None),
                "name": function_call.name,
                "response": {"result": result},
            },
        )
        debug_print(
            "Function response sent",
            {
                "id": getattr(function_call, "id", None),
                "name": function_call.name,
                "response": {"result": result},
            },
            debug,
        )

        contents.append(response.candidates[0].content)
        contents.append(function_response)

    if required_tools == ["fetch_products"] and tool_results:
        return format_products(tool_results[0]["result"])

    try:
        final_response = client.models.generate_content(
            model=MODEL,
            contents=contents,
            config=types.GenerateContentConfig(),
        )
        final_text = get_text(final_response).strip()
    except ClientError as error:
        append_log("Model final response failed", str(error))
        debug_print("Model final response failed", str(error), debug)
        final_text = ""

    if final_text:
        append_log("Model final response", final_text)
        debug_print("Model final response", final_text, debug)
        return final_text

    return format_operations_summary(tool_results)


def generate_with_retries(client, prompt: str, debug: bool, retries: int = 3) -> str:
    for attempt in range(retries + 1):
        try:
            return generate_once(client, prompt, debug)
        except ServerError as error:
            status_code = get_status_code(error)
            if status_code not in RETRY_STATUS_CODES or attempt == retries:
                append_log("Error", str(error))
                raise

            wait_seconds = 2**attempt
            append_log(
                "Retry",
                {
                    "status_code": status_code,
                    "attempt": attempt + 1,
                    "wait_seconds": wait_seconds,
                },
            )
            debug_print(
                "Retry",
                {
                    "status_code": status_code,
                    "attempt": attempt + 1,
                    "wait_seconds": wait_seconds,
                },
                debug,
            )
            print(
                f"Gemini returned {status_code}; retrying in {wait_seconds}s...",
                file=sys.stderr,
            )
            time.sleep(wait_seconds)


def main() -> None:
    load_dotenv()
    create_shop_db()

    args = sys.argv[1:]
    debug = "--debug" in args
    args = [arg for arg in args if arg != "--debug"]
    prompt = " ".join(args) or "Find 2 electronics products under $100."
    start_run_log(prompt)
    if "under 0" in prompt.lower():
        print(
            "Tip: your shell may have expanded $40 to 0. "
            "Use single quotes, like: python3 shop-ops-agent.py 'Find home products under $40'",
            file=sys.stderr,
        )

    client = genai.Client()
    answer = generate_with_retries(client, prompt, debug)
    append_log("Final answer", answer)
    save_final_report(prompt, answer)
    debug_print("Final answer", answer, debug)

    print(f"Saved final report to {REPORT_PATH}", file=sys.stderr)
    print(answer)


if __name__ == "__main__":
    main()
