import json
import shutil
import sqlite3
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from google import genai
from google.genai import types
from google.genai.errors import APIError
from google.genai.errors import ServerError


MODEL = "gemma-4-31b-it"
EXAMPLE_DB_PATH = Path("examples/shop-example.db")
REPORT_PATH = Path("model-tool-eval-report.md")
RUNS_DIR = Path("runs")
RETRY_STATUS_CODES = {500, 502, 503, 504}
PROMPT = (
    "We sold 100 electronics today - 15 TVs, 50 fridges, 35 washing machines - "
    "and we sold-out ALL microwaves. Update stock, give me monthly statistics - "
    "and plan the next bulk-buy electronics from our NovaTech partner."
)


def rows_to_dicts(rows) -> list:
    return [dict(row) for row in rows]


def get_status_code(error):
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
    conn.close()

    return {
        "month": month,
        "category": category,
        "units_sold": int(summary["units_sold"]),
        "revenue": round(float(summary["revenue"]), 2),
        "sale_lines": int(summary["sale_lines"]),
    }


def plan_partner_order(db_path: Path, partner_name: str, category: str) -> dict:
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
        SELECT name, category, price, stock
        FROM products
        WHERE category = ?
        ORDER BY stock ASC
        """,
        (category,),
    ).fetchall()
    conn.close()

    recommendations = []
    for row in rows:
        if row["stock"] >= 20:
            continue
        order_units = 20 - row["stock"]
        recommendations.append(
            {
                "product_name": row["name"],
                "current_stock": row["stock"],
                "suggested_order_units": order_units,
                "estimated_cost": round(order_units * row["price"], 2),
                "priority": "urgent" if row["stock"] == 0 else "normal",
            }
        )

    return {
        "partner": dict(partner) if partner else {"name": partner_name},
        "category": category,
        "recommendations": recommendations,
    }


def build_tool() -> types.Tool:
    category_schema = types.Schema(
        type=types.Type.STRING,
        enum=["all", "electronics", "home", "clothing", "books", "grocery"],
    )
    return types.Tool(
        function_declarations=[
            types.FunctionDeclaration(
                name="fetch_inventory",
                description="Fetch current product inventory from the local SQLite shop database.",
                parameters=types.Schema(
                    type=types.Type.OBJECT,
                    properties={"category": category_schema},
                    required=["category"],
                ),
            ),
            types.FunctionDeclaration(
                name="record_sales",
                description=(
                    "Record product sales and reduce inventory. Use exact product names "
                    "from fetched inventory. Pass one item for each sold product."
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
                                    "quantity": types.Schema(type=types.Type.INTEGER, minimum=0),
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
                        "month": types.Schema(type=types.Type.STRING),
                        "category": category_schema,
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


def generate_with_retries(client, model: str, contents: list, config, retries: int = 2):
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


def select_models(models: list[dict]) -> list[dict]:
    selected = []
    for model in models:
        name = model["name"]
        actions = model["supported_actions"]
        if "generateContent" not in actions:
            continue
        if "lite" in name.lower() or "gemma-4" in name.lower():
            selected.append(model)

    return selected


def evaluate_model(client, model_name: str, run_dir: Path) -> dict:
    db_path = run_dir / f"{model_name.replace('/', '_')}.db"
    shutil.copy(EXAMPLE_DB_PATH, db_path)
    tool = build_tool()
    contents = [
        types.Content(
            role="user",
            parts=[
                types.Part.from_text(
                    text=(
                        "You are evaluating tool calling. Use tools to inspect inventory, "
                        "record sales, compute monthly statistics, and plan partner orders. "
                        "For stockouts, first fetch inventory, then record the available stock "
                        "quantity as sold. Use exact product names from fetched inventory. "
                        "After all needed tools are complete, provide a concise final response.\n\n"
                        f"User request: {PROMPT}"
                    )
                )
            ],
        )
    ]

    calls = []
    final_text = ""
    started_at = time.time()
    for step in range(8):
        response = generate_with_retries(
            client,
            model_name,
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
            break

        contents.append(response.candidates[0].content)
        for function_call in function_calls:
            tool_result = run_tool(db_path, function_call)
            calls.append(tool_result)
            contents.append(make_function_response(function_call, tool_result["result"]))

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    sales = rows_to_dicts(
        conn.execute(
            """
            SELECT product_name, quantity
            FROM sales
            ORDER BY id
            """
        ).fetchall()
    )
    conn.close()

    expected_sales = {
        "NovaTech TV": 15,
        "NovaTech Fridge": 50,
        "NovaTech Washing Machine": 35,
        "NovaTech Microwave": 12,
    }
    actual_sales = {row["product_name"]: row["quantity"] for row in sales}
    call_names = [call["name"] for call in calls]
    fetched_before_sales = (
        "fetch_inventory" in call_names
        and "record_sales" in call_names
        and call_names.index("fetch_inventory") < call_names.index("record_sales")
    )
    sales_correct = actual_sales == expected_sales
    stats_called = "get_sales_stats" in call_names
    plan_called = "plan_partner_order" in call_names
    final_response = bool(final_text)
    leaked_narration = any(
        phrase in final_text.lower()
        for phrase in ["i will now", "i have all", "the user wants", "i should"]
    )

    score = 0
    score += 2 if fetched_before_sales else 0
    score += 3 if sales_correct else 0
    score += 1 if stats_called else 0
    score += 1 if plan_called else 0
    score += 2 if final_response else 0
    score += 1 if final_response and not leaked_narration else 0

    comments = []
    if fetched_before_sales:
        comments.append("fetched inventory before recording sales")
    else:
        comments.append("did not fetch inventory before sales")
    if sales_correct:
        comments.append("sales quantities correct")
    else:
        comments.append(f"sales mismatch: {actual_sales}")
    if not final_response:
        comments.append("no final text")
    elif leaked_narration:
        comments.append("final text leaked planning narration")
    else:
        comments.append("clean final response")

    return {
        "model": model_name,
        "score": score,
        "duration_seconds": round(time.time() - started_at, 1),
        "calls": call_names,
        "sales": actual_sales,
        "final_preview": final_text[:240].replace("\n", " "),
        "comments": "; ".join(comments),
    }


def write_report(models: list[dict], selected: list[dict], results: list[dict]) -> None:
    lines = [
        "# Model Tool-Calling Evaluation",
        "",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "## Scenario",
        "",
        "Copied `examples/shop-example.db`, asked the model to record electronics sales, compute monthly stats, and plan a NovaTech bulk buy.",
        "",
        "Score is out of 10: inventory-first flow 2, correct sales 3, stats 1, plan 1, final response 2, no leaked narration 1.",
        "",
        "## Tested Models",
        "",
        "| Model | Score | Calls | Comments |",
        "|---|---:|---|---|",
    ]
    for result in results:
        lines.append(
            f"| `{result['model']}` | {result['score']}/10 | "
            f"{', '.join(result['calls']) or 'none'} | {result['comments']} |"
        )

    lines.extend(
        [
            "",
            "## Selected Models",
            "",
        ]
    )
    for model in selected:
        lines.append(f"- `{model['name']}` - {model['display_name']}")

    lines.extend(
        [
            "",
            "## All Available Models",
            "",
        ]
    )
    for model in models:
        actions = ", ".join(model["supported_actions"])
        lines.append(f"- `{model['name']}` - {model['display_name']} - {actions}")

    REPORT_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    load_dotenv()
    client = genai.Client()
    models = [
        {
            "name": model.name,
            "display_name": getattr(model, "display_name", "") or "",
            "supported_actions": getattr(model, "supported_actions", []) or [],
        }
        for model in client.models.list()
    ]
    selected = select_models(models)
    run_dir = RUNS_DIR / datetime.now().strftime("model-eval-%Y%m%d-%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for model in selected:
        print(f"Evaluating {model['name']}...")
        try:
            result = evaluate_model(client, model["name"], run_dir)
        except APIError as error:
            result = {
                "model": model["name"],
                "score": 0,
                "duration_seconds": 0,
                "calls": [],
                "sales": {},
                "final_preview": "",
                "comments": f"API error: {str(error)[:180]}",
            }
        except Exception as error:
            result = {
                "model": model["name"],
                "score": 0,
                "duration_seconds": 0,
                "calls": [],
                "sales": {},
                "final_preview": "",
                "comments": f"error: {type(error).__name__}: {str(error)[:180]}",
            }
        results.append(result)

    write_report(models, selected, results)
    print(f"Wrote {REPORT_PATH}")


if __name__ == "__main__":
    main()
