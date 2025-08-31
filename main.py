import asyncio
from pydantic import ValidationError
from app.agents import translator
from app.models import QueryPlan
from app.compiler import compile_sql
from app.sql_exec import run_query
from app.utils import normalize_plan_json
from app.agents import narrator
import json

async def run_prompt(q: str):
    print(f"\n============ PROMPT ============\n{q}\n")
    t_res = await translator.run(q)
    raw_output: str = t_res.output

    try:
        plan_dict = normalize_plan_json(raw_output)
        plan: QueryPlan = QueryPlan.model_validate(plan_dict)
    except ValidationError as e:
        print("❌ Error: malformed JSON after normalization")
        print("Raw LLM output:", raw_output)
        print("Validation Error:", e)
        return

    try:
        sql, params = compile_sql(plan.model_dump())
        out = run_query(sql, params)
        print("✅ SQL:", out.get("sql"))
        print("Rows:", out.get("row_count"))
        print("Columns:", out.get("columns"))
        print("Sample rows:", out.get("rows")[:3])

        # 🔥 Pass results to Narrator Agent (Agent D)
        payload = {
            "columns": out.get("columns"),
            "rows": out.get("rows"),
            "row_count": out.get("row_count"),
            "sql": out.get("sql")
        }
        n_res = await narrator.run(json.dumps(payload))
        print("📝 Summary:", n_res.output.get("summary"))

    except Exception as e:
        print("❌ SQL compile/exec error:", e)
        print("Plan was:", plan.model_dump())
        return


async def main():
    prompts = [
        # HR-focused
        "How many total employees are in the join_emp_action table?",

        # Operations-style
        "Which departments had the most actions in the last year?",

        # Bias / Ethics check
        "What is the average performance rating by GenderID in 2020?",

        # Multiple measures (rate-style query)
        "What are the promotions and total employees for each department in August 2022?"
    ]

    for q in prompts:
        await run_prompt(q)


if __name__ == "__main__":
    asyncio.run(main())
