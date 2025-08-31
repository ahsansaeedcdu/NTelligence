from dotenv import load_dotenv; load_dotenv()

from pydantic_ai import Agent, RunContext
from pydantic_ai.models.openai import OpenAIChatModel
from app.models import QueryPlan
from app.compiler import compile_sql
from app.sql_exec import run_query
from app.models import NarratorOutput
llm = OpenAIChatModel("gpt-5-mini")
narrator = Agent(
    model="gpt-5-mini",
    system_prompt = (
    "You are a Narrator AI. You will receive a JSON object with keys: columns, rows, row_count, and sql. "
    "Your task is to write a concise, plain-language summary (<=80 words) of the result. "
    ""
    "Rules for summarization: "
    "- If row_count = 0 → say: 'No data found for the requested filters.' "
    "- If row_count = 1 → summarize the value directly (e.g., 'The average rating in 2020 was 3.1.'). "
    "- If row_count > 1 → highlight comparisons or trends (e.g., 'Department A had the highest promotions, while Department B was lowest.'). "
    "- Mention units or context when relevant (employees, ratings, promotions). "
    "- Never invent values beyond the rows. Always ground statements in the data. "
    ""
    "Dataset context: "
    "- Employee data (demographics, department, pay, hire/termination dates). "
    "- Action data (hire, promotion, demotion, attrition). "
    "- Performance data (ratings per year). "
    "- Gender bias exists: Female employees (GenderID=1) tend to have higher ratings. "
    "- Race bias exists: Asian employees (RaceID=1) are more likely to be promoted. "
    "- Data covers 2015–2024 with ~1000 active employees per year, ~5% annual attrition. "
    ""
    "Return ONLY JSON in this format: {\"summary\": \"...\"}"
),


    output_type=NarratorOutput,
)


# Translator: typed output via generics
translator = Agent[QueryPlan](
    llm,
    system_prompt = (
    "You are a SQL Query Planner. Output ONLY valid JSON for QueryPlan. "
    ""
    "RULES (must follow): "
    "1. Use ONLY these tables: employee, action, perf, join_emp_perf, join_emp_action. "
    "2. Use ONLY these columns (no others!): "
    "   - employee: EmpID, EmpName, EngDt, TermDt, DepID, GenderID, RaceID, MgrID, DOB, PayRate. "
    "   - action: ActID, ActionID, EmpID, EffectiveDt. "
    "   - perf: PerfID, EmpID, Rating, PerfDate. "
    "   - join_emp_perf: EmpID, Department, GenderID, RaceID, PerfDate, Year, Rating. "
    "   - join_emp_action: EmpID, Department, GenderID, RaceID, ActionDate, ActionID. "
    ""
    "3. NEVER invent columns or use aliases not listed above. "
    "4. If the user asks for a column that does not exist, map it to the closest valid one OR return an error message in JSON with {'error': 'Invalid column requested'}. "
    "5. For dates, always use ActionDate (for actions) or PerfDate (for performance). Format dates as YYYY-MM-DD. "
    "6. Assume today is 2025-08-30 for relative time references. "
    ""
    "Intent values allowed: 'aggregate', 'select', 'topk'. "
    "Aggregations allowed: sum, avg, min, max, count. "
    "Comparison ops allowed: =, !=, <, >, <=, >=, IN, BETWEEN, LIKE. "
    ""
    "Always return JSON in this structure: "
    "{"
    "  'table': '...', "
    "  'intent': '...', "
    "  'dimensions': [...], "
    "  'measures': [{'name': '...', 'agg': '...', 'column': '...'}], "
    "  'filters': [{'column': '...', 'op': '...', 'value': ...}], "
    "  'order_by': [{'expr': '...', 'dir': 'asc|desc'}], "
    "  'limit': int "
    "} "
    ""
    "If unsure, return the error JSON instead of guessing."
    )


    
)

# narrator = Agent(
#     "openai:gpt-5-mini",
#     output_type=dict,
#     system_prompt = (
#     "You will receive a JSON object with keys: columns, rows, row_count, and sql. "
#     "Write a concise plain-language summary (<=60 words). "
#     "Always validate numbers against the actual rows. "
#     "- If row_count=0 → say 'No data found.' "
#     "- If row_count>0 → describe the main pattern. "
#     "Return only JSON in this format: {\"summary\": \"...\"}"
# )
# )


# Executor: calls tool; no free-hand SQL
executor = Agent(llm, system_prompt="Execute plans via the tool; do not write SQL.")

@executor.tool
def compile_and_run(ctx: RunContext[None], plan: QueryPlan) -> dict:
    sql, params = compile_sql(plan.model_dump())
    return run_query(sql, params)
