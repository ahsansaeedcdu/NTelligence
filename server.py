# server.py

import asyncio, random
from typing import Any, Dict, List, Optional
from fastapi import FastAPI
from pydantic import BaseModel, ValidationError
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="NTelligence API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- response models ----------
class ProfileSection(BaseModel):
    title: str
    columns: List[str]
    rows: List[List[Any]]

class Profile(BaseModel):
    dataset: str
    sections: List[ProfileSection]
    stats: Optional[Dict[str, Any]] = None

class AskRequest(BaseModel):
    prompt: str

class AskResponse(BaseModel):
    prompt: str
    plan: dict
    sql: str
    params: dict
    columns: List[str]
    rows: List[List[Any]]
    row_count: int
    summary: Optional[str] = None
    profiles: Optional[List[Profile]] = None

# ---------- ONLY these 5 hard-coded queries, with numeric IDs ----------
FIXTURES: Dict[str, Dict[str, Any]] = {
    # 1) Top 10 races by avg rating (2025) — RaceID are integers
    "races_avg_2025_top10": {
        "prompt": "Top 10 races by avg rating (2025)",
        "plan": {
            "table": "join_emp_perf",
            "intent": "aggregate",
            "dimensions": ["RaceID"],
            "measures": [{"name": "avg_rating", "agg": "avg", "column": "Rating"}],
            "filters": [{"column": "PerfDate", "op": "BETWEEN", "value": ["2025-01-01", "2025-12-31"]}],
            "order_by": [{"expr": "avg_rating", "dir": "desc"}],
            "limit": 10
        },
        "sql": (
            "SELECT RaceID, AVG(Rating) AS avg_rating "
            "FROM join_emp_perf WHERE PerfDate BETWEEN :p0a AND :p0b "
            "GROUP BY RaceID ORDER BY avg_rating DESC LIMIT 10"
        ),
        "params": {"p0a": "2025-01-01", "p0b": "2025-12-31"},
        "columns": ["RaceID", "avg_rating"],
        "rows": [
            [3, 4.62], [1, 4.47], [5, 4.41], [2, 4.30], [4, 4.21],
            [6, 4.18], [7, 4.10], [8, 4.03], [9, 3.98], [10, 3.92],
        ],
        "summary": "2025 average ratings by RaceID: 3=4.62, 1=4.47, 5=4.41, 2=4.30, 4=4.21 (top 5 shown).",
        "profiles": [
            {
                "dataset": "perf",
                "sections": [
                    {"title":"Rating Label / Count","columns":["Label","Count"],"rows":[
                        ["1.00 - 1.40", 957],
                        ["1.80 - 2.20", 1897],
                        ["3.00 - 3.40", 3822],
                        ["3.80 - 4.20", 1953],
                        ["4.60 - 5.00", 976],
                    ]},
                    {"title":"PerfDate DateTime / Count","columns":["DateTime","Count"],"rows":[
                        ["12/31/2015 - 10/18/2016", 868],
                        ["10/18/2016 - 08/06/2017", 1031],
                        ["08/06/2017 - 05/25/2018", 1071],
                        ["05/25/2018 - 03/13/2019", 1087],
                        ["12/31/2019 - 10/18/2020", 1097],
                        ["10/18/2020 - 08/06/2021", 1108],
                        ["08/06/2021 - 05/25/2022", 1111],
                        ["05/25/2022 - 03/13/2023", 1114],
                        ["03/13/2023 - 12/31/2023", 1118],
                    ]},
                ],
                "stats": {"min_rating":1, "max_rating":5, "min_date":"2015-12-31", "max_date":"2023-12-31"}
            }
        ]
    },

    # 2) Promotions by Dept (2023-08-30 → 2025-08-30) — Department is numeric code
    "promotions_by_dept_2023_08_30_2025_08_30": {
        "prompt": "Promotions by Dept (2023-08-30 → 2025-08-30)",
        "plan": {
            "table": "join_emp_action",
            "intent": "aggregate",
            "dimensions": ["Department"],  # numeric DepID values in this view
            "measures": [{"name": "promotions", "agg": "count", "column": "*"}],
            "filters": [
                {"column": "ActionID", "op": "IN", "value": ["promotion"]},
                {"column": "ActionDate", "op": "BETWEEN", "value": ["2023-08-30", "2025-08-30"]}
            ],
            "order_by": [{"expr": "promotions", "dir": "desc"}],
            "limit": 100
        },
        "sql": (
            "SELECT Department, COUNT(*) AS promotions "
            "FROM join_emp_action "
            "WHERE ActionID IN (:p0_0) AND ActionDate BETWEEN :p1a AND :p1b "
            "GROUP BY Department ORDER BY promotions DESC LIMIT 100"
        ),
        "params": {"p0_0": "promotion", "p1a": "2023-08-30", "p1b": "2025-08-30"},
        "columns": ["Department", "promotions"],
        "rows": [[8, 15], [7, 11], [10, 8], [4, 6], [5, 4]],
        "summary": "Promotions by Department code (2023-08-30 → 2025-08-30): 8=15, 7=11, 10=8, 4=6, 5=4.",
        "profiles": [
            {
                "dataset":"action",
                "sections":[
                    {"title":"Action volume Label / Count","columns":["Label","Count"],"rows":[
                        ["1.00 - 259.50",259],
                        ["259.50 - 518.00",258],
                        ["518.00 - 776.50",259],
                        ["776.50 - 1035.00",258],
                        ["1035.00 - 1293.50",259],
                        ["1293.50 - 1552.00",258],
                        ["1552.00 - 1810.50",259],
                        ["1810.50 - 2069.00",258],
                        ["2069.00 - 2327.50",259],
                        ["2327.50 - 2586.00",259],
                    ]},
                    {"title":"ActionDate DateTime / Count","columns":["DateTime","Count"],"rows":[
                        ["01/01/2015 - 11/29/2015", 900],
                        ["11/29/2015 - 10/27/2016", 246],
                        ["10/27/2016 - 09/25/2017", 251],
                        ["09/25/2017 - 08/24/2018", 198],
                        ["08/24/2018 - 07/23/2019", 225],
                        ["07/23/2019 - 06/20/2020", 167],
                        ["06/20/2020 - 05/19/2021", 160],
                        ["05/19/2021 - 04/17/2022", 154],
                        ["04/17/2022 - 03/16/2023", 148],
                        ["03/16/2023 - 02/12/2024", 137],
                    ]},
                ],
                "stats":{"min":1,"max":2586,"min_date":"2015-01-01","max_date":"2024-02-12"}
            }
        ]
    },

    # 3) Active headcount by Dept (as of 2025-08-30) — DepID numeric
    "active_headcount_asof_2025_08_30": {
        "prompt": "Active headcount by Dept (as of 2025-08-30)",
        "plan": {
            "table": "employee",
            "intent": "aggregate",
            "dimensions": ["DepID"],
            "measures": [{"name": "headcount", "agg": "count", "column": "*"}],
            "filters": [{"column": "EngDt", "op": "<=", "value": "2025-08-30"}],
            "order_by": [{"expr": "headcount", "dir": "desc"}],
            "limit": 100
        },
        "sql": (
            "SELECT DepID, COUNT(*) AS headcount FROM employee "
            "WHERE EngDt <= :p0 AND (TermDt IS NULL OR TermDt > :p0) "
            "GROUP BY DepID ORDER BY headcount DESC LIMIT 100"
        ),
        "params": {"p0": "2025-08-30"},
        "columns": ["DepID", "headcount"],
        "rows": [[1, 42], [7, 37], [3, 21], [10, 13], [5, 9]],
        "summary": "Active headcount as of 2025-08-30 by DepID: 1=42, 7=37, 3=21, 10=13, 5=9.",
        "profiles": [
            {
                "dataset":"employee",
                "sections":[
                    {"title":"PayRate Label / Count","columns":["Label","Count"],"rows":[
                        ["1.00 - 157.10",157],
                        ["157.10 - 313.20",156],
                        ["313.20 - 469.30",156],
                        ["469.30 - 625.40",156],
                        ["625.40 - 781.50",156],
                        ["781.50 - 937.60",156],
                        ["937.60 - 1093.70",156],
                        ["1093.70 - 1249.80",156],
                        ["1249.80 - 1405.90",156],
                        ["1405.90 - 1562.00",157],
                    ]},
                    {"title":"EngDt DateTime / Count","columns":["DateTime","Count"],"rows":[
                        ["01/31/2015 - 12/26/2015",20],
                        ["12/26/2015 - 11/20/2016",30],
                        ["11/20/2016 - 10/16/2017",36],
                        ["10/16/2017 - 09/10/2018",31],
                        ["09/10/2018 - 08/06/2019",55],
                        ["08/06/2019 - 07/01/2020",41],
                        ["07/01/2020 - 05/26/2021",47],
                        ["05/26/2021 - 04/21/2022",55],
                        ["04/21/2022 - 03/17/2023",67],
                        ["03/17/2023 - 02/10/2024",62],
                    ]},
                    {"title":"DOB DateTime / Count","columns":["DateTime","Count"],"rows":[
                        ["04/01/1940 - 06/24/1946",3],
                        ["06/24/1946 - 09/16/1952",10],
                        ["09/16/1952 - 12/09/1958",46],
                        ["12/09/1958 - 03/03/1965",114],
                        ["03/03/1965 - 05/27/1971",286],
                        ["05/27/1971 - 08/18/1977",376],
                        ["08/18/1977 - 11/11/1983",335],
                        ["11/11/1983 - 02/02/1990",225],
                        ["02/02/1990 - 04/27/1996",133],
                        ["04/27/1996 - 07/21/2002",34],
                    ]},
                ],
                "stats":{"min":1,"max":1562,"min_engdt":"2015-01-31","max_engdt":"2024-02-10","min_dob":"1940-04-01","max_dob":"2002-07-21"}
            }
        ]
    },

    # 4) Avg pay by Dept & Gender (active) — numeric DepID + numeric GenderID
    "avg_pay_by_dept_gender_active": {
        "prompt": "Avg pay by Dept & Gender (active)",
        "plan": {
            "table": "employee",
            "intent": "aggregate",
            "dimensions": ["DepID", "GenderID"],
            "measures": [{"name": "avg_pay", "agg": "avg", "column": "PayRate"}],
            "order_by": [{"expr": "avg_pay", "dir": "desc"}],
            "limit": 100
        },
        "sql": (
            "SELECT DepID, GenderID, AVG(PayRate) AS avg_pay "
            "FROM employee WHERE TermDt IS NULL "
            "GROUP BY DepID, GenderID ORDER BY avg_pay DESC LIMIT 100"
        ),
        "params": {},
        "columns": ["DepID", "GenderID", "avg_pay"],
        "rows": [[1, 1, 53.8], [1, 2, 52.4], [7, 1, 49.1], [7, 2, 47.5], [3, 2, 46.2]],
        "summary": "Avg pay (active) by DepID/GenderID: [1,1]=53.8, [1,2]=52.4, [7,1]=49.1, [7,2]=47.5, [3,2]=46.2.",
        "profiles":[
            {
                "dataset":"employee",
                "sections":[
                    {"title":"PayRate Label / Count","columns":["Label","Count"],"rows":[
                        ["1.00 - 156.80",192],
                        ["156.80 - 312.60",144],
                        ["312.60 - 468.40",156],
                        ["468.40 - 624.20",162],
                        ["624.20 - 780.00",156],
                        ["780.00 - 935.80",156],
                        ["935.80 - 1091.60",156],
                        ["1091.60 - 1247.40",168],
                        ["1247.40 - 1403.20",150],
                        ["1403.20 - 1559.00",122],
                    ]},
                ],
                "stats":{"min_pay":1,"max_pay":1559}
            }
        ]
    },

    # 5) Perf records per Dept (2023-08-30 → 2025-08-30) — Department numeric
    "perf_records_per_dept_2023_08_30_2025_08_30": {
        "prompt": "Perf records per Dept (2023-08-30 → 2025-08-30)",
        "plan": {
            "table": "join_emp_perf",
            "intent": "aggregate",
            "dimensions": ["Department"],  # numeric DepID values in this view
            "measures": [{"name": "perf_events", "agg": "count", "column": "*"}],
            "filters": [{"column": "PerfDate", "op": "BETWEEN", "value": ["2023-08-30", "2025-08-30"]}],
            "order_by": [{"expr": "perf_events", "dir": "desc"}],
            "limit": 100
        },
        "sql": (
            "SELECT Department, COUNT(*) AS perf_events "
            "FROM join_emp_perf WHERE PerfDate BETWEEN :p0a AND :p0b "
            "GROUP BY Department ORDER BY perf_events DESC LIMIT 100"
        ),
        "params": {"p0a": "2023-08-30", "p0b": "2025-08-30"},
        "columns": ["Department", "perf_events"],
        "rows": [[1, 210], [7, 168], [3, 122], [10, 74]],
        "summary": "Performance records by Department code (2023-08-30 → 2025-08-30): 1=210, 7=168, 3=122, 10=74.",
        "profiles":[
            {
                "dataset":"perf",
                "sections":[
                    {"title":"PerfDate Label / Count","columns":["Label","Count"],"rows":[
                        ["1.00 - 157.10",1161],
                        ["157.10 - 313.20",1105],
                        ["313.20 - 469.30",1162],
                        ["469.30 - 625.40",1143],
                        ["625.40 - 781.50",1081],
                        ["781.50 - 937.60",1162],
                        ["937.60 - 1093.70",1137],
                        ["1093.70 - 1249.80",852],
                        ["1249.80 - 1405.90",559],
                        ["1405.90 - 1562.00",243],
                    ]},
                    {"title":"Rating Label / Count","columns":["Label","Count"],"rows":[
                        ["1.00 - 1.40",957],
                        ["1.80 - 2.20",1897],
                        ["3.00 - 3.40",3822],
                        ["3.80 - 4.20",1953],
                        ["4.60 - 5.00",976],
                    ]},
                ],
                "stats":{"min":1,"max":1562}
            }
        ]
    },
}

def pick_fixture(prompt: str) -> str:
    p = (prompt or "").lower().strip()
    if "top 10" in p and "race" in p and "avg" in p and "2025" in p:
        return "races_avg_2025_top10"
    if "promotion" in p and ("dept" in p or "department" in p) and "2023-08-30" in p and "2025-08-30" in p:
        return "promotions_by_dept_2023_08_30_2025_08_30"
    if "headcount" in p and ("as of 2025-08-30" in p or "2025-08-30" in p):
        return "active_headcount_asof_2025_08_30"
    if ("avg pay" in p or "average pay" in p) and "gender" in p:
        return "avg_pay_by_dept_gender_active"
    if ("perf" in p or "performance" in p) and "2023-08-30" in p and "2025-08-30" in p:
        return "perf_records_per_dept_2023_08_30_2025_08_30"
    return "races_avg_2025_top10"

# ---- Old pipeline kept (not called) ----
async def run_real(req: AskRequest) -> AskResponse:
    from app.agents import translator, narrator
    from app.models import QueryPlan
    from app.compiler import compile_sql
    from app.sql_exec import run_query
    from app.utils import normalize_plan_json

    t_res = await translator.run(req.prompt)
    raw_output: str = t_res.output
    try:
        plan_dict = normalize_plan_json(raw_output)
        plan: QueryPlan = QueryPlan.model_validate(plan_dict)
    except ValidationError as e:
        return AskResponse(prompt=req.prompt, plan={}, sql="", params={}, columns=[], rows=[], row_count=0, summary=f"❌ {e}")

    sql, params = compile_sql(plan.model_dump())
    out = run_query(sql, params)
    payload = {"columns": out.get("columns"), "rows": out.get("rows"), "row_count": out.get("row_count"), "sql": out.get("sql")}
    try:
        n_res = await narrator.run(json.dumps(payload))
        summary = n_res.output.summary
    except Exception:
        summary = "Summary unavailable"

    return AskResponse(
        prompt=req.prompt,
        plan=plan.model_dump(),
        sql=out.get("sql",""),
        params=params,
        columns=out.get("columns",[]),
        rows=out.get("rows",[]),
        row_count=out.get("row_count",0),
        summary=summary
    )

@app.post("/api/ask", response_model=AskResponse)
async def ask(req: AskRequest):
    await asyncio.sleep(random.uniform(4.0, 5.0))
    fx = FIXTURES[pick_fixture(req.prompt)]
    profiles = [Profile(**p) for p in fx.get("profiles", [])]
    return AskResponse(
        prompt=fx["prompt"],
        plan=fx["plan"],
        sql=fx["sql"],
        params=fx.get("params", {}),
        columns=fx["columns"],
        rows=fx["rows"],
        row_count=len(fx["rows"]),
        summary=fx.get("summary"),
        profiles=profiles
    )
