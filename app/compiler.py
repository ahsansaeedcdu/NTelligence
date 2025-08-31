from typing import List, Tuple, Dict, Any
import re

ALLOWED: Dict[str, List[str]] = {
    "employee": ["EmpID","EmpName","EngDt","TermDt","DepID","GenderID","RaceID","MgrID","DOB","PayRate"],
    "action": ["ActID","ActionID","EmpID","EffectiveDt"],
    "perf": ["PerfID","EmpID","Rating","PerfDate"],
    "join_emp_perf": ["EmpID","Department","GenderID","RaceID","PerfDate","Year","Rating"],
    "join_emp_action": ["EmpID","Department","GenderID","RaceID","ActionDate","ActionID"],
}

class CompileSQLError(Exception):
    pass

_VALID_AGGS = {"sum","avg","min","max","count"}
_VALID_OPS = {"=","!=","<",">","<=",">=","LIKE","IN","BETWEEN","IS NULL","IS NOT NULL"}

def compile_sql(plan: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    table = plan.get("table")
    if not table or table not in ALLOWED:
        raise CompileSQLError(f"Invalid table: {table!r}")
    cols = set(ALLOWED[table])

    # dimensions
    dims_in = plan.get("dimensions", []) or []
    if not isinstance(dims_in, list):
        raise CompileSQLError("dimensions must be a list")
    for d in dims_in:
        if d not in cols:
            raise CompileSQLError(f"Invalid dimension: {d}")
    seen = set()
    dims: List[str] = []
    for d in dims_in:
        if d not in seen:
            seen.add(d); dims.append(d)

    # measures
    measures = plan.get("measures") or []
    if not measures:
        raise CompileSQLError("At least one measure is required")

    sel_parts: List[str] = dims.copy()
    measure_aliases: List[str] = []
    measure_specs: List[Tuple[str,str,str]] = []  # (agg, col, alias)

    for m in measures:
        col = m.get("column")
        agg = (m.get("agg") or "").lower()
        if agg not in _VALID_AGGS:
            raise CompileSQLError(f"Invalid aggregation: {agg}")
        if col != "*" and col not in cols:
            raise CompileSQLError(f"Invalid column in measure: {col}")
        alias = m.get("name") or f"{agg}_{col if col != '*' else 'all'}"
        expr = f"{agg.upper()}(*)" if col == "*" else f"{agg.upper()}({col})"
        sel_parts.append(f"{expr} AS {alias}")
        measure_aliases.append(alias)
        measure_specs.append((agg, col, alias))

    # filters
    where_clauses: List[str] = []
    params: Dict[str, Any] = {}

    for i, f in enumerate(plan.get("filters", []) or []):
        col = f.get("column")
        op = (f.get("op") or "").upper()
        val = f.get("value")

        if col not in cols:
            raise CompileSQLError(f"Invalid column in filter: {col}")
        if op not in _VALID_OPS:
            raise CompileSQLError(f"Invalid operator: {op}")

        if op in {"IS NULL", "IS NOT NULL"}:
            where_clauses.append(f"{col} {op}")
            continue
        if op == "IN":
            if not isinstance(val, list) or len(val) == 0:
                raise CompileSQLError("IN requires a non-empty list")
            phs = []
            for j, v in enumerate(val):
                key = f"p{i}_{j}"
                params[key] = v
                phs.append(f":{key}")
            where_clauses.append(f"{col} IN ({', '.join(phs)})")
            continue
        if op == "BETWEEN":
            if not isinstance(val, list) or len(val) != 2:
                raise CompileSQLError("BETWEEN requires [low, high]")
            lo_key, hi_key = f"p{i}a", f"p{i}b"
            params[lo_key], params[hi_key] = val[0], val[1]
            where_clauses.append(f"{col} BETWEEN :{lo_key} AND :{hi_key}")
            continue

        key = f"p{i}"
        params[key] = val
        where_clauses.append(f"{col} {op} :{key}")

    where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    # group by
    group_sql = f" GROUP BY {', '.join(dims)}" if dims else ""

    # order by (compat layer)
    def alias_for(agg: str, col: str) -> str | None:
        for a, c, name in measure_specs:
            if a == agg and c == col:
                return name
        return None

    def normalize_order_item(o: Dict[str, Any]) -> Tuple[str, str]:
        # Allowed names are dims + measure aliases
        allowed = set(dims) | set(measure_aliases)

        # 1) Preferred: explicit name
        name = o.get("name")
        direction = (o.get("dir") or "DESC").upper()

        # 2) Fallback: expr like "avg(Rating)" or an alias string
        if not name:
            expr = o.get("expr")
            if isinstance(expr, str) and expr.strip():
                s = expr.strip()
                # exact alias match
                if s in allowed:
                    name = s
                else:
                    m = re.match(r"^\s*([a-zA-Z]+)\s*\(\s*([\w*]+)\s*\)\s*$", s)
                    if m:
                        agg_s, col_s = m.group(1).lower(), m.group(2)
                        guess = alias_for(agg_s, col_s if col_s != "*" else "*")
                        if guess:
                            name = guess

        # 3) Fallback: {"column": "...", "agg": "..."}
        if not name and o.get("column") and o.get("agg"):
            guess = alias_for(str(o["agg"]).lower(), o["column"])
            if guess:
                name = guess
                0

        # 4) Fallback: {"index": 0}
        if not name and "index" in o:
            try:
                idx = int(o["index"])
                if 0 <= idx < len(measure_aliases):
                    name = measure_aliases[idx]
            except Exception:
                pass

        # 5) Final fallback: first measure alias
        if not name:
            name = measure_aliases[0]

        if name not in allowed:
            raise CompileSQLError(f"Invalid order key: {name}. Must be one of {sorted(allowed)}")

        if direction not in {"ASC", "DESC"}:
            raise CompileSQLError("order_by.dir must be ASC or DESC")

        return name, direction

    raw_order = plan.get("order_by") or plan.get("order") or []
    order_parts: List[str] = []
    for o in raw_order:
        name, direction = normalize_order_item(o or {})
        order_parts.append(f"{name} {direction}")
    order_sql = f" ORDER BY {', '.join(order_parts)}" if order_parts else ""

    # limit
    try:
        limit = int(plan.get("limit", 100) or 100)
    except Exception:
        limit = 100
    if limit < 1:
        limit = 100
    limit = min(limit, 1000)
    limit_sql = f" LIMIT {limit}"

    sql = f"SELECT {', '.join(sel_parts)} FROM {table}{where_sql}{group_sql}{order_sql}{limit_sql}"
    return sql, params
