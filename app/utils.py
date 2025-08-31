def normalize_plan_json(raw: str) -> dict:
    import json
    d = json.loads(raw)

    # --- Normalize measures ---
    for m in d.get("measures", []):
        agg = m.get("agg", "").lower()
        if agg == "count_distinct":
            m["agg"] = "count"
        # CASE WHEN expressions → treat as column
        if "CASE WHEN" in str(m.get("column", "")):
            m["column"] = "ActionID"   # safe fallback column

    # --- Normalize filters ---
    for f in d.get("filters", []):
        # map expr/col/val → column/value
        if "col" in f: f["column"] = f.pop("col")
        if "expr" in f: f["column"] = f.pop("expr")
        if "val" in f: f["value"] = f.pop("val")

        # fix operators
        op = f.get("op", "").lower()
        if op == "gte": f["op"] = ">="
        elif op == "lte": f["op"] = "<="
        elif op == "in": f["op"] = "IN"
        elif op == "between": f["op"] = "BETWEEN"
        else:
            f["op"] = f.get("op", "").upper()

    # --- Default limit ---
    if d.get("limit") is None:
        d["limit"] = 100

    return d
