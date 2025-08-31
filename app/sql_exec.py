from sqlalchemy import create_engine, text
import os, hashlib

ENGINE = create_engine(os.getenv("DATABASE_URL","sqlite:///./hr.db"), future=True)

def run_query(sql: str, params: dict):
    with ENGINE.connect() as conn:
        res = conn.execute(text(sql), params)
        rows = res.fetchall()
        cols = list(res.keys())
    return {
        "columns": cols,
        "rows": [list(r) for r in rows],
        "row_count": len(rows),
        "sql": sql,
        "params": params,
        "trust": {"engine": ENGINE.dialect.name,
                  "query_hash": hashlib.md5((sql+repr(params)).encode()).hexdigest()}
    }
