# NTelligence — Backend (FastAPI)

NTelligence Backend is a compact FastAPI service that powers the conversational data pipeline (UI → API → Agents → KB → Verify → Audit). It compiles parameterized SQL against configured allowed tables and views (e.g., employee, action, perf, join_emp_perf, join_emp_action) and returns grounded answers with plan/SQL/summary/trust. Designed for quick demos with a single entrypoint and minimal setup.

---

## Setup

### 1) Prerequisites
- Python **3.11+**
- `pip`

### 2) Create & activate a virtual environment
**macOS/Linux**
```bash
python3 -m venv .venv && source .venv/bin/activate
```
**Windows (PowerShell)**
```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 3) Install dependencies
```bash
pip install fastapi "uvicorn[standard]" sqlalchemy pandas pydantic pydantic-ai openai python-dotenv
```

### 4) Configure environment variables
Create a `.env` file next to `server.py`:
```ini
OPENAI_API_KEY=sk-********************************
DATABASE_URL=sqlite:///./ntelligence.db
; OPENAI_MODEL=gpt-4o-mini
CORS_ORIGINS=http://localhost:5173,http://127.0.0.1:5173
```

### 5) (Optional) Create curated views in your DB
> Agent 3 executes **only** on these views for stable joins/types. Adjust SQL to your schema.

```sql
CREATE VIEW IF NOT EXISTS join_emp_perf AS
SELECT p.PerfID, e.EmployeeID, e.DepartmentID, p.Rating, p.PerfDate
FROM perf p JOIN employee e ON e.EmployeeID = p.EmployeeID;

CREATE VIEW IF NOT EXISTS join_emp_action AS
SELECT a.ActionID, e.EmployeeID, e.DepartmentID, a.ActionType, a.ActionDate
FROM action a JOIN employee e ON e.EmployeeID = a.EmployeeID;
```

### 6) Run the API (from the folder that contains `server.py`)
**Dev (hot reload)**
```bash
uvicorn server:app --reload --port 8000
```
**Prod (simple)**
```bash
uvicorn server:app --host 0.0.0.0 --port 8000
```

---

## Project Structure (minimal reference)
```
backend/
  server.py                # FastAPI entrypoint (exposes `app`)
  app/
    agents.py
    compiler.py            # compile_sql(...)
    sql_exec.py            # run_query(...)
    models.py              # QueryPlan (Pydantic)
    utils.py               # normalize_plan_json(...)
  .env
  README.md
```
