# load_to_sqlite.py
# Usage:
#   pip install pandas
#   python load_to_sqlite.py --data-dir ./data --db ./hr.db
import argparse, sqlite3, sys, os, csv
import pandas as pd
from pathlib import Path

DDL = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS employee (
  EmpID      TEXT PRIMARY KEY,
  EmpName    TEXT,
  EngDt      TEXT,   -- ISO date string YYYY-MM-DD
  TermDt     TEXT,   -- ISO date string
  DepID      TEXT,
  GenderID   TEXT,
  RaceID     TEXT,
  MgrID      TEXT,   -- manager EmpID
  DOB        TEXT,   -- ISO date string
  PayRate    REAL
);

CREATE TABLE IF NOT EXISTS action (
  ActID        TEXT PRIMARY KEY,
  ActionID     TEXT,     -- e.g., 'hire','promotion','demotion','attrition' OR a code
  EmpID        TEXT NOT NULL,
  EffectiveDt  TEXT,     -- ISO date string
  FOREIGN KEY (EmpID) REFERENCES employee(EmpID)
);
CREATE INDEX IF NOT EXISTS idx_action_empid ON action(EmpID);
CREATE INDEX IF NOT EXISTS idx_action_effective ON action(EffectiveDt);

CREATE TABLE IF NOT EXISTS perf (
  PerfID     TEXT PRIMARY KEY,
  EmpID      TEXT NOT NULL,
  Rating     REAL NOT NULL,
  PerfDate   TEXT,   -- ISO date string
  FOREIGN KEY (EmpID) REFERENCES employee(EmpID)
);
CREATE INDEX IF NOT EXISTS idx_perf_empid ON perf(EmpID);
CREATE INDEX IF NOT EXISTS idx_perf_date ON perf(PerfDate);
"""

def iso_dateify(series):
    # Convert to YYYY-MM-DD strings; leave invalid as None
    s = pd.to_datetime(series, errors="coerce").dt.date.astype("string")
    # Pandas may output '<NA>'; convert to None so sqlite stores NULL
    return s.where(~s.eq("<NA>"), None)

def load_employee(conn, path):
    df = pd.read_csv(path)
    # normalize columns (strip spaces, exact match expected)
    df.columns = [c.strip() for c in df.columns]
    required = {"EmpID","EmpName","EngDt","TermDt","DepID","GenderID","RaceID","MgrID","DOB","PayRate"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"employee CSV missing columns: {missing}")

    # types
    df["EmpID"]   = df["EmpID"].astype(str).str.strip()
    df["EmpName"] = df["EmpName"].astype(str).str.strip()
    df["DepID"]   = df["DepID"].astype(str).str.strip()
    df["GenderID"]= df["GenderID"].astype(str).str.strip()
    df["RaceID"]  = df["RaceID"].astype(str).str.strip()
    df["MgrID"]   = df["MgrID"].astype(str).str.strip()
    df["PayRate"] = pd.to_numeric(df["PayRate"], errors="coerce")

    # dates → ISO strings
    df["EngDt"] = iso_dateify(df["EngDt"])
    df["TermDt"] = iso_dateify(df["TermDt"])
    df["DOB"] = iso_dateify(df["DOB"])

    # dedupe on EmpID (keep first)
    df = df.drop_duplicates(subset=["EmpID"], keep="first")
    allowed = ["EmpID","EmpName","EngDt","TermDt","DepID","GenderID","RaceID","MgrID","DOB","PayRate"]
    extra = [c for c in df.columns if c not in allowed]
    if extra: print(f"Dropping unexpected employee columns: {extra}")
    df = df[allowed]
    df.to_sql("employee", conn, if_exists="append", index=False)
    return len(df)

def load_action(conn, path):
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    required = {"ActID","ActionID","EmpID","EffectiveDt"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"action CSV missing columns: {missing}")

    df["ActID"] = df["ActID"].astype(str).str.strip()
    df["ActionID"] = df["ActionID"].astype(str).str.strip()
    df["EmpID"] = df["EmpID"].astype(str).str.strip()
    df["EffectiveDt"] = iso_dateify(df["EffectiveDt"])

    df = df.drop_duplicates(subset=["ActID"], keep="first")
    allowed = ["ActID","ActionID","EmpID","EffectiveDt"]
    extra = [c for c in df.columns if c not in allowed]
    if extra: print(f"Dropping unexpected action columns: {extra}")
    df = df[allowed]
    df.to_sql("action", conn, if_exists="append", index=False)
    return len(df)

def load_perf(conn, path):
    df = pd.read_csv(path)
    df.columns = [c.strip() for c in df.columns]
    required = {"PerfID","EmpID","Rating","PerfDate"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"perf CSV missing columns: {missing}")

    df["PerfID"] = df["PerfID"].astype(str).str.strip()
    df["EmpID"] = df["EmpID"].astype(str).str.strip()
    df["Rating"] = pd.to_numeric(df["Rating"], errors="coerce")
    df["PerfDate"] = iso_dateify(df["PerfDate"])

    df = df.drop_duplicates(subset=["PerfID"], keep="first")
    allowed = ["PerfID","EmpID","Rating","PerfDate"]
    extra = [c for c in df.columns if c not in allowed]
    if extra: print(f"Dropping unexpected perf columns: {extra}")
    df = df[allowed]
    df.to_sql("perf", conn, if_exists="append", index=False)
    return len(df)

def stage_hr_txt(conn, path):
    # Tries to read a structured text file with unknown delimiter; stores as raw staging.
    # Result table: hr_data_raw (all TEXT)
    try:
        df = pd.read_csv(path, sep=None, engine="python", dtype=str)
    except Exception:
        # fallback: try tab, semicolon, pipe
        for sep in ["\t",";","|",","]:
            try:
                df = pd.read_csv(path, sep=sep, dtype=str)
                break
            except Exception:
                df = None
        if df is None:
            print(f"Could not parse {path}; skipped.")
            return 0
    df.columns = [str(c).strip() for c in df.columns]
    # write as raw staging (strings)
    # make table if not exists with generic columns
    cols = ", ".join([f'"{c}" TEXT' for c in df.columns])
    create = f'CREATE TABLE IF NOT EXISTS hr_data_raw ({cols});'
    conn.execute(create)
    df.to_sql("hr_data_raw", conn, if_exists="append", index=False)
    return len(df)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", required=True, help="Folder containing your CSV/TXT files")
    ap.add_argument("--db", default="./hr.db", help="SQLite DB path to create/use")
    ap.add_argument("--stage-hr-txt", action="store_true", help="Also stage HR DATA.txt into hr_data_raw")
    args = ap.parse_args()

    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        print(f"Data dir not found: {data_dir}")
        sys.exit(1)

    # Connect DB + create tables
    conn = sqlite3.connect(args.db)
    try:
        conn.executescript(DDL)
        conn.commit()

        # Load EMPLOYEE first (FK target)
        emp_path = data_dir / "tbl_Employee.csv"
        act_path = data_dir / "tbl_Action.csv"
        perf_path = data_dir / "tbl_Perf.csv"
        hr_txt   = data_dir / "HR DATA.txt"

        if not emp_path.exists(): raise FileNotFoundError(emp_path)
        if not act_path.exists(): print("WARN: tbl_Action.csv not found; skipping.")
        if not perf_path.exists(): print("WARN: tbl_Perf.csv not found; skipping.")

        emp_n = load_employee(conn, emp_path)
        print(f"Loaded employee rows: {emp_n}")

        if act_path.exists():
            act_n = load_action(conn, act_path)
            print(f"Loaded action rows: {act_n}")

        if perf_path.exists():
            perf_n = load_perf(conn, perf_path)
            print(f"Loaded perf rows: {perf_n}")

        if args.stage_hr_txt and hr_txt.exists():
            staged = stage_hr_txt(conn, hr_txt)
            print(f"Staged HR DATA.txt rows: {staged} (table: hr_data_raw)")
        elif args.stage_hr_txt:
            print("WARN: --stage-hr-txt set, but HR DATA.txt not found; skipped.")

        # quick sanity counts
        for tbl in ["employee","action","perf"]:
            try:
                cur = conn.execute(f"SELECT COUNT(*) FROM {tbl}")
                print(f"{tbl} count:", cur.fetchone()[0])
            except Exception:
                pass

        conn.commit()
        print(f"Done. SQLite DB at: {os.path.abspath(args.db)}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
