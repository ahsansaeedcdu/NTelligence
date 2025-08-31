from app.compiler import compile_sql
sql, params = compile_sql({
  "table":"join_emp_perf","intent":"aggregate",
  "dimensions":["Year"],
  "measures":[{"name":"avg_rating","agg":"avg","column":"Rating"}],
  "filters":[], "order_by":[{"expr":"Year","dir":"asc"}], "limit":10
})
print(sql, params)