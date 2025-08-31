DROP VIEW IF EXISTS join_emp_perf;
CREATE VIEW join_emp_perf AS
SELECT
  e.EmpID,
  e.DepID AS Department,
  e.GenderID,
  e.RaceID,
  p.PerfDate,
  CAST(strftime('%Y', p.PerfDate) AS INTEGER) AS Year,
  p.Rating
FROM employee e
JOIN perf p ON e.EmpID = p.EmpID;

DROP VIEW IF EXISTS join_emp_action;
CREATE VIEW join_emp_action AS
SELECT
  e.EmpID,
  e.DepID AS Department,
  e.GenderID,
  e.RaceID,
  a.EffectiveDt AS ActionDate,
  a.ActionID
FROM employee e
LEFT JOIN action a ON e.EmpID = a.EmpID;
