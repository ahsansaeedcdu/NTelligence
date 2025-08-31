import React, { useState, useEffect, useMemo } from "react";

// Local storage hook
function useLocalMemory(key, initialValue) {
  const [value, setValue] = useState(() => {
    const saved = localStorage.getItem(key);
    return saved ? JSON.parse(saved) : initialValue;
  });
  useEffect(() => {
    localStorage.setItem(key, JSON.stringify(value));
  }, [key, value]);
  return [value, setValue];
}

export default function App() {
  const [prompt, setPrompt] = useState("");
  const [runs, setRuns] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);
  const [mockMode, setMockMode] = useLocalMemory("mockMode", true);
  const [mem, setMem] = useLocalMemory("ntelligence_memory", {
    date_from: "2023-08-30",
    date_to: "2025-08-30",
    top_k: 10,
  });

  // ---- Suggestions (auto-filled from memory) -------------------------------
  const suggestions = useMemo(() => {
    const df = mem.date_from || "2024-01-01";
    const dt = mem.date_to || "2025-12-31";
    const k = mem.top_k || 10;
    const yearTo = (dt || "").slice(0, 4) || "2025";
    return [
      {
        label: `Top ${k} races by avg rating (${yearTo})`,
        prompt: `Top ${k} races by average rating in ${yearTo}.`,
      },
      {
        label: `Promotions by Dept (${df} → ${dt})`,
        prompt: `Promotions by Department between ${df} and ${dt}.`,
      },
      {
        label: `Active headcount by Dept (as of ${dt})`,
        prompt: `Active headcount by department as of ${dt}.`,
      },
      {
        label: `Avg pay by Dept & Gender (active)`,
        prompt: `Average pay by department and gender (active employees only).`,
      },
      {
        label: `Perf records per Dept (${df} → ${dt})`,
        prompt: `Count of performance records per Department between ${df} and ${dt}.`,
      },
    ];
  }, [mem]);

  const runAsk = async (q) => {
    if (!q || !q.trim()) return;
    setError(null);
    setLoading(true);
    try {
      let resp;
      if (mockMode) {
        resp = await mockBackend({ prompt: q, memory: mem });
      } else {
        const res = await fetch("http://localhost:8000/api/ask", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ prompt: q, memory: mem }),
        });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        resp = await res.json();
      }
      // Always store the original prompt with the run
      setRuns((r) => [{ id: Date.now(), prompt: q, ...resp }, ...r]);
      // Clear the input AFTER a successful run
      setPrompt("");
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  };

  // Optional: Ctrl/Cmd+Enter to run
  const onTextKeyDown = (e) => {
    if ((e.ctrlKey || e.metaKey) && e.key === "Enter" && !loading) {
      runAsk(prompt);
    }
  };

  return (
    <div className="min-h-screen w-full flex items-center justify-center bg-gradient-to-br from-gray-50 to-gray-100 dark:from-gray-900 dark:to-gray-950 text-gray-900 dark:text-gray-100 p-6">
      <div className="w-full max-w-6xl rounded-3xl shadow-2xl bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-700 flex flex-col h-[95vh]">
        {/* Header */}
        <header className="bg-white dark:bg-gray-800 shadow p-4 flex items-center justify-between rounded-t-3xl">
          <h1 className="text-xl font-extrabold tracking-tight">⚡ NTelligence</h1>
          <div className="flex items-center gap-2 text-sm">
            {/* <label className="text-gray-600 dark:text-gray-300">Mock Mode</label> */}
            {/* <input
              type="checkbox"
              className="accent-blue-600 w-4 h-4"
              checked={mockMode}
              onChange={(e) => setMockMode(e.target.checked)}
            /> */}
          </div>
        </header>

        {/* Main */}
        <main className="flex-1 overflow-y-auto w-full p-6 grid grid-cols-1 lg:grid-cols-3 gap-6">
          {/* Context Memory */}
          <div className="bg-gray-50 dark:bg-gray-800 rounded-2xl shadow-inner p-6 space-y-4 border border-gray-200 dark:border-gray-700">
            <h2 className="font-bold text-lg">⚙️ Context Memory</h2>
            <div className="space-y-2">
              <label className="block text-sm">Date from</label>
              <input
                className="border rounded-lg w-full px-3 py-2 focus:ring-2 focus:ring-blue-400 focus:outline-none dark:bg-gray-900 dark:border-gray-700"
                value={mem.date_from}
                onChange={(e) => setMem({ ...mem, date_from: e.target.value })}
              />
            </div>
            <div className="space-y-2">
              <label className="block text-sm">Date to</label>
              <input
                className="border rounded-lg w-full px-3 py-2 focus:ring-2 focus:ring-blue-400 focus:outline-none dark:bg-gray-900 dark:border-gray-700"
                value={mem.date_to}
                onChange={(e) => setMem({ ...mem, date_to: e.target.value })}
              />
            </div>
            <div className="space-y-2">
              <label className="block text-sm">Top-K</label>
              <input
                type="number"
                className="border rounded-lg w-full px-3 py-2 focus:ring-2 focus:ring-blue-400 focus:outline-none dark:bg-gray-900 dark:border-gray-700"
                value={mem.top_k}
                onChange={(e) => setMem({ ...mem, top_k: Number(e.target.value) })}
                min={1}
              />
            </div>
          </div>

          {/* Chat + Results */}
          <div className="lg:col-span-2 space-y-6">
            {/* Ask */}
            <div className="bg-gray-50 dark:bg-gray-800 rounded-2xl shadow-inner p-6 border border-gray-200 dark:border-gray-700">
              <textarea
                className="w-full border rounded-xl px-3 py-3 min-h-[100px] focus:ring-2 focus:ring-blue-500 focus:outline-none resize-none dark:bg-gray-900 dark:border-gray-700"
                placeholder="💬 Ask something about your data..."
                value={prompt}
                onChange={(e) => setPrompt(e.target.value)}
                onKeyDown={onTextKeyDown}
              />
              {/* Suggestions */}
              <div className="flex flex-wrap gap-2 mt-3">
                {suggestions.map((s, idx) => (
                  <button
                    key={idx}
                    title={s.prompt}
                    className="text-xs md:text-sm px-3 py-1.5 rounded-full border border-gray-300 dark:border-gray-700 bg-white dark:bg-gray-900 hover:bg-gray-100 dark:hover:bg-gray-800 transition shadow-sm"
                    onClick={() => runAsk(s.prompt)}
                    disabled={loading}
                  >
                    💡 {s.label}
                  </button>
                ))}
              </div>

              <div className="flex items-center gap-3 mt-4">
                <button
                  className="bg-blue-600 hover:bg-blue-700 transition text-white px-5 py-2 rounded-lg shadow-md disabled:opacity-50"
                  onClick={() => runAsk(prompt)}
                  disabled={loading || !prompt.trim()}
                >
                  {loading ? "⏳ Running..." : "🚀 Run"}
                </button>
                <button
                  className="px-5 py-2 rounded-lg border shadow-sm hover:bg-gray-100 dark:hover:bg-gray-700"
                  onClick={() => setRuns([])}
                >
                  🧹 Clear
                </button>
                {error && <div className="text-red-600 text-sm">{error}</div>}
              </div>
            </div>

            {/* Results */}
            {runs.map((r) => (
              <div key={r.id} className="bg-white dark:bg-gray-800 rounded-2xl shadow-lg p-6 space-y-4 border border-gray-200 dark:border-gray-700">
                <div className="text-sm text-gray-500 dark:text-gray-400">📝 Prompt: {r.prompt}</div>
                <div>
                  <h3 className="font-semibold mb-1">📋 QueryPlan</h3>
                  <pre className="bg-gray-100 dark:bg-gray-900 p-3 rounded-lg text-xs overflow-auto border border-gray-200 dark:border-gray-700">
                    {JSON.stringify(r.plan, null, 2)}
                  </pre>
                </div>
                <div>
                  <h3 className="font-semibold mb-1">🛠️ SQL</h3>
                  <pre className="bg-gray-100 dark:bg-gray-900 p-3 rounded-lg text-xs overflow-auto border border-gray-200 dark:border-gray-700">
                    {r.sql}
                  </pre>
                </div>
                <div>
                  <h3 className="font-semibold mb-1">📊 Rows ({r.row_count})</h3>
                  {Array.isArray(r.rows) && r.rows.length > 0 ? (
                    <table className="w-full text-sm border border-gray-200 dark:border-gray-700 rounded-lg overflow-hidden">
                      <thead>
                        <tr className="bg-gray-50 dark:bg-gray-900">
                          {r.columns.map((c) => (
                            <th key={c} className="border px-3 py-2 text-left font-medium">{c}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {r.rows.map((row, i) => (
                          <tr key={i} className="odd:bg-white even:bg-gray-50 dark:odd:bg-gray-800 dark:even:bg-gray-900">
                            {row.map((cell, j) => (
                              <td key={j} className="border px-3 py-2">{String(cell)}</td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  ) : <div className="text-gray-500 dark:text-gray-400 text-sm">No data returned.</div>}
                </div>
                <div>
                  <h3 className="font-semibold mb-1">💡 Summary</h3>
                  <p className="text-sm bg-blue-50 dark:bg-blue-900 border border-blue-100 dark:border-blue-700 rounded-lg p-3">
                    {r.summary || "(No summary)"}
                  </p>
                </div>
              </div>
            ))}
          </div>
        </main>
      </div>
    </div>
  );
}

// --- Mock backend for demo ---
async function mockBackend(req) {
  await new Promise((r) => setTimeout(r, 300));
  return {
    prompt: req.prompt,
    plan: {
      table: "join_emp_action",
      intent: "aggregate",
      dimensions: ["Department"],
      measures: [{ name: "promotions", agg: "count", column: "EmpID" }],
      order_by: [{ expr: "promotions", dir: "desc" }],
      limit: 100
    },
    sql: "SELECT Department, COUNT(EmpID) AS promotions FROM join_emp_action GROUP BY Department ORDER BY promotions DESC LIMIT 100",
    params: {},
    columns: ["Department", "promotions"],
    rows: [["Finance", 15], ["IT", 11], ["HR", 8]],
    row_count: 3,
    summary: "Finance leads promotions (15), followed by IT (11) and HR (8).",
    trust: { engine: "sqlite", query_hash: "mock-abc123" },
  };
}
