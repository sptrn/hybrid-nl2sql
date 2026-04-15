import { FormEvent, useEffect, useState } from "react";

type SourceKind = "auto" | "polaris" | "mysql" | "postgresql" | "oracle";

type QueryResponse = {
  mode: string;
  model: string | null;
  selected_sources: SourceKind[];
  generated_sql: Array<{
    source: SourceKind;
    statement: string;
    rationale: string;
  }>;
  guardrails: Array<{
    approved: boolean;
    issues: string[];
    normalized_statement: string | null;
  }>;
  execution_summary: string;
  rows: Array<Record<string, unknown>>;
  metadata: {
    source_count: number;
    spark_sources: Array<{
      source: string;
      enabled: boolean;
      via: string;
    }>;
  };
};

type HealthResponse = {
  status: string;
  app: string;
  llm_mode: string;
  spark_ready: boolean;
};

const sourceOptions: Array<{ value: SourceKind; label: string }> = [
  { value: "auto", label: "Auto" },
  { value: "polaris", label: "Polaris / Iceberg" },
  { value: "mysql", label: "MySQL" },
  { value: "postgresql", label: "PostgreSQL" },
  { value: "oracle", label: "Oracle" }
];

const apiBase = import.meta.env.VITE_API_BASE_URL ?? "/api/v1";

export default function App() {
  const [question, setQuestion] = useState("Show the latest orders from Polaris.");
  const [sourcePreference, setSourcePreference] = useState<SourceKind>("auto");
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [response, setResponse] = useState<QueryResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetch(`${apiBase}/health`)
      .then(async (res) => {
        if (!res.ok) {
          throw new Error(`Health check failed with ${res.status}`);
        }
        return res.json() as Promise<HealthResponse>;
      })
      .then(setHealth)
      .catch((err: Error) => {
        setError(err.message);
      });
  }, []);

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setLoading(true);
    setError(null);

    try {
      const res = await fetch(`${apiBase}/query`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          question,
          source_preference: [sourcePreference],
          max_rows: 100,
          include_explain: true
        })
      });

      if (!res.ok) {
        throw new Error(`Query failed with ${res.status}`);
      }

      const payload = (await res.json()) as QueryResponse;
      setResponse(payload);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unknown error";
      setError(message);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="shell">
      <aside className="sidebar">
        <p className="eyebrow">Hybrid NL2SQL</p>
        <h1>OCI GenAI + Spark SQL workbench</h1>
        <p className="lede">
          Starter UI for schema-aware natural-language querying across Polaris,
          MySQL, PostgreSQL, and Oracle through a single backend.
        </p>

        <div className="status-card">
          <h2>Runtime</h2>
          <dl>
            <div>
              <dt>API</dt>
              <dd>{health?.status ?? "checking"}</dd>
            </div>
            <div>
              <dt>LLM mode</dt>
              <dd>{health?.llm_mode ?? "unknown"}</dd>
            </div>
            <div>
              <dt>Spark</dt>
              <dd>{health ? (health.spark_ready ? "ready" : "not ready") : "unknown"}</dd>
            </div>
          </dl>
        </div>

        <div className="status-card">
          <h2>Suggested prompts</h2>
          <button
            type="button"
            onClick={() => setQuestion("Show the top 10 customers by order total from Polaris.")}
          >
            Top customers in Polaris
          </button>
          <button
            type="button"
            onClick={() => setQuestion("Count active CRM customers in MySQL by region.")}
          >
            CRM counts from MySQL
          </button>
          <button
            type="button"
            onClick={() => setQuestion("List Oracle invoices over 10000 from the latest month.")}
          >
            Oracle invoice filter
          </button>
        </div>
      </aside>

      <main className="content">
        <section className="panel composer">
          <form onSubmit={onSubmit}>
            <label htmlFor="question">Ask a question</label>
            <textarea
              id="question"
              value={question}
              onChange={(event) => setQuestion(event.target.value)}
              rows={6}
            />

            <div className="toolbar">
              <label>
                Source
                <select
                  value={sourcePreference}
                  onChange={(event) => setSourcePreference(event.target.value as SourceKind)}
                >
                  {sourceOptions.map((option) => (
                    <option key={option.value} value={option.value}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </label>

              <button type="submit" disabled={loading}>
                {loading ? "Generating..." : "Generate SQL"}
              </button>
            </div>
          </form>
        </section>

        {error ? <section className="panel error">{error}</section> : null}

        <section className="panel results">
          <div className="panel-header">
            <h2>Results</h2>
            <span>{response?.model ?? "No model yet"}</span>
          </div>

          {!response ? (
            <p className="empty-state">
              Submit a question to inspect the generated SQL and execution summary.
            </p>
          ) : (
            <>
              <div className="chips">
                {response.selected_sources.map((source) => (
                  <span key={source} className="chip">
                    {source}
                  </span>
                ))}
              </div>

              <p className="summary">{response.execution_summary}</p>

              {response.generated_sql.map((query, index) => (
                <article className="sql-card" key={`${query.source}-${index}`}>
                  <div className="sql-meta">
                    <strong>{query.source}</strong>
                    <span>
                      {response.guardrails[index]?.approved ? "approved" : "blocked"}
                    </span>
                  </div>
                  <p>{query.rationale}</p>
                  <pre>{response.guardrails[index]?.normalized_statement ?? query.statement}</pre>
                  {response.guardrails[index]?.issues.length ? (
                    <p className="issue-list">
                      {response.guardrails[index]?.issues.join(" ")}
                    </p>
                  ) : null}
                </article>
              ))}

              <div className="table-wrap">
                {response.rows.length ? (
                  <table>
                    <thead>
                      <tr>
                        {Object.keys(response.rows[0]).map((column) => (
                          <th key={column}>{column}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {response.rows.map((row, index) => (
                        <tr key={index}>
                          {Object.values(row).map((value, cellIndex) => (
                            <td key={cellIndex}>{String(value)}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  <p className="empty-state">
                    No rows returned yet. That is expected until Spark and your sources are fully wired.
                  </p>
                )}
              </div>
            </>
          )}
        </section>
      </main>
    </div>
  );
}

