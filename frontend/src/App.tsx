import { FormEvent, useEffect, useState } from "react";

type AppTab = "query" | "backup";
type SourceKind = "auto" | "polaris" | "mysql" | "postgresql" | "oracle";
type BackupSourceKind = "mysql" | "postgresql";
type BackupScope = "table" | "schema" | "database";

type SourceExecution = {
  source: SourceKind;
  execution_summary: string;
  rows: Array<Record<string, unknown>>;
};

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
  execution_results: Array<SourceExecution>;
  rows: Array<Record<string, unknown>>;
  metadata: {
    source_count: number;
    spark_sources: Array<{
      source: string;
      enabled: boolean;
      via: string;
    }>;
    unavailable_sources?: string[];
    llm_generation_error?: string | null;
  };
};

type HealthResponse = {
  status: string;
  app: string;
  llm_mode: string;
  spark_ready: boolean;
};

type BackupTableOption = {
  logical_name: string;
  physical_name: string;
  table_name: string;
  schema_name: string;
  database_name: string;
};

type BackupContainerOption = {
  name: string;
  tables: BackupTableOption[];
};

type BackupSourceOption = {
  source: BackupSourceKind;
  enabled: boolean;
  database_name: string | null;
  schema_label: string;
  database_label: string;
  containers: BackupContainerOption[];
};

type BackupDiscoveryResponse = {
  sources: BackupSourceOption[];
};

type BackupTableResult = {
  source_table: string;
  destination_table: string;
  row_count: number;
  status: string;
  message: string;
};

type BackupResponse = {
  source: BackupSourceKind;
  scope: BackupScope;
  execution_summary: string;
  destination_namespace: string;
  copied_tables: BackupTableResult[];
  metadata: {
    copied_count?: number;
    failed_count?: number;
    selected_count?: number;
    total_rows?: number;
    overwrite?: boolean;
  };
};

type ChartItem = {
  label: string;
  value: number;
  tone?: string;
  subtitle?: string;
  displayValue?: string;
};

type SelectableItem = {
  value: string;
  title: string;
  subtitle: string;
};

const sourceOptions: Array<{ value: SourceKind; label: string }> = [
  { value: "auto", label: "Auto" },
  { value: "polaris", label: "Polaris / Iceberg" },
  { value: "mysql", label: "MySQL" },
  { value: "postgresql", label: "PostgreSQL" },
  { value: "oracle", label: "Oracle" }
];

const backupSourceOptions: Array<{ value: BackupSourceKind; label: string }> = [
  { value: "mysql", label: "MySQL" },
  { value: "postgresql", label: "PostgreSQL" }
];

const backupScopeOptions: Array<{ value: BackupScope; label: string }> = [
  { value: "table", label: "Table" },
  { value: "schema", label: "Schema" },
  { value: "database", label: "Database" }
];

const apiBase = import.meta.env.VITE_API_BASE_URL ?? "/api/v1";
const preferredMetricColumns = [
  "total_spent",
  "total_order_amount",
  "total_quantity_shipped",
  "order_count",
  "shipment_count",
  "quantity",
  "order_total"
];
const preferredDateColumns = ["latest_activity", "order_ts", "shipped_at"];
const preferredLabelColumns = ["customer_name", "product_name", "region", "category"];

function isPresent<T>(value: T | null | undefined): value is T {
  return value != null;
}

function renderValue(value: unknown) {
  return value == null ? "null" : String(value);
}

function parseNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }

  if (typeof value === "string") {
    const normalized = Number(value);
    return Number.isFinite(normalized) ? normalized : null;
  }

  return null;
}

function parseTimestamp(value: unknown): number | null {
  if (typeof value !== "string" || !value.trim()) {
    return null;
  }

  const timestamp = Date.parse(value);
  return Number.isNaN(timestamp) ? null : timestamp;
}

function formatCompactNumber(value: number) {
  return new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: value < 10 ? 1 : 0
  }).format(value);
}

function formatShortDate(timestamp: number | null) {
  if (timestamp == null) {
    return "n/a";
  }

  return new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric"
  }).format(new Date(timestamp));
}

function findMetricColumn(rows: Array<Record<string, unknown>>) {
  if (!rows.length) {
    return null;
  }

  const keys = Object.keys(rows[0]);
  for (const preferred of preferredMetricColumns) {
    if (keys.includes(preferred)) {
      return preferred;
    }
  }

  return keys.find((key) =>
    rows.some((row) => parseNumber(row[key]) != null && !key.endsWith("_id"))
  ) ?? null;
}

function findDateColumn(rows: Array<Record<string, unknown>>) {
  if (!rows.length) {
    return null;
  }

  const keys = Object.keys(rows[0]);
  for (const preferred of preferredDateColumns) {
    if (keys.includes(preferred)) {
      return preferred;
    }
  }

  return keys.find((key) => rows.some((row) => parseTimestamp(row[key]) != null)) ?? null;
}

function findLabelColumn(rows: Array<Record<string, unknown>>) {
  if (!rows.length) {
    return null;
  }

  const keys = Object.keys(rows[0]);
  for (const preferred of preferredLabelColumns) {
    if (keys.includes(preferred)) {
      return preferred;
    }
  }

  return (
    keys.find((key) => !key.endsWith("_id") && rows.some((row) => typeof row[key] === "string")) ??
    keys[0] ??
    null
  );
}

function buildChartSeries(rows: Array<Record<string, unknown>>) {
  const metricColumn = findMetricColumn(rows);
  if (!metricColumn) {
    return [];
  }

  const labelColumn = findLabelColumn(rows);

  return rows
    .map((row, index) => {
      const value = parseNumber(row[metricColumn]);
      if (value == null) {
        return null;
      }

      const labelValue = labelColumn ? row[labelColumn] : null;
      return {
        label: labelValue == null ? `Row ${index + 1}` : String(labelValue),
        value
      };
    })
    .filter((item): item is { label: string; value: number } => item != null)
    .sort((left, right) => right.value - left.value)
    .slice(0, 5);
}

function sumMetric(rows: Array<Record<string, unknown>>) {
  const metricColumn = findMetricColumn(rows);
  if (!metricColumn) {
    return null;
  }

  const total = rows.reduce((sum, row) => sum + (parseNumber(row[metricColumn]) ?? 0), 0);
  return total > 0 ? total : null;
}

function latestTimestamp(rows: Array<Record<string, unknown>>) {
  const dateColumn = findDateColumn(rows);
  if (!dateColumn) {
    return null;
  }

  const timestamps = rows
    .map((row) => parseTimestamp(row[dateColumn]))
    .filter((value): value is number => value != null);

  return timestamps.length ? Math.max(...timestamps) : null;
}

function sourceAccent(source: SourceKind | BackupSourceKind) {
  switch (source) {
    case "polaris":
      return "#24554f";
    case "mysql":
      return "#d9822b";
    case "postgresql":
      return "#2d5bba";
    case "oracle":
      return "#b24a3a";
    default:
      return "#5f6b73";
  }
}

function isPolarisBackupStatement(statement: string | null | undefined) {
  return (statement ?? "").toLowerCase().includes("polaris.backups.");
}

function SourceBarChart({
  title,
  items
}: {
  title: string;
  items: ChartItem[];
}) {
  if (!items.length) {
    return (
      <article className="dashboard-card chart-card">
        <div className="dashboard-card-header">
          <h3>{title}</h3>
        </div>
        <p className="empty-state">No chartable data yet.</p>
      </article>
    );
  }

  const maxValue = Math.max(...items.map((item) => item.value), 1);

  return (
    <article className="dashboard-card chart-card">
      <div className="dashboard-card-header">
        <h3>{title}</h3>
      </div>
      <div className="bar-chart">
        {items.map((item) => (
          <div className="bar-row" key={item.label}>
            <div className="bar-meta">
              <span>{item.label}</span>
              <strong>{item.displayValue ?? formatCompactNumber(item.value)}</strong>
            </div>
            <div className="bar-track">
              <div
                className="bar-fill"
                style={{
                  width: `${(item.value / maxValue) * 100}%`,
                  background: item.tone ?? "#24554f"
                }}
              />
            </div>
            {item.subtitle ? <span className="bar-subtitle">{item.subtitle}</span> : null}
          </div>
        ))}
      </div>
    </article>
  );
}

function SelectionChecklist({
  items,
  selectedValues,
  onToggle,
  onSelectAll,
  onClear
}: {
  items: SelectableItem[];
  selectedValues: string[];
  onToggle: (value: string) => void;
  onSelectAll: () => void;
  onClear: () => void;
}) {
  return (
    <div className="selection-card">
      <div className="selection-card-header">
        <h3>Choose Targets</h3>
        <div className="selection-actions">
          <button type="button" onClick={onSelectAll}>
            Select all
          </button>
          <button type="button" onClick={onClear}>
            Clear
          </button>
        </div>
      </div>

      {items.length ? (
        <div className="selection-list">
          {items.map((item) => {
            const checked = selectedValues.includes(item.value);
            return (
              <label className="selection-item" key={item.value}>
                <input
                  type="checkbox"
                  checked={checked}
                  onChange={() => onToggle(item.value)}
                />
                <div>
                  <strong>{item.title}</strong>
                  <span>{item.subtitle}</span>
                </div>
              </label>
            );
          })}
        </div>
      ) : (
        <p className="empty-state">No selectable targets were discovered for this source.</p>
      )}
    </div>
  );
}

export default function App() {
  const [activeTab, setActiveTab] = useState<AppTab>("query");
  const [question, setQuestion] = useState("Show the latest orders from Polaris.");
  const [sourcePreference, setSourcePreference] = useState<SourceKind>("auto");
  const [health, setHealth] = useState<HealthResponse | null>(null);
  const [response, setResponse] = useState<QueryResponse | null>(null);
  const [submittedQuestion, setSubmittedQuestion] = useState<string | null>(null);
  const [responseVersion, setResponseVersion] = useState(0);
  const [queryLoading, setQueryLoading] = useState(false);
  const [queryError, setQueryError] = useState<string | null>(null);
  const [healthError, setHealthError] = useState<string | null>(null);

  const [backupOptions, setBackupOptions] = useState<BackupDiscoveryResponse | null>(null);
  const [backupSource, setBackupSource] = useState<BackupSourceKind>("mysql");
  const [backupScope, setBackupScope] = useState<BackupScope>("table");
  const [backupTargets, setBackupTargets] = useState<string[]>([]);
  const [backupNamespace, setBackupNamespace] = useState("backups");
  const [backupOverwrite, setBackupOverwrite] = useState(true);
  const [backupLoading, setBackupLoading] = useState(false);
  const [backupError, setBackupError] = useState<string | null>(null);
  const [backupResponse, setBackupResponse] = useState<BackupResponse | null>(null);

  const sourceViews = response
    ? response.generated_sql.map((query, index) => ({
        query,
        guardrail: response.guardrails[index],
        execution: response.execution_results.find((result) => result.source === query.source)
      }))
    : [];

  const executedSources = sourceViews.filter((view) =>
    view.execution?.execution_summary.toLowerCase().startsWith("executed")
  );
  const totalRows = sourceViews.reduce((sum, view) => sum + (view.execution?.rows.length ?? 0), 0);
  const freshestEvent = sourceViews.reduce<number | null>((latest, view) => {
    const candidate = latestTimestamp(view.execution?.rows ?? []);
    if (candidate == null) {
      return latest;
    }
    return latest == null ? candidate : Math.max(latest, candidate);
  }, null);

  const sourceActivityChart = sourceViews
    .map((view) => ({
      label: view.query.source,
      value: view.execution?.rows.length ?? 0,
      tone: sourceAccent(view.query.source),
      subtitle: view.execution?.execution_summary
    }))
    .filter((item) => item.value > 0);

  const recencyChart = sourceViews
    .map((view) => {
      const timestamp = latestTimestamp(view.execution?.rows ?? []);
      return timestamp == null
        ? null
        : {
            label: view.query.source,
            value: timestamp,
            tone: sourceAccent(view.query.source),
            displayValue: formatShortDate(timestamp),
            subtitle: formatShortDate(timestamp)
          };
    })
    .filter(isPresent)
    .sort((left, right) => right.value - left.value);

  const activeBackupSource = backupOptions?.sources.find((source) => source.source === backupSource) ?? null;
  const backupTableCount = activeBackupSource
    ? activeBackupSource.containers.reduce((sum, container) => sum + container.tables.length, 0)
    : 0;
  const backupSelectableItems = buildBackupSelectableItems(activeBackupSource, backupScope);
  const backupChartItems = (backupResponse?.copied_tables ?? [])
    .filter((item) => item.status === "copied")
    .map((item) => ({
      label: item.destination_table.split(".").slice(-1)[0] ?? item.destination_table,
      value: item.row_count,
      tone: sourceAccent(backupResponse?.source ?? backupSource),
      subtitle: item.source_table,
      displayValue: String(item.row_count)
    }))
    .sort((left, right) => right.value - left.value);

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
        setHealthError(err.message);
      });
  }, []);

  useEffect(() => {
    void fetchBackupOptions();
  }, []);

  useEffect(() => {
    if (!activeBackupSource) {
      setBackupTargets([]);
      return;
    }

    if (backupScope === "database" && activeBackupSource.database_name) {
      setBackupTargets([activeBackupSource.database_name]);
      return;
    }

    setBackupTargets([]);
  }, [backupScope, backupSource, activeBackupSource?.database_name]);

  async function fetchBackupOptions() {
    try {
      const res = await fetch(`${apiBase}/backup/options`);
      if (!res.ok) {
        throw new Error(`Backup discovery failed with ${res.status}`);
      }
      const payload = (await res.json()) as BackupDiscoveryResponse;
      setBackupOptions(payload);
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unknown error";
      setBackupError(message);
    }
  }

  async function onSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setQueryLoading(true);
    setQueryError(null);
    setResponse(null);
    setSubmittedQuestion(question);
    setResponseVersion((current) => current + 1);

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
      setQueryError(message);
    } finally {
      setQueryLoading(false);
    }
  }

  async function onBackupSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setBackupLoading(true);
    setBackupError(null);
    setBackupResponse(null);

    try {
      const res = await fetch(`${apiBase}/backup/run`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json"
        },
        body: JSON.stringify({
          source: backupSource,
          scope: backupScope,
          targets: backupTargets,
          destination_namespace: backupNamespace,
          overwrite: backupOverwrite
        })
      });

      if (!res.ok) {
        throw new Error(`Backup failed with ${res.status}`);
      }

      const payload = (await res.json()) as BackupResponse;
      setBackupResponse(payload);
      await fetchBackupOptions();
    } catch (err) {
      const message = err instanceof Error ? err.message : "Unknown error";
      setBackupError(message);
    } finally {
      setBackupLoading(false);
    }
  }

  function toggleBackupTarget(value: string) {
    setBackupTargets((current) =>
      current.includes(value) ? current.filter((item) => item !== value) : [...current, value]
    );
  }

  return (
    <div className="shell">
      <aside className="sidebar">
        <p className="eyebrow">Hybrid NL2SQL</p>
        <h1>OCI GenAI + Spark SQL workbench</h1>
        <p className="lede">
          Query federated sources, then preserve operational history in Iceberg so it becomes queryable through Polaris.
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

        {activeTab === "query" ? (
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
              onClick={() => setQuestion("Compare the latest customer activity between all available data sources.")}
            >
              Cross-source activity
            </button>
            <button
              type="button"
              onClick={() => setQuestion("List Oracle invoices over 10000 from the latest month.")}
            >
              Oracle invoice filter
            </button>
          </div>
        ) : (
          <div className="status-card">
            <h2>Backup flow</h2>
            <p className="lede">
              Choose MySQL or PostgreSQL, select a table, schema, or database, and materialize it into Polaris-managed Iceberg tables for historical querying.
            </p>
          </div>
        )}

        {healthError ? <div className="status-card error">{healthError}</div> : null}
      </aside>

      <main className="content">
        <section className="panel tab-strip">
          <button
            type="button"
            className={activeTab === "query" ? "tab-button active" : "tab-button"}
            onClick={() => setActiveTab("query")}
          >
            NL2SQL Dashboard
          </button>
          <button
            type="button"
            className={activeTab === "backup" ? "tab-button active" : "tab-button"}
            onClick={() => setActiveTab("backup")}
          >
            Backup To Iceberg
          </button>
        </section>

        {activeTab === "query" ? (
          <>
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

                  <button type="submit" disabled={queryLoading}>
                    {queryLoading ? "Generating..." : "Generate SQL"}
                  </button>
                </div>
              </form>
            </section>

            {queryError ? <section className="panel error">{queryError}</section> : null}

            <section className="panel results">
              <div className="panel-header">
                <h2>Results</h2>
                <span>{response?.model ?? "No model yet"}</span>
              </div>

              {queryLoading ? (
                <p className="empty-state">
                  Generating SQL for{submittedQuestion ? `: "${submittedQuestion}"` : " your question"}.
                </p>
              ) : !response ? (
                <p className="empty-state">
                  Submit a question to inspect the generated SQL and execution summary.
                </p>
              ) : (
                <div key={responseVersion}>
                  {submittedQuestion ? <p className="summary">Question: {submittedQuestion}</p> : null}
                  <div className="chips">
                    {response.selected_sources.map((source) => (
                      <span key={source} className="chip">
                        {source}
                      </span>
                    ))}
                  </div>

                  <p className="summary">{response.execution_summary}</p>
                  {response.metadata.llm_generation_error ? (
                    <p className="issue-list">
                      OCI generation fell back to backup SQL: {response.metadata.llm_generation_error}
                    </p>
                  ) : null}

                  <section className="dashboard-grid">
                    <article className="dashboard-card">
                      <div className="dashboard-card-header">
                        <h3>Sources Executed</h3>
                      </div>
                      <strong className="metric-value">{executedSources.length}</strong>
                      <p className="metric-caption">
                        of {sourceViews.length} generated source plans ran successfully
                      </p>
                    </article>

                    <article className="dashboard-card">
                      <div className="dashboard-card-header">
                        <h3>Rows Surfaced</h3>
                      </div>
                      <strong className="metric-value">{formatCompactNumber(totalRows)}</strong>
                      <p className="metric-caption">combined preview rows across executed sources</p>
                    </article>

                    <article className="dashboard-card">
                      <div className="dashboard-card-header">
                        <h3>Freshest Event</h3>
                      </div>
                      <strong className="metric-value">{formatShortDate(freshestEvent)}</strong>
                      <p className="metric-caption">most recent timestamp visible in returned datasets</p>
                    </article>
                  </section>

                  <section className="dashboard-grid charts-grid">
                    <SourceBarChart title="Rows By Source" items={sourceActivityChart} />
                    <SourceBarChart title="Latest Activity By Source" items={recencyChart} />
                  </section>

                  {sourceViews.map((view, index) => {
                    const series = buildChartSeries(view.execution?.rows ?? []);
                    const aggregate = sumMetric(view.execution?.rows ?? []);
                    const displayedStatement = view.guardrail?.normalized_statement ?? view.query.statement;
                    const isBackupCard =
                      view.query.source === "polaris" && isPolarisBackupStatement(displayedStatement);

                    return (
                      <article className="sql-card dashboard-source-card" key={`${view.query.source}-${index}`}>
                        <div className="sql-meta">
                          <div className="sql-meta-primary">
                            <strong>{view.query.source}</strong>
                            {isBackupCard ? (
                              <span className="table-kind-badge">Polaris Backup Table</span>
                            ) : null}
                          </div>
                          <span>{view.guardrail?.approved ? "approved" : "blocked"}</span>
                        </div>
                        <p>{view.query.rationale}</p>

                        <div className="source-dashboard-layout">
                          <div className="source-dashboard-panel">
                            <div className="mini-metrics">
                              <div>
                                <span>Rows</span>
                                <strong>{view.execution?.rows.length ?? 0}</strong>
                              </div>
                              <div>
                                <span>Primary Metric</span>
                                <strong>{aggregate == null ? "n/a" : formatCompactNumber(aggregate)}</strong>
                              </div>
                            </div>
                            <SourceBarChart
                              title="Top Drivers"
                              items={series.map((item) => ({
                                ...item,
                                tone: sourceAccent(view.query.source)
                              }))}
                            />
                          </div>

                          <div className="source-dashboard-panel sql-panel">
                            <details open>
                              <summary>Generated SQL</summary>
                              <pre>{displayedStatement}</pre>
                            </details>
                          </div>
                        </div>

                        {view.guardrail?.issues.length ? (
                          <p className="issue-list">{view.guardrail.issues.join(" ")}</p>
                        ) : null}
                        {(() => {
                          const execution = view.execution;

                          if (!execution) {
                            return null;
                          }

                          return (
                            <div className="table-wrap">
                              <p className="summary">{execution.execution_summary}</p>
                              {execution.rows.length ? (
                                <table>
                                  <thead>
                                    <tr>
                                      {Object.keys(execution.rows[0]).map((column) => (
                                        <th key={column}>{column}</th>
                                      ))}
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {execution.rows.map((row, rowIndex) => (
                                      <tr key={rowIndex}>
                                        {Object.values(row).map((value, cellIndex) => (
                                          <td key={cellIndex}>{renderValue(value)}</td>
                                        ))}
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              ) : (
                                <p className="empty-state">No rows returned for this source yet.</p>
                              )}
                            </div>
                          );
                        })()}
                      </article>
                    );
                  })}
                </div>
              )}
            </section>
          </>
        ) : (
          <>
            <section className="panel composer">
              <form className="backup-form" onSubmit={onBackupSubmit}>
                <div className="backup-grid">
                  <label>
                    Source system
                    <select
                      value={backupSource}
                      onChange={(event) => setBackupSource(event.target.value as BackupSourceKind)}
                    >
                      {backupSourceOptions.map((option) => (
                        <option key={option.value} value={option.value}>
                          {option.label}
                        </option>
                      ))}
                    </select>
                  </label>

                  <label>
                    Backup scope
                    <select
                      value={backupScope}
                      onChange={(event) => setBackupScope(event.target.value as BackupScope)}
                    >
                      {backupScopeOptions.map((option) => (
                        <option key={option.value} value={option.value}>
                          {option.label}
                        </option>
                      ))}
                    </select>
                  </label>

                  <label>
                    Polaris namespace prefix
                    <input
                      type="text"
                      value={backupNamespace}
                      onChange={(event) => setBackupNamespace(event.target.value)}
                    />
                  </label>

                  <label className="toggle-card">
                    <span>Write mode</span>
                    <button
                      type="button"
                      className={backupOverwrite ? "toggle-button active" : "toggle-button"}
                      onClick={() => setBackupOverwrite((current) => !current)}
                    >
                      {backupOverwrite ? "Overwrite existing tables" : "Append into existing tables"}
                    </button>
                  </label>
                </div>

                <section className="dashboard-grid backup-summary-grid">
                  <article className="dashboard-card">
                    <div className="dashboard-card-header">
                      <h3>Catalog Discovered</h3>
                    </div>
                    <strong className="metric-value">{activeBackupSource?.containers.length ?? 0}</strong>
                    <p className="metric-caption">schemas currently introspected for {backupSource}</p>
                  </article>

                  <article className="dashboard-card">
                    <div className="dashboard-card-header">
                      <h3>Tables Available</h3>
                    </div>
                    <strong className="metric-value">{backupTableCount}</strong>
                    <p className="metric-caption">tables ready to copy into Polaris/Iceberg</p>
                  </article>

                  <article className="dashboard-card">
                    <div className="dashboard-card-header">
                      <h3>Targets Selected</h3>
                    </div>
                    <strong className="metric-value">{backupTargets.length}</strong>
                    <p className="metric-caption">scope items that will be materialized on the next run</p>
                  </article>
                </section>

                {activeBackupSource && !activeBackupSource.enabled ? (
                  <section className="panel error">
                    {backupSource} is not configured for JDBC backup in this environment yet.
                  </section>
                ) : null}

                <SelectionChecklist
                  items={backupSelectableItems}
                  selectedValues={backupTargets}
                  onToggle={toggleBackupTarget}
                  onSelectAll={() => setBackupTargets(backupSelectableItems.map((item) => item.value))}
                  onClear={() => setBackupTargets([])}
                />

                <div className="toolbar">
                  <p className="summary">
                    Backups are materialized as Iceberg tables under
                    {" "}
                    <code>polaris.{backupNamespace}.{backupSource}...</code>
                  </p>
                  <button type="submit" disabled={backupLoading || !activeBackupSource?.enabled}>
                    {backupLoading ? "Backing up..." : "Materialize Backup"}
                  </button>
                </div>
              </form>
            </section>

            {backupError ? <section className="panel error">{backupError}</section> : null}

            <section className="panel results">
              <div className="panel-header">
                <h2>Iceberg Backup Dashboard</h2>
                <span>{activeBackupSource?.database_name ?? "No source selected"}</span>
              </div>

              {backupLoading ? (
                <p className="empty-state">Copying source data into Polaris/Iceberg.</p>
              ) : !backupResponse ? (
                <p className="empty-state">
                  Choose a source, select the tables or namespaces you want, and run a backup to publish historical data into Polaris.
                </p>
              ) : (
                <div>
                  <p className="summary">{backupResponse.execution_summary}</p>
                  <div className="chips">
                    <span className="chip">{backupResponse.source}</span>
                    <span className="chip">{backupResponse.scope}</span>
                    <span className="chip">{backupResponse.destination_namespace}</span>
                  </div>

                  <section className="dashboard-grid">
                    <article className="dashboard-card">
                      <div className="dashboard-card-header">
                        <h3>Tables Copied</h3>
                      </div>
                      <strong className="metric-value">{backupResponse.metadata.copied_count ?? 0}</strong>
                      <p className="metric-caption">
                        successful Iceberg tables now visible in the Polaris catalog
                      </p>
                    </article>

                    <article className="dashboard-card">
                      <div className="dashboard-card-header">
                        <h3>Rows Materialized</h3>
                      </div>
                      <strong className="metric-value">
                        {formatCompactNumber(backupResponse.metadata.total_rows ?? 0)}
                      </strong>
                      <p className="metric-caption">rows copied across the current backup run</p>
                    </article>

                    <article className="dashboard-card">
                      <div className="dashboard-card-header">
                        <h3>Mode</h3>
                      </div>
                      <strong className="metric-value">
                        {backupResponse.metadata.overwrite ? "Replace" : "Append"}
                      </strong>
                      <p className="metric-caption">write strategy used when materializing Iceberg tables</p>
                    </article>
                  </section>

                  <section className="dashboard-grid charts-grid">
                    <SourceBarChart title="Rows Copied By Table" items={backupChartItems} />
                    <article className="dashboard-card chart-card">
                      <div className="dashboard-card-header">
                        <h3>Next Query Step</h3>
                      </div>
                      <p className="summary">
                        Your new historical tables are published under
                        {" "}
                        <code>polaris.{backupResponse.destination_namespace}.{backupResponse.source}...</code>.
                      </p>
                      <p className="summary">
                        Switch back to the NL2SQL tab and ask for trends, history, or cross-source comparisons against the new Polaris backup tables.
                      </p>
                    </article>
                  </section>

                  <div className="backup-result-list">
                    {backupResponse.copied_tables.map((table) => (
                      <article className="sql-card backup-result-card" key={table.destination_table}>
                        <div className="sql-meta">
                          <strong>{table.destination_table}</strong>
                          <span className={table.status === "copied" ? "status-good" : "status-bad"}>
                            {table.status}
                          </span>
                        </div>
                        <div className="backup-result-meta">
                          <span>Source: {table.source_table}</span>
                          <span>Rows: {table.row_count}</span>
                        </div>
                        <p className="summary">{table.message}</p>
                      </article>
                    ))}
                  </div>
                </div>
              )}
            </section>
          </>
        )}
      </main>
    </div>
  );
}

function buildBackupSelectableItems(
  source: BackupSourceOption | null,
  scope: BackupScope
): SelectableItem[] {
  if (!source) {
    return [];
  }

  if (scope === "table") {
    return source.containers.flatMap((container) =>
      container.tables.map((table) => ({
        value: table.physical_name,
        title: table.logical_name,
        subtitle: `${table.physical_name} -> polaris.backups.${source.source}.${table.database_name}.${table.table_name}`
      }))
    );
  }

  if (scope === "schema") {
    return source.containers.map((container) => ({
      value: container.name,
      title: container.name,
      subtitle: `${container.tables.length} tables will be copied from this schema`
    }));
  }

  const tableCount = source.containers.reduce((sum, container) => sum + container.tables.length, 0);
  return [
    {
      value: source.database_name ?? source.source,
      title: source.database_name ?? source.source,
      subtitle: `${tableCount} tables across ${source.containers.length} schemas will be copied`
    }
  ];
}
