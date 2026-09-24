import { useCallback, useEffect, useRef, useState } from "react";
import {
  Activity,
  ArrowRight,
  Bell,
  Bookmark,
  Check,
  ChevronRight,
  CircleDot,
  ExternalLink,
  Layers3,
  LogOut,
  Radio,
  RefreshCw,
  Search,
  Send,
  Settings2,
  Sparkles,
  Telescope,
  Waypoints,
  X,
} from "lucide-react";
import { api, defaultBackend, safeUrl, validateBackend, watch } from "./api";
import type {
  Agent,
  Briefing,
  Card,
  Case,
  CaseDetail,
  Overview,
  Preferences,
  Source,
  Task,
} from "./api";

const familyNames: Record<string, string> = {
  light: "Electromagnetic",
  gravity: "Gravitational waves",
  neutrino: "Neutrinos",
};
const date = (value?: number | string) =>
  value
    ? new Date(
        typeof value === "number" ? value * 1000 : value,
      ).toLocaleString()
    : "No observations yet";
type Tab = "briefing" | "cases" | "agents" | "sources" | "subscriptions";

function Empty({ title, text }: { title: string; text: string }) {
  return (
    <div className="empty">
      <CircleDot size={32} />
      <h3>{title}</h3>
      <p>{text}</p>
    </div>
  );
}
function Tags({ values }: { values: string[] }) {
  return (
    <div className="tags">
      {values.map((v) => (
        <span key={v} className={"tag " + v}>
          {familyNames[v] || v}
        </span>
      ))}
    </div>
  );
}
function Brief({
  value,
  openCase,
}: {
  value: Briefing;
  openCase: (id: string) => void;
}) {
  return (
    <>
      <div className="card-meta">
        <span className={"status " + value.status}>{value.status}</span>
        <span>
          {value.type === "case" ? "EVENT CASE" : "AGENT BRIEFING"}{" "}
          {value.version ? `· REV ${value.version}` : ""}
        </span>
      </div>
      <h3>{value.title}</h3>
      <p>{value.summary}</p>
      <Tags values={value.families} />
      {value.interpretation && (
        <div className="interpretation">
          <span>
            <Sparkles size={13} /> Agent interpretation
          </span>
          <p>{value.interpretation}</p>
        </div>
      )}
      <p className="uncertainty">{value.uncertainty}</p>
      <div className="card-bottom">
        <span>{value.evidence_ids.length} evidence records</span>
        {value.case_id && (
          <button
            className="text-button"
            onClick={() => openCase(value.case_id!)}
          >
            Open case <ArrowRight size={14} />
          </button>
        )}
      </div>
    </>
  );
}

export default function App() {
  const [base, setBase] = useState(defaultBackend),
    [token, setToken] = useState(
      () => sessionStorage.getItem("observatory-token") || "",
    );
  const [connected, setConnected] = useState(false),
    [connecting, setConnecting] = useState(false);
  const [tab, setTab] = useState<Tab>("briefing"),
    [error, setError] = useState(""),
    [notice, setNotice] = useState("");
  const [overview, setOverview] = useState<Overview | null>(null),
    [agents, setAgents] = useState<Agent[]>([]);
  const [cases, setCases] = useState<Case[]>([]),
    [messages, setMessages] = useState<Card[]>([]),
    [sources, setSources] = useState<Source[]>([]);
  const [tasks, setTasks] = useState<Task[]>([]),
    [prefs, setPrefs] = useState<Preferences>({
      families: [],
      instruments: [],
      multi_messenger_only: false,
    });
  const [selected, setSelected] = useState<Agent | null>(null),
    [detail, setDetail] = useState<CaseDetail | null>(null);
  const [question, setQuestion] = useState(""),
    [verify, setVerify] = useState(true),
    [busy, setBusy] = useState(false);
  const [filter, setFilter] = useState("all"),
    [query, setQuery] = useState(""),
    [lastSync, setLastSync] = useState<number>();
  const refreshing = useRef(false);
  const preferencesDirty = useRef(false);
  function editPrefs(value: Preferences) {
    preferencesDirty.current = true;
    setPrefs(value);
  }
  const call = useCallback(
    <T,>(path: string, options?: RequestInit) =>
      api<T>(base, token, path, options),
    [base, token],
  );
  const refresh = useCallback(async () => {
    if (refreshing.current) return;
    refreshing.current = true;
    try {
      const [o, a, c, m, s, t, p] = await Promise.all([
        call<Overview>("/overview"),
        call<Agent[]>("/agents"),
        call<Case[]>("/cases"),
        call<Card[]>("/messages"),
        call<Source[]>("/sources"),
        call<Task[]>("/tasks"),
        call<Preferences>("/preferences"),
      ]);
      setOverview(o);
      setAgents(a);
      setCases((previous) => [
        ...c,
        ...previous.filter((old) => !c.some((row) => row.id === old.id)),
      ]);
      setMessages((previous) => [
        ...m,
        ...previous.filter((old) => !m.some((row) => row.id === old.id)),
      ]);
      setSources(s);
      setTasks(t);
      if (!preferencesDirty.current) setPrefs(p);
      setError("");
      setLastSync(Date.now());
    } catch (e) {
      setError((e as Error).message);
    } finally {
      refreshing.current = false;
    }
  }, [call]);
  useEffect(() => {
    if (!connected) return;
    void refresh();
    const interval = setInterval(() => void refresh(), 10000);
    const abort = new AbortController();
    void watch(base, token, () => void refresh(), abort.signal);
    return () => {
      clearInterval(interval);
      abort.abort();
    };
  }, [connected, refresh, base, token]);
  useEffect(() => {
    if (!notice) return;
    const id = setTimeout(() => setNotice(""), 4000);
    return () => clearTimeout(id);
  }, [notice]);
  useEffect(() => {
    if (!connected || !detail?.id) return;
    const id = detail.id;
    const interval = setInterval(() => {
      void call<CaseDetail>("/cases/" + encodeURIComponent(id))
        .then(setDetail)
        .catch((e) => setError(e.message));
    }, 5000);
    return () => clearInterval(interval);
  }, [connected, detail?.id, call]);

  async function connect(event: React.FormEvent) {
    event.preventDefault();
    setConnecting(true);
    setError("");
    try {
      const origin = validateBackend(base);
      const o = await api<Overview>(origin, token, "/overview");
      localStorage.setItem("observatory-backend", origin);
      sessionStorage.setItem("observatory-token", token);
      setBase(origin);
      setOverview(o);
      setConnected(true);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setConnecting(false);
    }
  }
  async function action(fn: () => Promise<unknown>, success = "") {
    setBusy(true);
    try {
      await fn();
      if (success) setNotice(success);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  async function openCase(id: string) {
    setBusy(true);
    try {
      setDetail(await call<CaseDetail>("/cases/" + encodeURIComponent(id)));
      setSelected(null);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  async function ask() {
    const target = selected?.id || detail?.agent_id || "sky";
    await action(async () => {
      await call("/tasks", {
        method: "POST",
        body: JSON.stringify({
          agent_id: target,
          message: question,
          case_id: detail?.id || null,
          verify,
          request_id: crypto.randomUUID(),
        }),
      });
      setQuestion("");
      await refresh();
    }, "Request queued. Results appear in your briefing.");
  }
  async function loadMore() {
    if (tab === "cases" && cases.length) {
      const more = await call<Case[]>(
        "/cases?before=" + cases[cases.length - 1].updated_at,
      );
      setCases((current) => [...current, ...more]);
    } else if (messages.length) {
      const more = await call<Card[]>(
        "/messages?before=" + messages[messages.length - 1].seq,
      );
      setMessages((current) => [...current, ...more]);
    }
  }

  if (!connected)
    return (
      <div className="connect-page">
        <div className="connect-orbit" aria-hidden="true" />
        <header className="brand">
          <Telescope size={25} />
          <span>OBSERVATORY</span>
        </header>
        <main className="connect-content">
          <span className="eyebrow">A SHARED VIEW OF THE UNIVERSE</span>
          <h1>
            Many instruments.
            <br />
            One unfolding story.
          </h1>
          <p className="connect-lead">
            A living network of agents following the sky. Connect to your
            observatory to explore real GCN alerts, source evidence, and events
            as they develop.
          </p>
          <div className="connect-families">
            <span>
              <i className="light" /> Light
            </span>
            <span>
              <i className="gravity" /> Gravitational waves
            </span>
            <span>
              <i className="neutrino" /> Neutrinos
            </span>
          </div>
          <form className="connect-form" onSubmit={connect}>
            <h2>Connect to your observatory</h2>
            <label>
              Backend URL
              <input
                type="url"
                required
                value={base}
                onChange={(e) => setBase(e.target.value)}
                placeholder="https://observatory.example.com"
              />
            </label>
            <label>
              Access token
              <input
                type="password"
                required
                value={token}
                onChange={(e) => setToken(e.target.value)}
                placeholder="Your observer or administrator token"
                autoComplete="off"
              />
            </label>
            {error && (
              <div role="alert" className="error">
                {error}
              </div>
            )}
            <button className="primary" disabled={connecting}>
              {connecting ? "Connecting…" : "Open observatory"}
              <ArrowRight size={17} />
            </button>
            <p>
              GCN credentials stay on your backend. Your access token is kept
              only for this browser session.
            </p>
          </form>
          <div className="connect-note">
            12 permanent agents · Persistent event cases · Source-backed
            briefings
          </div>
        </main>
      </div>
    );

  const live =
    overview?.gcn.state === "connected" &&
    Boolean(overview?.workers_online) &&
    !error;
  const displayed = messages.filter(
    (m) =>
      m.status !== "superseded" &&
      !messages.some(
        (other) =>
          other.payload.id === m.payload.id &&
          (other.payload.version || 0) > (m.payload.version || 0),
      ) &&
      (filter === "all" ||
        (filter === "saved" && m.saved) ||
        m.payload.families.includes(filter)),
  );
  const filteredCases = cases.filter((c) =>
    c.name.toLowerCase().includes(query.toLowerCase()),
  );
  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="brand">
          <Telescope size={25} />
          <span>
            OBSERVATORY<small>MULTI-MESSENGER INTELLIGENCE</small>
          </span>
        </div>
        <div className="workspace-label">YOUR WORKSPACE</div>
        <nav>
          {(
            [
              ["briefing", "Briefing desk", Bell],
              ["cases", "Event cases", Layers3],
              ["agents", "Agent network", Waypoints],
              ["sources", "Live streams", Radio],
              ["subscriptions", "Subscriptions", Settings2],
            ] as const
          ).map(([id, label, Icon]) => (
            <button
              className={tab === id ? "nav-item active" : "nav-item"}
              key={id}
              onClick={() => setTab(id)}
            >
              <Icon size={18} />
              {label}
              {id === "briefing" && Boolean(overview?.unread) && (
                <b>{overview?.unread}</b>
              )}
            </button>
          ))}
        </nav>
        <div className="sidebar-bottom">
          <span className={"connection " + (live ? "live" : "")}>
            <i />
            {live ? "GCN connected" : "Awaiting live connection"}
          </span>
          <p>NASA GCN + participating observatories</p>
          <button
            className="text-button"
            onClick={() => {
              sessionStorage.removeItem("observatory-token");
              setConnected(false);
              setToken("");
              setError("");
            }}
          >
            <LogOut size={14} /> Disconnect
          </button>
        </div>
      </aside>
      <main className="workspace">
        <header className="topbar">
          <span>
            Mission control <ChevronRight size={13} />{" "}
            {tab === "briefing" ? "Briefing desk" : tab}
          </span>
          <div>
            <span className="utc">
              {lastSync
                ? `Synced ${new Date(lastSync).toLocaleTimeString()}`
                : "Connecting"}
            </span>
            <button
              className="icon-button"
              aria-label="Refresh"
              onClick={() => void refresh()}
            >
              <RefreshCw size={16} />
            </button>
            <span className="avatar">
              {overview?.role === "admin" ? "OP" : "OB"}
            </span>
          </div>
        </header>
        <div className="workspace-content">
          {error && (
            <div className="error" role="alert">
              {error} · Last retrieved data remains visible.
            </div>
          )}
          {!live && overview && (
            <div className="connection-banner">
              <Radio size={18} />
              <div>
                <strong>
                  {overview.gcn.state === "connected"
                    ? "Waiting for agent workers"
                    : "Live ingestion is not connected"}
                </strong>
                <p>
                  {overview.gcn.error ||
                    `GCN status: ${overview.gcn.state}. Check Live streams for connection details.`}
                </p>
              </div>
            </div>
          )}
          <div className="page-heading">
            <div>
              <span className="eyebrow">THE UNIVERSE, IN CONTEXT</span>
              <h1>
                {
                  {
                    briefing: "Your view of the sky.",
                    cases: "Follow the whole story.",
                    agents: "Intelligence, composed.",
                    sources: "Straight from the source.",
                    subscriptions: "Keep the signals that matter.",
                  }[tab]
                }
              </h1>
              <p>
                {
                  {
                    briefing:
                      "Instrument alerts become evidence. Agents connect the context.",
                    cases:
                      "Every revision, follow-up, and retraction stays with its event.",
                    agents:
                      "Specialists report upward. Event cases bring their evidence together.",
                    sources:
                      "External Kafka streams from NASA’s General Coordinates Network.",
                    subscriptions:
                      "Choose the families and instruments you want in your briefing.",
                  }[tab]
                }
              </p>
            </div>
            <span className={"live-badge " + (live ? "online" : "")}>
              <i />
              {live ? "LIVE OBSERVATORY" : "CONNECTION PENDING"}
            </span>
          </div>
          <div className="stats">
            {[
              [
                "EVENT CASES",
                overview?.cases ?? "—",
                "Persistent investigations",
                Layers3,
              ],
              [
                "SOURCE REPORTS",
                overview?.reports ?? "—",
                "Current live revisions",
                Radio,
              ],
              [
                "PERMANENT AGENTS",
                overview?.agents ?? "—",
                `${overview?.workers_online || 0} workers online`,
                Waypoints,
              ],
              [
                "QUEUED WORK",
                overview?.pending ?? "—",
                `${overview?.quarantined || 0} quarantined records`,
                Activity,
              ],
            ].map(([label, value, note, Icon]) => {
              const I = Icon as typeof Activity;
              return (
                <div className="stat" key={String(label)}>
                  <span>
                    {String(label)}
                    <I size={16} />
                  </span>
                  <strong>{String(value)}</strong>
                  <p>{String(note)}</p>
                </div>
              );
            })}
          </div>
          {tab === "briefing" && (
            <div className="briefing-layout">
              <section>
                <div className="section-heading">
                  <h2>
                    Latest intelligence <span>{displayed.length}</span>
                  </h2>
                  <div className="filter-tabs">
                    {["all", "light", "gravity", "neutrino", "saved"].map(
                      (f) => (
                        <button
                          key={f}
                          className={filter === f ? "active" : ""}
                          onClick={() => setFilter(f)}
                        >
                          {
                            (
                              {
                                all: "All signals",
                                light: "Light",
                                gravity: "Gravity",
                                neutrino: "Neutrinos",
                                saved: "Saved",
                              } as Record<string, string>
                            )[f]
                          }
                        </button>
                      ),
                    )}
                  </div>
                </div>
                {!displayed.length && (
                  <Empty
                    title="Listening for the next story"
                    text="Briefings will appear when live source messages have been received and processed. No example events are loaded."
                  />
                )}
                <div className="feed">
                  {displayed.map((card) => (
                    <article
                      className={"briefing-card " + card.payload.status}
                      key={card.id}
                    >
                      <Brief
                        value={card.payload}
                        openCase={(id) => void openCase(id)}
                      />
                      <div className="message-actions">
                        <time>{date(card.created_at)}</time>
                        <button
                          aria-label={
                            card.saved ? "Unsave briefing" : "Save briefing"
                          }
                          className={
                            card.saved ? "icon-button selected" : "icon-button"
                          }
                          disabled={busy}
                          onClick={() =>
                            void action(async () => {
                              await call("/messages/" + card.id, {
                                method: "PATCH",
                                body: JSON.stringify({ saved: !card.saved }),
                              });
                              await refresh();
                            })
                          }
                        >
                          <Bookmark size={16} />
                        </button>
                        <button
                          className="text-button"
                          disabled={busy || card.status === "read"}
                          onClick={() =>
                            void action(async () => {
                              await call("/messages/" + card.id, {
                                method: "PATCH",
                                body: JSON.stringify({ read: true }),
                              });
                              await refresh();
                            })
                          }
                        >
                          <Check size={14} />
                          {card.status === "read" ? "Read" : "Mark read"}
                        </button>
                      </div>
                    </article>
                  ))}
                </div>
                {messages.length >= 50 && (
                  <button
                    className="secondary"
                    onClick={() => void action(loadMore)}
                  >
                    Load older briefings
                  </button>
                )}
              </section>
              <aside className="right-rail">
                <div className="ask-panel">
                  <div className="panel-icon">
                    <Sparkles size={21} />
                  </div>
                  <h2>Ask the whole sky</h2>
                  <p>
                    The coordinator can ask its specialists to review their
                    stored evidence.
                  </p>
                  <textarea
                    aria-label="Question for coordinator"
                    value={question}
                    onChange={(e) => setQuestion(e.target.value)}
                    placeholder="What changed across the instruments?"
                  />
                  <label className="checkbox">
                    <input
                      type="checkbox"
                      checked={verify}
                      onChange={(e) => setVerify(e.target.checked)}
                    />{" "}
                    Ask child agents to check
                  </label>
                  <button
                    className="primary"
                    disabled={!question.trim() || busy}
                    onClick={() => void ask()}
                  >
                    Send request <Send size={15} />
                  </button>
                  <small>
                    {overview?.model
                      ? `Reasoning model: ${overview.model}`
                      : "Evidence mode · configure a model for natural-language analysis"}
                  </small>
                </div>
                <div className="rail-panel">
                  <h3>Agent requests</h3>
                  {!tasks.length && (
                    <p className="muted">
                      Your agent conversations start here.
                    </p>
                  )}
                  {tasks.slice(0, 6).map((t) => (
                    <div className="task-item" key={t.id}>
                      <span className={"status " + t.state}>{t.state}</span>
                      <p>{t.request.message}</p>
                      {t.error && <p className="error-text">{t.error}</p>}
                      {t.result && (
                        <p>{t.result.interpretation || t.result.summary}</p>
                      )}
                      {["submitted", "working"].includes(t.state) && (
                        <button
                          className="text-button"
                          onClick={() =>
                            void action(async () => {
                              await call(`/tasks/${t.id}/cancel`, {
                                method: "POST",
                              });
                              await refresh();
                            })
                          }
                        >
                          Cancel request
                        </button>
                      )}
                    </div>
                  ))}
                </div>
                <div className="rail-panel quiet">
                  <h3>Evidence before interpretation</h3>
                  <p>
                    A shared event reference connects reports. It does not, by
                    itself, confirm a shared physical origin.
                  </p>
                </div>
              </aside>
            </div>
          )}
          {tab === "cases" && (
            <section className="panel">
              <div className="section-heading">
                <h2>Event case files</h2>
                <label className="search">
                  <Search size={16} />
                  <input
                    aria-label="Search cases"
                    placeholder="Search event identifier"
                    value={query}
                    onChange={(e) => setQuery(e.target.value)}
                  />
                </label>
              </div>
              {!filteredCases.length && (
                <Empty
                  title="No matching event cases"
                  text="Case agents are created automatically when external alerts arrive."
                />
              )}
              <div className="case-grid">
                {filteredCases.map((c) => (
                  <button
                    className="case-card"
                    key={c.id}
                    onClick={() => void openCase(c.id)}
                  >
                    <div className="card-meta">
                      <CircleDot size={17} />
                      <span
                        className={"status " + (c.briefing?.status || "queued")}
                      >
                        {c.briefing?.status || "queued"}
                      </span>
                    </div>
                    <h3>{c.name}</h3>
                    <p>
                      {c.briefing?.summary ||
                        "A case agent is reviewing this source report."}
                    </p>
                    <Tags values={c.briefing?.families || []} />
                    <div className="card-bottom">
                      <time>{date(c.updated_at)}</time>
                      <ArrowRight size={16} />
                    </div>
                  </button>
                ))}
              </div>
              {cases.length >= 50 && (
                <button
                  className="secondary"
                  onClick={() => void action(loadMore)}
                >
                  Load older cases
                </button>
              )}
            </section>
          )}
          {tab === "agents" && (
            <section className="network">
              <div className="root-agent">
                <span className="eyebrow">WHOLE-SKY COORDINATOR</span>
                <button
                  onClick={() => {
                    setDetail(null);
                    setSelected(agents.find((a) => a.id === "sky") || null);
                  }}
                >
                  <Telescope size={29} />
                  <div>
                    <h2>One shared perspective</h2>
                    <p>
                      Cross-family changes, themes, and evidence-backed answers
                    </p>
                  </div>
                  <ChevronRight />
                </button>
              </div>
              <div className="family-grid">
                {Object.entries(familyNames).map(([family, label]) => (
                  <div className={"family-column " + family} key={family}>
                    <button
                      className="family-heading"
                      onClick={() =>
                        setSelected(
                          agents.find((a) => a.id === "family:" + family) ||
                            null,
                        )
                      }
                    >
                      <i />
                      <div>
                        <span>SIGNAL FAMILY</span>
                        <h3>{label}</h3>
                      </div>
                      <ChevronRight size={17} />
                    </button>
                    {agents
                      .filter(
                        (a) => a.level === "instrument" && a.family === family,
                      )
                      .map((a) => (
                        <button
                          className="agent-card"
                          key={a.id}
                          onClick={() => setSelected(a)}
                        >
                          <div>
                            <span className={"agent-dot " + a.state} />
                            <strong>{a.name}</strong>
                            <span className="agent-state">{a.state}</span>
                          </div>
                          <p>
                            {a.memory.fact_count || 0} reports · {a.runs} runs
                          </p>
                          <small>
                            {a.pending
                              ? `${a.pending} jobs waiting`
                              : "Wakes on incoming evidence"}
                          </small>
                        </button>
                      ))}
                  </div>
                ))}
              </div>
              <div className="crosscutting">
                <button
                  onClick={() =>
                    setSelected(
                      agents.find((a) => a.id === "circulars") || null,
                    )
                  }
                >
                  <Layers3 size={22} />
                  <div>
                    <h3>GCN Circulars agent</h3>
                    <p>Human-written observations and follow-up bulletins</p>
                  </div>
                  <ChevronRight size={18} />
                </button>
                <button onClick={() => setTab("cases")}>
                  <Waypoints size={22} />
                  <div>
                    <h3>{overview?.cases || 0} event-case agents</h3>
                    <p>
                      Durable case files connecting evidence across branches
                    </p>
                  </div>
                  <ChevronRight size={18} />
                </button>
              </div>
            </section>
          )}
          {tab === "sources" && (
            <section className="panel">
              <div className="section-heading">
                <div>
                  <h2>External GCN subscriptions</h2>
                  <p className="muted">
                    GCN status: {overview?.gcn.state} · Heartbeat:{" "}
                    {date(overview?.gcn.heartbeat)}
                  </p>
                </div>
                <span className="tag">Kafka → durable inbox → agents</span>
              </div>
              <div className="source-list">
                {sources.map((s) => (
                  <div className="source-row" key={s.id}>
                    <div>
                      <code>{s.id}</code>
                      <small>
                        {agents.find((a) => a.id === s.definition.agent_id)
                          ?.name || s.definition.agent_id}
                      </small>
                      {s.error && <p className="error-text">{s.error}</p>}
                    </div>
                    <span className={"status " + s.state}>
                      {s.state.replaceAll("_", " ")}
                    </span>
                    <div>
                      <b>{s.item_count}</b>
                      <small>messages retained</small>
                    </div>
                    <time>{date(s.last_success)}</time>
                  </div>
                ))}
              </div>
              <p className="source-note">
                A quiet instrument can be healthy. Connection heartbeats and
                Kafka lag are tracked separately from the last scientific alert.
              </p>
              {overview?.gcn.lag && (
                <details>
                  <summary>Partition lag</summary>
                  <pre>{JSON.stringify(overview.gcn.lag, null, 2)}</pre>
                </details>
              )}
            </section>
          )}
          {tab === "subscriptions" && (
            <section className="preferences panel">
              <div>
                <h2>Your observation interests</h2>
                <p>
                  Leave everything unselected to receive the full observatory
                  briefing. Corrections to previously delivered findings still
                  reach you.
                </p>
              </div>
              <div>
                <h3>Signal families</h3>
                {Object.entries(familyNames).map(([id, label]) => (
                  <label className="checkbox" key={id}>
                    <input
                      type="checkbox"
                      checked={prefs.families.includes(id)}
                      onChange={(e) =>
                        editPrefs({
                          ...prefs,
                          families: e.target.checked
                            ? [...prefs.families, id]
                            : prefs.families.filter((f) => f !== id),
                        })
                      }
                    />
                    {label}
                  </label>
                ))}
                <h3>Instruments</h3>
                <div className="instrument-options">
                  {agents
                    .filter((a) => a.level === "instrument")
                    .map((a) => (
                      <label className="checkbox" key={a.id}>
                        <input
                          type="checkbox"
                          checked={prefs.instruments.includes(a.id)}
                          onChange={(e) =>
                            editPrefs({
                              ...prefs,
                              instruments: e.target.checked
                                ? [...prefs.instruments, a.id]
                                : prefs.instruments.filter((i) => i !== a.id),
                            })
                          }
                        />
                        {a.name}
                      </label>
                    ))}
                </div>
                <label className="checkbox">
                  <input
                    type="checkbox"
                    checked={prefs.multi_messenger_only}
                    onChange={(e) =>
                      editPrefs({
                        ...prefs,
                        multi_messenger_only: e.target.checked,
                      })
                    }
                  />{" "}
                  Only briefings with multiple signal families
                </label>
                <p className="muted">
                  Multiple families can include follow-up searches and
                  non-detections.
                </p>
                <button
                  className="primary"
                  disabled={busy}
                  onClick={() =>
                    void action(async () => {
                      await call("/preferences", {
                        method: "PUT",
                        body: JSON.stringify(prefs),
                      });
                      preferencesDirty.current = false;
                    }, "Subscriptions saved")
                  }
                >
                  Save subscriptions <Check size={16} />
                </button>
              </div>
            </section>
          )}
          <footer>
            <span>
              <Telescope size={15} /> Space Observatory
            </span>
            <p>Many signals. Traceable evidence.</p>
            <a href={base + "/docs"} target="_blank" rel="noreferrer">
              API documentation <ExternalLink size={12} />
            </a>
          </footer>
        </div>
      </main>
      {(selected || detail) && (
        <div
          className="drawer-backdrop"
          onClick={() => {
            setSelected(null);
            setDetail(null);
          }}
        >
          <aside
            className="drawer"
            onClick={(e) => e.stopPropagation()}
            aria-label="Details"
          >
            <button
              className="drawer-close icon-button"
              aria-label="Close details"
              onClick={() => {
                setSelected(null);
                setDetail(null);
              }}
            >
              <X />
            </button>
            <span className="eyebrow">
              {selected ? `${selected.level} AGENT` : "EVENT CASE FILE"}
            </span>
            <h2>{selected?.name || detail?.name}</h2>
            {selected && (
              <>
                <span className="status">{selected.state}</span>
                <p>
                  {selected.runs} completed runs · {selected.pending} pending
                  jobs
                </p>
                <p className="muted">
                  Heartbeat: {date(selected.last_heartbeat)}
                </p>
                {selected.memory.assessment && (
                  <div className="interpretation">
                    <span>Agent interpretation</span>
                    <p>{selected.memory.assessment}</p>
                  </div>
                )}
                <h3>Recent evidence</h3>
                {selected.memory.recent_titles?.map((t, i) => (
                  <p className="memory-line" key={i}>
                    {t}
                  </p>
                ))}
                {!selected.memory.recent_titles?.length && (
                  <p className="muted">Waiting for relevant source reports.</p>
                )}
                <details>
                  <summary>Agent interface</summary>
                  <code>{selected.topic}</code>
                  <p>
                    A2A endpoint: {base}/a2a/{selected.id}/
                  </p>
                </details>
              </>
            )}
            {detail && (
              <>
                <Brief value={detail.briefing!} openCase={() => {}} />
                <h3>Current source reports</h3>
                {detail.reports.map((r) => (
                  <div className="evidence" key={r.id}>
                    <span className={"status " + r.payload.status}>
                      {r.payload.status} · {r.payload.kind}
                    </span>
                    <h4>{r.payload.title}</h4>
                    <p>{r.payload.summary}</p>
                    <a
                      href={safeUrl(r.payload.source_url)}
                      target="_blank"
                      rel="noreferrer"
                    >
                      Source reference <ExternalLink size={12} />
                    </a>
                    <details>
                      <summary>Measurements and source references</summary>
                      <pre>
                        {JSON.stringify(
                          {
                            metadata: r.payload.metadata,
                            references: r.payload.references,
                          },
                          null,
                          2,
                        )}
                      </pre>
                    </details>
                    <button
                      className="text-button"
                      onClick={() =>
                        void action(async () => {
                          const raw = await call<{ payload: string }>(
                            "/raw/" + r.payload.raw_id,
                          );
                          const u = URL.createObjectURL(
                            new Blob([raw.payload], { type: "text/plain" }),
                          );
                          const a = document.createElement("a");
                          a.href = u;
                          a.download =
                            "gcn-" + r.payload.raw_id.slice(0, 12) + ".txt";
                          a.click();
                          setTimeout(() => URL.revokeObjectURL(u), 1000);
                        })
                      }
                    >
                      Download original Kafka payload
                    </button>
                  </div>
                ))}
                <h3>Revision history</h3>
                {detail.history.map((r) => (
                  <div className="history-item" key={r.id}>
                    <span>{date(r.payload.notice_time)}</span>
                    <p>
                      {r.payload.title} · {r.payload.status}
                    </p>
                  </div>
                ))}
              </>
            )}
            <div className="drawer-question">
              <h3>Ask {selected ? "this agent" : "the case agent"}</h3>
              <textarea
                aria-label="Question for selected agent"
                value={question}
                onChange={(e) => setQuestion(e.target.value)}
                placeholder="What does the evidence establish?"
              />
              <button
                className="primary"
                disabled={!question.trim() || busy}
                onClick={() => void ask()}
              >
                Send request <Send size={15} />
              </button>
            </div>
          </aside>
        </div>
      )}
      {notice && (
        <div className="toast" role="status">
          <Check size={16} />
          {notice}
        </div>
      )}
    </div>
  );
}
