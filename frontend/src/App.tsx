import React, { useEffect, useMemo, useState } from "react";
import {
  Activity,
  ArrowDown,
  ArrowRight,
  Bell,
  BookOpen,
  Check,
  CheckCheck,
  ChevronRight,
  CircleDot,
  Compass,
  ExternalLink,
  Globe2,
  Layers3,
  Loader2,
  MapPin,
  MessageSquare,
  Network,
  Plus,
  Radio,
  RefreshCw,
  Search,
  Send,
  Settings2,
  ShieldCheck,
  SlidersHorizontal,
  Sparkles,
  TriangleAlert,
  X,
} from "lucide-react";
import {
  api,
  watchMessages,
  type Agent,
  type Card,
  type Source,
  type Overview,
  type Preferences,
  type Task,
} from "./api";
import "./styles.css";

const countries: Record<string, { name: string; color: string; flag: string }> =
  {
    ee: { name: "Estonia", color: "#568bba", flag: "🇪🇪" },
    lv: { name: "Latvia", color: "#a75761", flag: "🇱🇻" },
    lt: { name: "Lithuania", color: "#a99145", flag: "🇱🇹" },
  };
const timeAgo = (n: number) =>
  !n
    ? "Never checked"
    : new Intl.RelativeTimeFormat("en", { numeric: "auto" }).format(
        -Math.max(0, Math.floor((Date.now() / 1000 - n) / 60)),
        "minute",
      );
const dateLabel = (s?: string | null) =>
  s
    ? new Date(s).toLocaleDateString("en", { month: "short", day: "numeric" })
    : "Date unconfirmed";
const cityLabel = (id: string) =>
  id.split(":").pop()?.replaceAll("-", " ") || id;
function IconMark() {
  return (
    <div className="brand-mark">
      <Compass size={26} />
    </div>
  );
}

export default function App() {
  const [tab, setTab] = useState("briefing");
  const [overview, setOverview] = useState<Overview | null>(null);
  const [agents, setAgents] = useState<Agent[]>([]);
  const [cards, setCards] = useState<Card[]>([]);
  const [sources, setSources] = useState<Source[]>([]);
  const [tasks, setTasks] = useState<Task[]>([]);
  const [prefs, setPrefs] = useState<Preferences | null>(null);
  const [country, setCountry] = useState("all");
  const [kind, setKind] = useState("all");
  const [search, setSearch] = useState("");
  const [question, setQuestion] = useState("");
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState("");
  const [error, setError] = useState("");
  const [selected, setSelected] = useState<Agent | null>(null);
  const [access, setAccess] = useState("");
  const [login, setLogin] = useState(false);
  const [verify, setVerify] = useState(false);
  const [savedOnly, setSavedOnly] = useState(false);
  const merge = (incoming: Card[]) =>
    setCards((previous) => {
      const map = new Map(previous.map((x) => [x.id, x]));
      incoming.forEach((x) => map.set(x.id, x));
      const latest = new Map<string, number>();
      for (const x of map.values())
        latest.set(
          x.finding_id,
          Math.max(latest.get(x.finding_id) || 0, x.version),
        );
      return [...map.values()]
        .filter((x) => x.version === latest.get(x.finding_id))
        .sort((a, b) => b.seq - a.seq);
    });
  async function refresh() {
    try {
      const [o, a, m, s, t, p] = await Promise.all([
        api<Overview>("/overview"),
        api<Agent[]>("/agents"),
        api<Card[]>("/messages?limit=500"),
        api<Source[]>("/sources"),
        api<Task[]>("/tasks"),
        api<Preferences>("/preferences"),
      ]);
      setOverview(o);
      setAgents(a);
      merge(m);
      setSources(s);
      setTasks(t);
      setPrefs(p);
      setError("");
      setLogin(false);
    } catch (e) {
      const msg = (e as Error).message;
      setError(msg);
      if (msg.includes("token")) setLogin(true);
    }
  }
  useEffect(() => {
    refresh();
    const interval = setInterval(refresh, 4000);
    const abort = new AbortController();
    watchMessages((card) => merge([card]), abort.signal);
    return () => {
      clearInterval(interval);
      abort.abort();
    };
  }, []);
  useEffect(() => {
    if (notice) {
      const id = setTimeout(() => setNotice(""), 5000);
      return () => clearTimeout(id);
    }
  }, [notice]);
  async function act(fn: () => Promise<unknown>, success: string) {
    setBusy(true);
    try {
      await fn();
      setNotice(success);
      await refresh();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  }
  async function ask() {
    if (!question.trim()) return;
    const message = question;
    await act(
      () =>
        api("/query", {
          method: "POST",
          body: JSON.stringify({
            message,
            agent_id: selected?.id || "baltic:coordinator",
            verify,
            request_id: crypto.randomUUID(),
          }),
        }),
      "Your request is queued. The answer will appear in your briefing.",
    );
    setQuestion("");
  }
  const filtered = useMemo(
    () =>
      cards.filter(
        (c) =>
          c.status !== "dismissed" &&
          c.status !== "superseded" &&
          (country === "all" || c.payload.countries?.includes(country)) &&
          (kind === "all" || c.payload.type === kind) &&
          (!savedOnly || c.saved) &&
          (!search ||
            `${c.payload.title} ${c.payload.summary}`
              .toLowerCase()
              .includes(search.toLowerCase())),
      ),
    [cards, country, kind, search, savedOnly],
  );
  const activeTasks = tasks.filter((t) =>
    ["submitted", "working"].includes(t.state),
  );
  const visibleAgents = agents.filter(
    (a) => a.level === "city" && (country === "all" || a.country === country),
  );
  const cityName = (id: string) =>
    agents.find((a) => a.city_id === id)?.name || cityLabel(id);
  async function cardAction(card: Card, action: string) {
    await act(
      () =>
        api(`/messages/${card.id}/actions`, {
          method: "POST",
          body: JSON.stringify({ action }),
        }),
      action === "save_to_trip"
        ? "Trip selection updated"
        : action === "request_verification"
          ? "Verification delegated to the relevant agents"
          : "Briefing updated",
    );
  }

  return (
    <div className="shell">
      <aside className="sidebar">
        <a
          className="brand"
          href="#"
          onClick={(e) => {
            e.preventDefault();
            setTab("briefing");
          }}
        >
          <IconMark />
          <div>
            Baltic<span>GUIDE INTELLIGENCE</span>
          </div>
        </a>
        <div className="workspace">
          <div className="workspace-avatar">BG</div>
          <div>
            My guide workspace<small>Three countries. One perspective.</small>
          </div>
        </div>
        <div className="nav-label">WORKSPACE</div>
        <nav>
          {[
            ["briefing", BookOpen, "Your briefing"],
            ["network", Network, "Agent network"],
            ["sources", Radio, "Data sources"],
            ["settings", SlidersHorizontal, "Trip preferences"],
          ].map(([id, Icon, label]) => {
            const I = Icon as typeof BookOpen;
            return (
              <button
                key={id as string}
                className={tab === id ? "nav-item selected" : "nav-item"}
                onClick={() => setTab(id as string)}
              >
                <I size={19} />
                <span>{label as string}</span>
                {id === "briefing" && !!overview?.unread && (
                  <b>{overview.unread}</b>
                )}
              </button>
            );
          })}
        </nav>
        <div className="sidebar-bottom">
          <div className="network-status">
            <span
              className={
                overview?.workers_online ? "status-dot" : "status-dot offline"
              }
            />
            <span>
              {overview?.workers_online
                ? "Network is listening"
                : "Waiting for workers"}
              <small>
                {overview?.workers_online || 0} workers ·{" "}
                {overview?.pending || 0} queued
              </small>
            </span>
          </div>
          <button className="account" onClick={() => setLogin(!login)}>
            <div className="avatar">SG</div>
            <div>
              Tour guide
              <small>
                {overview?.demo_mode ? "Demo workspace" : "Connected workspace"}
              </small>
            </div>
            <Settings2 size={17} />
          </button>
        </div>
      </aside>
      <main>
        <header>
          <div className="breadcrumb">
            Workspace <ChevronRight size={14} />{" "}
            <span>
              {
                {
                  briefing: "Your briefing",
                  network: "Agent network",
                  sources: "Data sources",
                  settings: "Trip preferences",
                }[tab]
              }
            </span>
          </div>
          <div className="header-right">
            <span className="region-tag">
              <Globe2 size={15} /> THE BALTICS
            </span>
            <button aria-label="Refresh dashboard" onClick={refresh}>
              <RefreshCw size={17} />
            </button>
            <button
              aria-label="Show unread briefing"
              onClick={() => {
                setTab("briefing");
                setKind("all");
              }}
            >
              <Bell size={18} />
            </button>
          </div>
        </header>
        {login && (
          <div className="login-panel">
            <ShieldCheck size={22} />
            <div>
              <b>Connect your workspace</b>
              <p>Enter the guide access token issued by your administrator.</p>
            </div>
            <input
              type="password"
              aria-label="Access token"
              value={access}
              onChange={(e) => setAccess(e.target.value)}
              placeholder="Guide access token"
            />
            <button
              className="primary"
              onClick={() => {
                sessionStorage.setItem("baltic-token", access);
                setAccess("");
                window.location.reload();
              }}
            >
              Connect
            </button>
          </div>
        )}
        <div className="content">
          {error && (
            <div className="error-banner" role="alert">
              <TriangleAlert size={18} />
              {error}
              <button onClick={() => setError("")} aria-label="Dismiss error">
                <X size={16} />
              </button>
            </div>
          )}
          <div className="page-title">
            <div>
              <div className="eyebrow">LOCAL KNOWLEDGE, CONNECTED</div>
              <h1>
                {tab === "briefing"
                  ? "A wider perspective."
                  : tab === "network"
                    ? "Thirty cities. Always listening."
                    : tab === "sources"
                      ? "Know where it comes from."
                      : "Make it relevant to your tour."}
              </h1>
              <p>
                {tab === "briefing"
                  ? "The changes, connections, and opportunities that matter to your next tour."
                  : tab === "network"
                    ? "Independent local agents, working together across the Baltics."
                    : tab === "sources"
                      ? "Track source coverage, freshness, and the evidence behind every finding."
                      : "Tell your agents where you’re going and what your group cares about."}
              </p>
            </div>
            {overview?.demo_mode && (
              <button
                className="secondary"
                disabled={busy}
                onClick={() =>
                  act(
                    () => api("/demo/seed", { method: "POST" }),
                    "Illustrative scenario queued. Agents are processing it.",
                  )
                }
              >
                <Plus size={16} /> Load demo scenario
              </button>
            )}
          </div>
          {overview?.demo_mode && (
            <div className="demo-strip">
              <Sparkles size={15} />
              <span>
                Demo workspace · Illustrative events are clearly marked. No API
                key is needed to explore.
              </span>
              <button
                onClick={() =>
                  act(
                    () => api("/demo/cancel", { method: "POST" }),
                    "Cancellation queued. Watch the affected findings update.",
                  )
                }
                disabled={busy}
              >
                Simulate a cancellation <ArrowRight size={14} />
              </button>
            </div>
          )}
          <div className="stats-grid">
            <Stat
              label="CITY AGENTS"
              value={overview?.cities ?? "—"}
              note="Across Estonia, Latvia & Lithuania"
              icon={<MapPin size={18} />}
            />
            <Stat
              label="SOURCE OBSERVATIONS"
              value={overview?.facts ?? "—"}
              note="Deduplicated, with evidence"
              icon={<Layers3 size={18} />}
            />
            <Stat
              label="YOUR UNREAD UPDATES"
              value={overview?.unread ?? "—"}
              note="Matched to your subscriptions"
              icon={<Bell size={18} />}
            />
            <Stat
              label="NETWORK ACTIVITY"
              value={overview?.pending ?? "—"}
              note="Durable tasks in the queue"
              icon={<Activity size={18} />}
            />
          </div>
          {tab === "briefing" && (
            <div className="briefing-grid">
              <section className="feed-section">
                <div className="section-top">
                  <div>
                    <h2>
                      Your live briefing{" "}
                      <span className="live-indicator">LIVE</span>
                    </h2>
                    <p>Evidence first. Useful connections next.</p>
                  </div>
                  <button
                    className={savedOnly ? "mini active" : "mini"}
                    onClick={() => setSavedOnly(!savedOnly)}
                  >
                    <BookOpen size={15} />
                    {savedOnly ? "Saved to trip" : "All updates"}
                  </button>
                </div>
                <div className="filters">
                  <div className="country-tabs">
                    {["all", "ee", "lv", "lt"].map((c) => (
                      <button
                        key={c}
                        onClick={() => setCountry(c)}
                        className={country === c ? "active" : ""}
                      >
                        {c === "all"
                          ? "All countries"
                          : countries[c].flag + " " + countries[c].name}
                      </button>
                    ))}
                  </div>
                  <div className="filter-row">
                    <label className="search">
                      <Search size={16} />
                      <input
                        aria-label="Search briefing"
                        placeholder="Search your briefing…"
                        value={search}
                        onChange={(e) => setSearch(e.target.value)}
                      />
                    </label>
                    <select
                      aria-label="Finding type"
                      value={kind}
                      onChange={(e) => setKind(e.target.value)}
                    >
                      <option value="all">All updates</option>
                      <option value="theme">Cross-city themes</option>
                      <option value="opportunity">Opportunities</option>
                      <option value="disruption">Disruptions</option>
                      <option value="answer">Agent answers</option>
                    </select>
                  </div>
                </div>
                <div className="cards">
                  {filtered.length === 0 ? (
                    <div className="empty">
                      <Compass size={38} />
                      <h3>
                        {cards.length
                          ? "No matching updates"
                          : "Your local knowledge starts here"}
                      </h3>
                      <p>
                        {cards.length
                          ? "Try another country or filter."
                          : "Load the illustrative scenario, or enable real sources to see your agents connect the dots."}
                      </p>
                    </div>
                  ) : (
                    filtered.map((card) => (
                      <article
                        key={card.id}
                        className={`finding-card ${card.payload.type === "theme" ? "theme-card" : ""} ${card.payload.status === "retracted" ? "retracted" : ""}`}
                      >
                        <div className="card-meta">
                          <span className={"type-badge " + card.payload.type}>
                            {card.payload.type === "theme" ? (
                              <Network size={13} />
                            ) : card.payload.type === "disruption" ? (
                              <TriangleAlert size={13} />
                            ) : (
                              <CircleDot size={13} />
                            )}{" "}
                            {card.payload.type.replaceAll("_", " ")}
                          </span>
                          <span>{timeAgo(card.created_at)}</span>
                          {card.payload.illustrative && (
                            <span className="demo-badge">ILLUSTRATIVE</span>
                          )}
                        </div>
                        <h3>{card.payload.title}</h3>
                        <div className="places">
                          {card.payload.city_ids.slice(0, 4).map((cid) => (
                            <span key={cid}>
                              <MapPin size={12} />
                              {cityName(cid)}
                            </span>
                          ))}
                          {card.payload.city_ids.length > 4 && (
                            <span>
                              +{card.payload.city_ids.length - 4} cities
                            </span>
                          )}
                        </div>
                        {card.payload.status === "retracted" && (
                          <div className="retract-banner">
                            Recommendation withdrawn · Check the updated
                            evidence
                          </div>
                        )}
                        <p className="summary">{card.payload.summary}</p>
                        {card.payload.starts_at && (
                          <div className="dates">
                            {dateLabel(card.payload.starts_at)} —{" "}
                            {dateLabel(card.payload.ends_at)}{" "}
                            <span>· Local times in source</span>
                          </div>
                        )}
                        {card.payload.recommended_action && (
                          <div className="insight">
                            <ArrowRight size={15} />
                            <p>{card.payload.recommended_action}</p>
                          </div>
                        )}
                        <div className="card-evidence">
                          <span>
                            <ShieldCheck size={14} />
                            {card.payload.verification.replaceAll("_", " ")}
                          </span>
                          <details>
                            <summary>
                              {card.payload.source_urls.length} source
                              {card.payload.source_urls.length !== 1 ? "s" : ""}
                            </summary>
                            {card.payload.source_urls.map((url) => (
                              <a
                                key={url}
                                href={url}
                                target="_blank"
                                rel="noreferrer"
                              >
                                {new URL(url).hostname}
                                <ExternalLink size={12} />
                              </a>
                            ))}
                          </details>
                          <span>v{card.version}</span>
                        </div>
                        <div className="card-actions">
                          <button
                            onClick={() => cardAction(card, "save_to_trip")}
                          >
                            <BookOpen size={14} />
                            {card.saved ? "Saved to trip" : "Save to trip"}
                          </button>
                          <button
                            onClick={() =>
                              cardAction(card, "request_verification")
                            }
                          >
                            <RefreshCw size={14} />
                            Verify
                          </button>
                          <button
                            onClick={() => cardAction(card, "acknowledge")}
                          >
                            <Check size={14} />
                            {card.status === "acknowledged"
                              ? "Acknowledged"
                              : "Acknowledge"}
                          </button>
                          <button
                            onClick={() => cardAction(card, "dismiss")}
                            aria-label={"Dismiss " + card.payload.title}
                          >
                            <X size={14} />
                          </button>
                        </div>
                      </article>
                    ))
                  )}
                </div>
              </section>
              <aside className="right-column">
                <div className="ask-panel">
                  <div className="coordinator-icon">
                    <Sparkles size={22} />
                  </div>
                  <span className="eyebrow">YOUR BALTIC COORDINATOR</span>
                  <h2>Ask the whole network.</h2>
                  <p>
                    One question. Local evidence from the places that matter to
                    your group.
                  </p>
                  <textarea
                    aria-label="Ask the agents"
                    value={question}
                    onChange={(e) => setQuestion(e.target.value)}
                    placeholder="What changed for my Tallinn–Rīga–Vilnius tour next week?"
                  />
                  <label className="check-label">
                    <input
                      type="checkbox"
                      checked={verify}
                      onChange={(e) => setVerify(e.target.checked)}
                    />{" "}
                    Ask city agents to verify their evidence
                  </label>
                  <button
                    className="primary"
                    onClick={ask}
                    disabled={busy || !question.trim()}
                  >
                    {busy ? (
                      <Loader2 size={16} className="spin" />
                    ) : (
                      <Send size={16} />
                    )}{" "}
                    Ask coordinator
                  </button>
                  <div className="suggested">
                    Try a question
                    <button
                      onClick={() =>
                        setQuestion(
                          "Which cities have events with overlapping dates?",
                        )
                      }
                    >
                      Find a cross-city theme <ArrowRight size={13} />
                    </button>
                    <button
                      onClick={() =>
                        setQuestion("What disruptions should I know about?")
                      }
                    >
                      Check travel disruptions <ArrowRight size={13} />
                    </button>
                  </div>
                </div>
                <div className="coverage-panel">
                  <h3>Listening across the Baltics</h3>
                  {Object.entries(countries).map(([code, c]) => (
                    <button
                      className="country-health"
                      key={code}
                      onClick={() => {
                        setCountry(code);
                        setTab("network");
                      }}
                    >
                      <span>{c.flag}</span>
                      <div>
                        <b>{c.name}</b>
                        <small>
                          {
                            agents.filter(
                              (a) => a.country === code && a.level === "city",
                            ).length
                          }{" "}
                          city agents ·{" "}
                          {
                            sources.filter(
                              (s) =>
                                s.definition.country === code &&
                                s.state === "healthy",
                            ).length
                          }{" "}
                          healthy sources
                        </small>
                      </div>
                      <ChevronRight size={15} />
                    </button>
                  ))}
                  <p className="coverage-note">
                    An idle agent is ready to wake. A quiet feed doesn’t
                    guarantee complete city coverage.
                  </p>
                </div>
                {activeTasks.length > 0 && (
                  <div className="task-panel">
                    <h3>
                      <Loader2 size={16} className="spin" /> Working on your
                      requests
                    </h3>
                    {activeTasks.map((t) => (
                      <div key={t.id}>
                        <p>{t.request.message}</p>
                        <span>{t.state}</span>
                        <button
                          onClick={() =>
                            act(
                              () =>
                                api(`/tasks/${t.id}/cancel`, {
                                  method: "POST",
                                }),
                              "Request canceled",
                            )
                          }
                        >
                          Cancel
                        </button>
                      </div>
                    ))}
                  </div>
                )}
              </aside>
            </div>
          )}
          {tab === "network" && (
            <section className="network-view">
              <div
                className="coordinator-node"
                onClick={() =>
                  setSelected(agents.find((a) => a.level === "baltic") || null)
                }
                role="button"
                tabIndex={0}
                onKeyDown={(e) =>
                  e.key === "Enter" &&
                  setSelected(agents.find((a) => a.level === "baltic") || null)
                }
              >
                <div className="coordinator-icon">
                  <Network size={25} />
                </div>
                <div>
                  <span className="eyebrow">CROSS-COUNTRY INTELLIGENCE</span>
                  <h2>Baltic coordinator</h2>
                  <p>The common thread between 30 local perspectives.</p>
                </div>
                <span className="pill">
                  {agents.find((a) => a.level === "baltic")?.state || "offline"}
                </span>
              </div>
              <div className="tree-line">
                <ArrowDown size={20} />
              </div>
              <div className="country-columns">
                {Object.entries(countries).map(([code, c]) => (
                  <div className="country-column" key={code}>
                    <button
                      className="country-node"
                      onClick={() =>
                        setSelected(
                          agents.find((a) => a.id === "country:" + code) ||
                            null,
                        )
                      }
                    >
                      <span className="flag">{c.flag}</span>
                      <div>
                        <h3>{c.name}</h3>
                        <small>Country intelligence</small>
                      </div>
                      <ChevronRight size={17} />
                    </button>
                    <div className="city-grid">
                      {agents
                        .filter((a) => a.country === code && a.level === "city")
                        .map((a) => (
                          <button
                            key={a.id}
                            aria-label={`${a.name}, ${a.memory.fact_count || 0} observations, ${a.state}`}
                            onClick={() => setSelected(a)}
                            className={"city-node " + a.state}
                          >
                            <span className={"agent-dot " + a.state} />
                            <span>
                              {a.name}
                              <small>
                                {a.memory.fact_count || 0} observations
                              </small>
                            </span>
                            <span className="city-state">{a.state}</span>
                          </button>
                        ))}
                    </div>
                  </div>
                ))}
              </div>
              <div className="network-legend">
                <span>
                  <i className="agent-dot working" />
                  Reasoning
                </span>
                <span>
                  <i className="agent-dot idle" />
                  Idle, listening
                </span>
                <span>
                  <i className="agent-dot queued" />
                  Queued
                </span>
                <span>
                  <i className="agent-dot offline" />
                  Worker offline
                </span>
              </div>
            </section>
          )}
          {tab === "sources" && (
            <section className="sources-view">
              <div className="section-top">
                <div>
                  <h2>Source health & coverage</h2>
                  <p>
                    {overview?.connectors_enabled
                      ? "Scheduled ingestion is enabled."
                      : "Scheduled ingestion is paused. An administrator can poll an enabled source below."}
                  </p>
                </div>
                <span className="pill">
                  {sources.filter((s) => s.state === "healthy").length} healthy
                  / {sources.length} configured
                </span>
              </div>
              <div className="source-table">
                <div className="source-row table-head">
                  <span>SOURCE</span>
                  <span>COUNTRY</span>
                  <span>STATUS</span>
                  <span>LAST SUCCESS</span>
                  <span>ACTION</span>
                </div>
                {sources.map((s) => (
                  <div className="source-row" key={s.id}>
                    <div>
                      <a
                        href={s.definition.url}
                        target="_blank"
                        rel="noreferrer"
                      >
                        <b>{s.definition.name}</b>
                        <ExternalLink size={12} />
                      </a>
                      <small>
                        {s.definition.kind.toUpperCase()} · {s.item_count} items
                      </small>
                      {(s.error || s.definition.reason) && (
                        <p>{s.error || s.definition.reason}</p>
                      )}
                    </div>
                    <span>
                      {countries[s.definition.country]?.flag}{" "}
                      {countries[s.definition.country]?.name}
                    </span>
                    <span className={"source-state " + s.state}>
                      {s.state.replaceAll("_", " ")}
                    </span>
                    <span>{timeAgo(s.last_success)}</span>
                    <button
                      className="mini"
                      disabled={
                        !s.definition.enabled ||
                        busy ||
                        overview?.role !== "admin"
                      }
                      onClick={() =>
                        act(
                          () =>
                            api(`/sources/${s.id}/poll`, { method: "POST" }),
                          "Source refreshed",
                        )
                      }
                    >
                      <RefreshCw size={13} />
                      Poll now
                    </button>
                  </div>
                ))}
              </div>
              <div className="source-footnote">
                <ShieldCheck size={19} />
                <p>
                  Disabled integrations need publisher access or parser
                  validation. Source claims retain their URLs and original
                  evidence. News mentions alone do not establish an event’s
                  location.
                </p>
              </div>
            </section>
          )}
          {tab === "settings" && prefs && (
            <PreferencesEditor
              value={prefs}
              agents={agents}
              onSave={(p) =>
                act(
                  () =>
                    api("/preferences", {
                      method: "PUT",
                      body: JSON.stringify(p),
                    }),
                  "Trip preferences saved",
                )
              }
              busy={busy}
            />
          )}
          <footer>
            <span>
              <IconMark /> Baltic Guide
            </span>
            <p>Local evidence. Shared perspective.</p>
            <a href="/docs" target="_blank">
              API documentation <ExternalLink size={12} />
            </a>
          </footer>
        </div>
      </main>
      {notice && (
        <div className="toast" role="status">
          <CheckCheck size={18} />
          {notice}
        </div>
      )}
      {selected && (
        <div className="drawer-backdrop" onClick={() => setSelected(null)}>
          <aside className="drawer" onClick={(e) => e.stopPropagation()}>
            <button
              className="drawer-close"
              onClick={() => setSelected(null)}
              aria-label="Close agent details"
            >
              <X />
            </button>
            <div className="coordinator-icon">
              <MapPin size={25} />
            </div>
            <span className="eyebrow">
              {selected.level.toUpperCase()} AGENT
            </span>
            <h2>{selected.name}</h2>
            <span className="pill">{selected.state}</span>
            <div className="drawer-stats">
              <Stat
                label="OBSERVATIONS"
                value={selected.memory.fact_count || 0}
                note="Persistent factual memory"
                icon={<Layers3 size={16} />}
              />
              <Stat
                label="COMPLETED RUNS"
                value={selected.runs}
                note={timeAgo(selected.last_run)}
                icon={<Activity size={16} />}
              />
            </div>
            {selected.memory.assessment && (
              <>
                <h3>Agent assessment</h3>
                <p className="memory-item">{selected.memory.assessment}</p>
                <p className="muted">
                  Model interpretation of stored evidence.
                </p>
              </>
            )}
            {selected.memory.model_error && (
              <p className="muted">{selected.memory.model_error}</p>
            )}
            <h3>Recent observations</h3>
            {selected.memory.recent_titles?.length ? (
              selected.memory.recent_titles.map((t) => (
                <p className="memory-item" key={t}>
                  {t}
                </p>
              ))
            ) : (
              <p className="muted">
                No observations yet. This agent will wake when relevant data
                arrives.
              </p>
            )}
            <h3>Ask this agent</h3>
            <textarea
              aria-label="Ask selected agent"
              value={question}
              onChange={(e) => setQuestion(e.target.value)}
              placeholder={`What’s happening in ${selected.name}?`}
            />
            <button
              className="primary"
              disabled={busy || !question.trim()}
              onClick={ask}
            >
              <Send size={15} />
              Send request
            </button>
            <a
              className="agent-card-link"
              href={`/a2a/${selected.id}/.well-known/agent-card.json`}
              target="_blank"
            >
              A2A Agent Card <ExternalLink size={13} />
            </a>
            {selected.last_error && (
              <div className="error-banner">{selected.last_error}</div>
            )}
          </aside>
        </div>
      )}
    </div>
  );
}
function Stat({
  label,
  value,
  note,
  icon,
}: {
  label: string;
  value: string | number;
  note: string;
  icon: React.ReactNode;
}) {
  return (
    <div className="stat">
      <div>
        <span>{label}</span>
        {icon}
      </div>
      <strong>{value}</strong>
      <p>{note}</p>
    </div>
  );
}
function PreferencesEditor({
  value,
  agents,
  onSave,
  busy,
}: {
  value: Preferences;
  agents: Agent[];
  onSave: (p: Preferences) => void;
  busy: boolean;
}) {
  const [p, setP] = useState(value);
  const toggle = (key: "city_ids" | "countries" | "interests", id: string) =>
    setP({
      ...p,
      [key]: p[key].includes(id)
        ? p[key].filter((x) => x !== id)
        : [...p[key], id],
    });
  return (
    <section className="preferences">
      <div className="preference-intro">
        <SlidersHorizontal size={25} />
        <h2>Your next tour, in focus.</h2>
        <p>
          Subscriptions apply to new findings. Corrections to messages you
          already received will still reach you.
        </p>
      </div>
      <div className="preference-form">
        <h3>Countries & cities</h3>
        <p className="muted">
          Leave all unselected to follow the full network. Selected countries
          include all their cities.
        </p>
        {Object.entries(countries).map(([code, c]) => (
          <div className="preference-country" key={code}>
            <label>
              <input
                type="checkbox"
                checked={p.countries.includes(code)}
                onChange={() => toggle("countries", code)}
              />
              {c.flag} {c.name}
            </label>
            <div>
              {agents
                .filter((a) => a.country === code && a.city_id)
                .map((a) => (
                  <button
                    key={a.id}
                    className={
                      p.city_ids.includes(a.city_id!) ? "chip active" : "chip"
                    }
                    onClick={() => toggle("city_ids", a.city_id!)}
                  >
                    {a.name}
                  </button>
                ))}
            </div>
          </div>
        ))}
        <h3>Travel dates</h3>
        <div className="date-inputs">
          <label>
            From
            <input
              type="date"
              value={p.starts_at?.slice(0, 10) || ""}
              onChange={(e) =>
                setP({
                  ...p,
                  starts_at: e.target.value
                    ? e.target.value + "T00:00:00Z"
                    : null,
                })
              }
            />
          </label>
          <label>
            Through
            <input
              type="date"
              value={p.ends_at?.slice(0, 10) || ""}
              onChange={(e) =>
                setP({
                  ...p,
                  ends_at: e.target.value
                    ? e.target.value + "T23:59:59Z"
                    : null,
                })
              }
            />
          </label>
        </div>
        <h3>Group interests</h3>
        <div className="interest-chips">
          {[
            "christmas",
            "music",
            "festival",
            "food",
            "art",
            "outdoors",
            "transport",
            "sport",
          ].map((t) => (
            <button
              className={p.interests.includes(t) ? "chip active" : "chip"}
              key={t}
              onClick={() => toggle("interests", t)}
            >
              {t}
            </button>
          ))}
        </div>
        <p className="muted">
          Disruptions bypass interest filters so relevant cancellations aren’t
          missed.
        </p>
        <label className="language-label">
          Briefing language
          <select
            value={p.language}
            onChange={(e) => setP({ ...p, language: e.target.value })}
          >
            <option value="en">English</option>
            <option value="et">Estonian</option>
            <option value="lv">Latvian</option>
            <option value="lt">Lithuanian</option>
            <option value="de">German</option>
          </select>
        </label>
        <p className="muted">
          Generated translations require a configured language model; source
          text is preserved.
        </p>
        <button className="primary" disabled={busy} onClick={() => onSave(p)}>
          <Check size={16} />
          Save preferences
        </button>
      </div>
    </section>
  );
}
