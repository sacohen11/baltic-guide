export type Briefing = {
  id?: string;
  version?: number;
  type: string;
  title: string;
  summary: string;
  status: string;
  case_id?: string;
  families: string[];
  instruments: string[];
  uncertainty: string;
  source_urls: string[];
  evidence_ids: string[];
  interpretation?: string;
  model_error?: string;
  as_of?: string;
  question?: string;
  reports: {
    entity_id: string;
    version: number;
    title: string;
    status: string;
    kind: string;
    source_url: string;
  }[];
};
export type Agent = {
  id: string;
  name: string;
  level: string;
  family: string | null;
  parent: string | null;
  topic: string;
  state: string;
  pending: number;
  runs: number;
  last_run: number;
  last_heartbeat: number;
  last_error: string | null;
  memory: {
    assessment?: string;
    model_error?: string;
    fact_count?: number;
    recent_titles?: string[];
  };
};
export type Case = {
  id: string;
  name: string;
  agent_id: string;
  updated_at: number;
  briefing: Briefing | null;
};
export type Evidence = {
  id: string;
  version?: number;
  created_at?: number;
  payload: {
    title: string;
    summary: string;
    raw_id: string;
    status: string;
    kind: string;
    notice_time: string;
    source_url: string;
    metadata: Record<string, unknown>;
    references: Record<string, unknown>[];
  };
};
export type CaseDetail = Case & {
  reports: Evidence[];
  history: Evidence[];
  agent: { memory: Agent["memory"] };
};
export type Card = {
  seq: number;
  id: string;
  status: string;
  saved: boolean;
  created_at: number;
  payload: Briefing;
};
export type Source = {
  id: string;
  definition: { agent_id: string };
  state: string;
  item_count: number;
  last_success: number;
  error: string | null;
};
export type Overview = {
  agents: number;
  cases: number;
  reports: number;
  pending: number;
  unread: number;
  workers_online: number;
  quarantined: number;
  outbox_pending: number;
  transport: string;
  model: string | null;
  role: string;
  gcn: {
    state: string;
    error?: string;
    heartbeat?: number;
    lag?: Record<string, number>;
  };
};
export type Task = {
  id: string;
  agent_id: string;
  state: string;
  request: { message: string };
  result: Briefing | null;
  error: string | null;
};
export type Preferences = {
  families: string[];
  instruments: string[];
  multi_messenger_only: boolean;
};

export function defaultBackend() {
  return (
    localStorage.getItem("observatory-backend") ||
    import.meta.env.VITE_API_BASE_URL ||
    (location.hostname.endsWith("github.io") ? "" : location.origin)
  );
}
export function validateBackend(value: string) {
  const url = new URL(value);
  if (
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    (url.pathname !== "/" && url.pathname !== "")
  )
    throw new Error(
      "Use the backend origin, such as https://observatory.example.com",
    );
  if (
    url.protocol !== "https:" &&
    !(
      url.protocol === "http:" &&
      ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname)
    )
  )
    throw new Error(
      "Use HTTPS for a remote backend. HTTP is allowed for localhost.",
    );
  return url.origin;
}
export function safeUrl(value: string) {
  try {
    const u = new URL(value);
    return ["http:", "https:"].includes(u.protocol) &&
      !u.username &&
      !u.password
      ? u.href
      : "#";
  } catch {
    return "#";
  }
}
export async function api<T>(
  base: string,
  token: string,
  path: string,
  options: RequestInit = {},
): Promise<T> {
  const response = await fetch(base + "/api" + path, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
      ...options.headers,
    },
  });
  if (!response.ok) {
    let detail = `Request failed (${response.status})`;
    try {
      const body = await response.json();
      detail =
        typeof body.detail === "string"
          ? body.detail
          : JSON.stringify(body.detail);
    } catch {
      /* Non-JSON proxy error */
    }
    throw new Error(detail);
  }
  return response.json();
}
export async function watch(
  base: string,
  token: string,
  onMessage: () => void,
  signal: AbortSignal,
) {
  let cursor = 0;
  while (!signal.aborted) {
    try {
      const r = await fetch(`${base}/api/stream?after=${cursor}`, {
        headers: { Authorization: `Bearer ${token}` },
        signal,
      });
      if (!r.ok || !r.body) throw new Error("Stream disconnected");
      const reader = r.body.getReader(),
        decoder = new TextDecoder();
      let buffer = "";
      try {
        while (!signal.aborted) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += decoder.decode(value, { stream: true });
          let end: number;
          while ((end = buffer.indexOf("\n\n")) >= 0) {
            const block = buffer.slice(0, end);
            buffer = buffer.slice(end + 2);
            const data = block
              .split("\n")
              .find((line) => line.startsWith("data: "));
            if (data) {
              const card = JSON.parse(data.slice(6)) as Card;
              cursor = card.seq;
              onMessage();
            }
          }
        }
      } finally {
        await reader.cancel().catch(() => {});
      }
    } catch {
      if (signal.aborted) return;
    }
    await new Promise<void>((resolve) => {
      const done = () => {
        clearTimeout(id);
        signal.removeEventListener("abort", done);
        resolve();
      };
      const id = setTimeout(done, 2000);
      signal.addEventListener("abort", done, { once: true });
    });
  }
}
