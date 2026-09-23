export type Agent = {
  id: string;
  name: string;
  level: string;
  country: string | null;
  city_id: string | null;
  parent: string | null;
  state: string;
  pending: number;
  runs: number;
  topic: string;
  memory: {
    assessment?: string;
    model_error?: string;
    fact_count?: number;
    active_count?: number;
    recent_titles?: string[];
  };
  last_run: number;
  last_error: string | null;
};
export type Payload = {
  id: string;
  version: number;
  agent_id: string;
  type: string;
  title: string;
  summary: string;
  city_ids: string[];
  countries: string[];
  tags: string[];
  status: string;
  verification: string;
  source_urls: string[];
  evidence_ids: string[];
  illustrative: boolean;
  priority?: string;
  starts_at?: string;
  ends_at?: string;
  recommended_action?: string;
  as_of: string;
  model_error?: string;
  coverage_note?: string;
};
export type Card = {
  seq: number;
  id: string;
  guide_id: string;
  finding_id: string;
  version: number;
  payload: Payload;
  status: string;
  saved: boolean;
  created_at: number;
};
export type Source = {
  id: string;
  definition: {
    name: string;
    country: string;
    kind: string;
    url: string;
    enabled: boolean;
    reason?: string;
    interval_seconds: number;
  };
  state: string;
  last_success: number;
  error: string | null;
  item_count: number;
};
export type Overview = {
  agents: number;
  cities: number;
  countries: number;
  demo_mode: boolean;
  transport: string;
  model: string;
  facts: number;
  findings: number;
  unread: number;
  pending: number;
  workers_online: number;
  connectors_enabled: boolean;
  role: string;
  guide_id: string;
};
export type Task = {
  id: string;
  agent_id: string;
  state: string;
  request: { message: string };
  result: Payload | null;
  error: string | null;
  created_at: number;
};
export type Preferences = {
  city_ids: string[];
  countries: string[];
  interests: string[];
  muted_tags: string[];
  starts_at: string | null;
  ends_at: string | null;
  language: string;
};
export function token() {
  return sessionStorage.getItem("baltic-token") || "";
}
export async function api<T>(
  path: string,
  options: RequestInit = {},
): Promise<T> {
  const t = token();
  const response = await fetch("/api" + path, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(t ? { Authorization: `Bearer ${t}` } : {}),
      ...options.headers,
    },
  });
  if (!response.ok) {
    let detail = `Request failed (${response.status})`;
    try {
      const j = await response.json();
      detail =
        typeof j.detail === "string" ? j.detail : JSON.stringify(j.detail);
    } catch {}
    throw new Error(detail);
  }
  return response.json();
}
export async function watchMessages(
  onMessage: (card: Card) => void,
  signal: AbortSignal,
) {
  let cursor = 0;
  while (!signal.aborted) {
    try {
      const t = token();
      const r = await fetch(`/api/stream?after=${cursor}`, {
        headers: t ? { Authorization: `Bearer ${t}` } : {},
        signal,
      });
      if (!r.ok || !r.body) throw new Error("Stream disconnected");
      const reader = r.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";
      while (!signal.aborted) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        let index;
        while ((index = buffer.indexOf("\n\n")) >= 0) {
          const block = buffer.slice(0, index);
          buffer = buffer.slice(index + 2);
          const data = block.split("\n").find((l) => l.startsWith("data: "));
          if (data) {
            const card = JSON.parse(data.slice(6));
            cursor = card.seq;
            onMessage(card);
          }
        }
      }
    } catch (e) {
      if (signal.aborted) return;
    }
    await new Promise((r) => setTimeout(r, 2000));
  }
}
