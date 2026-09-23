// @vitest-environment jsdom
import React from "react";
import { beforeEach, afterEach, describe, expect, it, vi } from "vitest";
import {
  cleanup,
  render,
  screen,
  fireEvent,
  waitFor,
} from "@testing-library/react";
import App from "./App";
import { api } from "./api";

vi.mock("./api", () => ({
  api: vi.fn(),
  watchMessages: vi.fn(() => new Promise(() => {})),
}));
const mocked = vi.mocked(api);
const preferences = {
  city_ids: [],
  countries: [],
  interests: [],
  muted_tags: [],
  starts_at: null,
  ends_at: null,
  language: "en",
};
const card = {
  seq: 1,
  id: "n1",
  finding_id: "f1",
  guide_id: "demo",
  version: 1,
  status: "unread",
  saved: false,
  created_at: Date.now() / 1000,
  payload: {
    id: "f1",
    version: 1,
    agent_id: "city:lv:riga",
    type: "opportunity",
    title: "Rīga winter market",
    summary: "Illustrative local event",
    city_ids: ["lv:riga"],
    countries: ["lv"],
    tags: ["christmas"],
    status: "active",
    verification: "needs_verification",
    source_urls: ["https://example.org/event"],
    evidence_ids: ["e1"],
    illustrative: true,
    as_of: new Date().toISOString(),
  },
};
const agents = [
  {
    id: "city:lv:riga",
    name: "Rīga",
    country: "lv",
    city_id: "lv:riga",
    parent: "country:lv",
    level: "city",
    state: "idle",
    pending: 0,
    runs: 1,
    memory: { fact_count: 1, recent_titles: ["Rīga winter market"] },
    last_run: Date.now() / 1000,
  },
  {
    id: "baltic:coordinator",
    name: "Baltic coordinator",
    level: "baltic",
    country: null,
    city_id: null,
    state: "idle",
    memory: {},
    runs: 1,
    last_run: 0,
  },
];
beforeEach(() => {
  mocked.mockReset();
  mocked.mockImplementation(async (path) => {
    if (path === "/overview")
      return {
        agents: 34,
        cities: 30,
        countries: 3,
        demo_mode: true,
        transport: "local",
        model: "Evidence rules",
        facts: 1,
        findings: 1,
        unread: 1,
        pending: 0,
        workers_online: 1,
        role: "admin",
      };
    if (path === "/agents") return agents;
    if (path.startsWith("/messages?")) return [card];
    if (path === "/sources") return [];
    if (path === "/tasks") return [];
    if (path === "/preferences") return preferences;
    return { status: "ok" };
  });
});
afterEach(() => cleanup());
describe("Guide dashboard", () => {
  it("renders evidence-backed cards and demo labeling", async () => {
    render(<App />);
    await screen.findByText("Rīga winter market");
    expect(screen.getByText("ILLUSTRATIVE")).toBeTruthy();
    expect(screen.getByText("needs verification")).toBeTruthy();
  });
  it("filters by country without inventing updates", async () => {
    render(<App />);
    await screen.findByText("Rīga winter market");
    fireEvent.click(screen.getByRole("button", { name: "🇪🇪 Estonia" }));
    expect(screen.queryByText("Rīga winter market")).toBeNull();
    expect(screen.getByText("No matching updates")).toBeTruthy();
  });
  it("sends a guide request with verification enabled", async () => {
    render(<App />);
    await screen.findByText("Rīga winter market");
    fireEvent.change(screen.getByLabelText("Ask the agents"), {
      target: { value: "What changed?" },
    });
    fireEvent.click(screen.getByRole("checkbox"));
    fireEvent.click(screen.getByRole("button", { name: "Ask coordinator" }));
    await waitFor(() =>
      expect(
        mocked.mock.calls.some(
          ([p, o]) => p === "/query" && JSON.parse(o!.body as string).verify,
        ),
      ).toBe(true),
    );
  });
  it("persists save-to-trip action", async () => {
    render(<App />);
    await screen.findByText("Rīga winter market");
    fireEvent.click(screen.getByRole("button", { name: "Save to trip" }));
    await waitFor(() =>
      expect(
        mocked.mock.calls.some(
          ([p, o]) =>
            p === "/messages/n1/actions" &&
            JSON.parse(o!.body as string).action === "save_to_trip",
        ),
      ).toBe(true),
    );
  });
  it("opens a city agent and its persistent memory", async () => {
    render(<App />);
    await screen.findByText("Rīga winter market");
    fireEvent.click(screen.getByRole("button", { name: "Agent network" }));
    fireEvent.click(
      screen.getByRole("button", { name: /Rīga, 1 observations/ }),
    );
    expect(screen.getByRole("heading", { name: "Rīga" })).toBeTruthy();
    expect(screen.getByText("Recent observations")).toBeTruthy();
  });
  it("saves changed trip preferences", async () => {
    render(<App />);
    await screen.findByText("Rīga winter market");
    fireEvent.click(screen.getByRole("button", { name: "Trip preferences" }));
    fireEvent.click(screen.getByRole("checkbox", { name: "🇱🇻 Latvia" }));
    fireEvent.click(screen.getByRole("button", { name: "Save preferences" }));
    await waitFor(() =>
      expect(
        mocked.mock.calls.some(
          ([p, o]) =>
            p === "/preferences" &&
            o?.method === "PUT" &&
            JSON.parse(o.body as string).countries.includes("lv"),
        ),
      ).toBe(true),
    );
  });
});
