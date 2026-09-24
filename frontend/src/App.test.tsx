// @vitest-environment jsdom
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import App from "./App";
import { safeUrl, validateBackend } from "./api";

const overview = {
  agents: 12,
  cases: 0,
  reports: 0,
  pending: 0,
  unread: 0,
  workers_online: 0,
  quarantined: 0,
  outbox_pending: 0,
  transport: "kafka",
  model: null,
  role: "admin",
  gcn: {
    state: "not_running",
    error: "GCN ingestor has not reported a heartbeat",
  },
};
beforeEach(() => {
  sessionStorage.clear();
  localStorage.clear();
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string) => {
      const path = String(url).split("/api")[1];
      if (path?.startsWith("/stream")) return new Response("", { status: 503 });
      const body =
        path === "/overview"
          ? overview
          : path === "/preferences"
            ? { families: [], instruments: [], multi_messenger_only: false }
            : [];
      return new Response(JSON.stringify(body), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }),
  );
});
afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe("live observatory", () => {
  it("requires an explicit backend and access token, without example events", () => {
    render(<App />);
    expect(screen.getByText("Connect to your observatory")).toBeTruthy();
    expect(screen.queryByText("S260923abc")).toBeNull();
    expect(screen.getByLabelText("Access token").getAttribute("type")).toBe(
      "password",
    );
  });
  it("shows real disconnected state and empty feed after authentication", async () => {
    render(<App />);
    fireEvent.change(screen.getByLabelText("Backend URL"), {
      target: { value: "https://observatory.example.com" },
    });
    fireEvent.change(screen.getByLabelText("Access token"), {
      target: { value: "private-session-token" },
    });
    fireEvent.click(screen.getByRole("button", { name: /Open observatory/ }));
    await waitFor(() =>
      expect(screen.getByText("Listening for the next story")).toBeTruthy(),
    );
    expect(screen.getByText("Live ingestion is not connected")).toBeTruthy();
    expect(localStorage.getItem("observatory-token")).toBeNull();
    expect(sessionStorage.getItem("observatory-token")).toBe(
      "private-session-token",
    );
    const calls = vi.mocked(fetch).mock.calls;
    expect(
      calls.some(([url]) =>
        String(url).startsWith("https://observatory.example.com/api/overview"),
      ),
    ).toBe(true);
    fireEvent.click(screen.getByRole("button", { name: /Disconnect/ }));
    expect(sessionStorage.getItem("observatory-token")).toBeNull();
  });
  it("refuses unsafe backend and source links", () => {
    expect(() => validateBackend("http://remote.example.com")).toThrow();
    expect(() => validateBackend("https://user:secret@example.com")).toThrow();
    expect(() => validateBackend("https://example.com/api")).toThrow();
    expect(validateBackend("http://localhost:8000")).toBe(
      "http://localhost:8000",
    );
    expect(safeUrl("javascript:alert(1)")).toBe("#");
  });
  it("shows authentication errors instead of a fake connected dashboard", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(
        async () =>
          new Response(JSON.stringify({ detail: "Invalid access token" }), {
            status: 401,
          }),
      ),
    );
    render(<App />);
    fireEvent.change(screen.getByLabelText("Backend URL"), {
      target: { value: "https://observatory.example.com" },
    });
    fireEvent.change(screen.getByLabelText("Access token"), {
      target: { value: "bad" },
    });
    fireEvent.click(screen.getByRole("button", { name: /Open observatory/ }));
    await waitFor(() =>
      expect(screen.getByRole("alert").textContent).toContain(
        "Invalid access token",
      ),
    );
    expect(screen.queryByText("Your view of the sky.")).toBeNull();
  });
});
