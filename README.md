# Space Observatory

A live multi-messenger observatory built on NASA GCN's **external Kafka streams**. It replaces the Baltic tourist-guide application in this repository. There is no demo feed, seed button, simulated live mode, or RSS-to-Kafka substitute.

The application contains a React/TypeScript dashboard, FastAPI backend, PostgreSQL persistence, internal Kafka routing, a native external GCN consumer, durable LangGraph workers, and A2A 1.0 interfaces.

## What it does

- Seven instrument agents: Fermi GBM/LAT, Swift BAT/XRT/UVOT, LVK, and IceCube.
- Three signal-family coordinators: electromagnetic, gravitational waves, and neutrinos.
- One whole-sky coordinator and one shared Circulars agent: **12 permanent agents**.
- Persistent case agents created from publisher event identifiers. They link explicit references across instruments, retain revisions, and track retractions.
- Live briefing cards, agent questions and delegated checks, subscriptions, source health, original payload downloads, and authenticated observer accounts.
- Optional model analysis using a configured OpenAI-compatible chat-completions endpoint. Without a model, ingestion and source-backed structured briefings work, but free-text questions return an evidence dossier rather than inferred answers. The UI labels this clearly.

## Run the real application on your computer

Install Docker Desktop, then:

```sh
git clone https://github.com/sacohen11/baltic-guide.git
cd baltic-guide
cp .env.example .env
```

1. Sign in at [NASA GCN](https://gcn.nasa.gov/quickstart), create Kafka client credentials, and set `GCN_CLIENT_ID` and `GCN_CLIENT_SECRET` in `.env`.
2. Run `openssl rand -hex 32` **twice**. Use the results for `ADMIN_TOKEN` and `POSTGRES_PASSWORD`, respectively.
3. Set `LLM_MODEL`, `LLM_API_KEY`, and, if needed, `LLM_BASE_URL` to enable agent reasoning. Credentials stay in the backend environment. Never put these values in frontend build variables.
4. Start the services:

```sh
docker compose up --build -d
docker compose ps
docker compose logs -f ingestor worker
```

Open **http://localhost:8000**, use that URL as the backend URL, and sign in with your `ADMIN_TOKEN`. The dashboard starts empty. Incoming real GCN records populate it. Some scientific streams are quiet for long periods: a healthy connection does not imply frequent alerts.

`GCN_AUTO_OFFSET_RESET=latest` starts a new consumer group at the live edge. To read available historical records, use `earliest` and a **new** `GCN_GROUP_ID`. Existing groups resume committed offsets regardless of this setting. Kafka retention limits historical availability. Replayed historical records keep their original timestamps.

The default subscribes to all topics in `backend/observatory/registry.py`. If GCN reports a topic unavailable, check your credentials and current topic access. Set `GCN_TOPICS` to an explicit comma-separated supported subset if needed; the dashboard marks the other subscriptions as inactive. The consumer does not silently pretend missing topics are connected.

## Host the backend permanently

GitHub Pages hosts static frontend files. It cannot run Python, PostgreSQL, Kafka consumers, or agent workers. The backend must run on an always-on computer or server with Docker, outbound access to GCN Kafka on port 9092, and HTTPS access to GCN OAuth and the configured model endpoint.

### Mac mini + Cloudflare Tunnel

This is the recommended single-machine setup for a Mac mini. Install Docker Desktop and enable **Start Docker Desktop when you sign in**. In macOS **System Settings → Energy**, turn on **Prevent automatic sleeping when the display is off** and **Start up automatically after a power failure**. After a full reboot, Docker Desktop needs a user session to start; test that the services actually come back before relying on this unattended.

1. Follow the [local setup](#run-the-real-application-on-your-computer) through creating `.env` and setting NASA GCN credentials, `ADMIN_TOKEN`, and `POSTGRES_PASSWORD`. Leave the secrets only in the Mac's `.env` file.
2. Add a domain you control to Cloudflare. In **Cloudflare Zero Trust → Networking → Tunnels**, create a **remotely managed** Cloudflare Tunnel with a Docker connector. Copy its tunnel token into `CLOUDFLARE_TUNNEL_TOKEN` in `.env`. Do not use a temporary Quick Tunnel: this dashboard uses a streaming connection and needs a stable hostname.
3. Give the tunnel a public hostname, such as `api.yourdomain.com`, with service URL **`http://api:8000`**. The `tunnel` and `api` containers share a Docker Compose network; `localhost` inside the tunnel container would point to the tunnel itself. The tunnel makes an outbound connection, so no router port forwarding or public home IP is needed.
4. In `.env`, set `PUBLIC_URL=https://api.yourdomain.com` and `CORS_ORIGINS=https://sacohen11.github.io`. If the dashboard will also run on a custom domain, add its full HTTPS origin separated by a comma. Do not append `/baltic-guide/` to an origin.
5. Start all services from the repository directory on the Mac:

```sh
docker compose --profile tunnel up --build -d
docker compose --profile tunnel ps
docker compose --profile tunnel logs --tail=50 tunnel ingestor worker
curl -fsS https://api.yourdomain.com/healthz
```

The last command should return `{"status":"ok"}`. Check `/readyz` when GCN has connected; it returns 503 until a worker and the external GCN subscription are both healthy. If the tunnel reports a 502 error, confirm the Cloudflare service URL is `http://api:8000` and inspect `docker compose logs api tunnel`. Kafka and PostgreSQL are kept on the private Compose network. The API's direct host port is bound to the Mac's loopback address for local troubleshooting.

In the GitHub repository, enable **Settings → Pages → Build and deployment → GitHub Actions**. Under **Settings → Secrets and variables → Actions → Variables**, set `OBSERVATORY_API_URL` to your public HTTPS API origin (`https://api.yourdomain.com`), or leave it unset and enter the backend URL on the dashboard sign-in screen. Run **Deploy observatory frontend to GitHub Pages** from Actions. Open the URL reported by that deployment and enter the observer or admin token from your Mac's `.env`. The browser sends requests directly from the Pages site to your tunnel hostname; GitHub does not receive the backend secret. The Pages site itself is public, including for a private repository if your GitHub plan permits Pages.

Mac downtime, home internet outages, and a stopped Docker Desktop interrupt live ingestion. Persistent Docker volumes hold PostgreSQL and Kafka data across restarts, but back up the database to a separate location; see [operations](docs/operations.md). Once connectivity returns, the GCN consumer resumes its committed offsets while Kafka retains them.

### Server with inbound HTTPS

On that host, follow the preceding setup, then point a domain's DNS at the server. In `.env` set:

```dotenv
OBSERVATORY_DOMAIN=observatory.example.com
PUBLIC_URL=https://observatory.example.com
CORS_ORIGINS=https://sacohen11.github.io,https://observatory.example.com
```

Replace the example domain with your real domain. Open inbound ports 80 and 443 and start Caddy's HTTPS profile:

```sh
docker compose --profile https up --build -d
```

Caddy obtains and renews a TLS certificate. Database and Kafka ports remain private; the API's direct host port is bound to loopback. This is a single-host deployment with persistent volumes, not a highly available cluster. Back up PostgreSQL and monitor disk usage before sustained operation. See [operations](docs/operations.md).

## Publish the frontend to GitHub Pages

The repository includes a Pages deployment workflow. It runs after **Test and build** succeeds on `main`, and can also be run manually.

1. In repository **Settings → Pages**, select **GitHub Actions** as the source. A private repository must have a GitHub plan that supports Pages for private repositories. Publishing the frontend does not change repository visibility.
2. Optionally set repository Actions variable `OBSERVATORY_API_URL` to your HTTPS backend origin. This is a public URL, **not a secret**. If unset, enter the backend URL on the sign-in screen.
3. Run **Deploy observatory frontend to GitHub Pages** under Actions, or push a tested change to `main`.
4. Use the exact site URL reported by the deployment job. The expected project URL is `https://sacohen11.github.io/baltic-guide/` once Pages is enabled and deployment succeeds.

GCN/model credentials and access tokens must never be repository variables or baked into the Pages bundle. Users enter their own observer token at runtime. The frontend keeps it in session storage. To create a restricted observer token, POST a name to `/api/observers` using the admin token; the response shows the new token once.

If `configure-pages` reports a missing site, enable Pages in Settings first. The standard Actions token cannot enable a new Pages site because that needs repository administration permissions.

## Development and tests

```sh
uv sync --frozen --extra dev
cd frontend
npm ci
npm test
npm run build
cd ..
uv run ruff check backend scripts
uv run pytest -q
```

Tests use explicit isolated fixtures. They are never loaded by the application. The Kafka/PostgreSQL integration test runs in CI using disposable service containers; locally it runs only when `TEST_DATABASE_URL` and `TEST_KAFKA_BOOTSTRAP` are set. Tests cover normalization, source schema variations, duplicate delivery, revision ordering, retractions, test-event isolation, leased job recovery, authentication, A2A, and browser connection behavior.

`TRANSPORT=local` is an internal database-backed transport for development/tests; it does not provide synthetic data. The deployment uses internal Kafka. The external ingestor always consumes real Kafka.

## Read more

- [Architecture and scientific boundaries](docs/architecture.md)
- [External topics and parser references](docs/sources.md)
- [API and A2A](docs/api.md)
- [Operations, backups, and recovery](docs/operations.md)

The application is independent software consuming NASA GCN. Participating observatories include non-NASA facilities. Source-provided classifications, probabilities, and search results remain source claims; an LLM cannot turn overlapping reports into a confirmed discovery.
