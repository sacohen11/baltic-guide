FROM node:24-slim AS frontend
WORKDIR /build
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY pyproject.toml requirements.lock ./
RUN pip install --no-cache-dir -r requirements.lock
COPY backend/ ./backend/
RUN pip install --no-cache-dir --no-deps .
COPY config/ ./config/
COPY scripts/ ./scripts/
COPY --from=frontend /build/dist/ ./frontend/dist/
RUN useradd --uid 10001 --create-home baltic && mkdir -p /app/data && chown -R baltic:baltic /app
USER baltic
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/healthz',timeout=3)" || exit 1
CMD ["baltic", "serve", "--host", "0.0.0.0"]
