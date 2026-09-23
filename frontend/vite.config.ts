import { defineConfig } from "vite";
export default defineConfig({
  server: {
    proxy: { "/api": "http://127.0.0.1:8000", "/a2a": "http://127.0.0.1:8000" },
  },
});
