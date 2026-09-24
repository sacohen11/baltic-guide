import { defineConfig, loadEnv } from "vite";
export default defineConfig(({ mode }) => ({
  base: loadEnv(mode, ".", "VITE_").VITE_BASE_PATH || "/",
  server: {
    proxy: { "/api": "http://127.0.0.1:8000", "/a2a": "http://127.0.0.1:8000" },
  },
}));
