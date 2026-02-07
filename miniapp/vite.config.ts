import react from "@vitejs/plugin-react-swc";
import { defineConfig } from "vite";
import tsconfigPaths from "vite-tsconfig-paths";

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => ({
  base: process.env.VITE_BASE || "/",
  plugins: [react(), tsconfigPaths()],
  server: {
    host: true,
    strictPort: true,
  },
  build: {
    outDir: "dist",
    sourcemap: mode === "production" ? false : "inline",
    rollupOptions: {
      output: {
        manualChunks: {
          "vendor-telegram": [
            "@telegram-apps/telegram-ui",
            "@tma.js/sdk-react",
          ],
          "vendor-tonconnect": ["@tonconnect/ui-react"],
          "vendor-core": ["react", "react-dom", "react-router-dom"],
        },
      },
    },
    minify: "terser",
    terserOptions: {
      compress: {
        drop_console: mode === "production",
        drop_debugger: true,
        pure_funcs:
          mode === "production" ? ["console.log", "console.debug"] : [],
      },
      mangle: true,
      format: {
        comments: false,
      },
    },
    // Use HTTPS by default for Telegram Mini App compatibility
    https: Boolean(process.env.HTTPS),
  },
  define: {
    __DEV__: JSON.stringify(mode !== "production"),
  },
}));
