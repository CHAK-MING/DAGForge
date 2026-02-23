import { defineConfig } from "vite";
import react from "@vitejs/plugin-react-swc";
import path from "path";
import { componentTagger } from "lovable-tagger";

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => ({
    server: {
        // VS Code port forwarding: browser hits http://localhost:8080
        // while this dev server runs remotely.
        host: true,
        port: 8080,
        strictPort: true,
        proxy: {
            '/api': {
                target: 'http://127.0.0.1:8888',
                changeOrigin: true,
                ws: true,
                rewrite: (path) => {
                    if (path === '/api/ws') {
                        return '/ws';
                    }
                    if (path.startsWith('/api/ws/')) {
                        return path.replace(/^\/api/, '');
                    }
                    return path;
                },
            },
        },
    },
    plugins: [react(), mode === "development" && componentTagger()].filter(Boolean),
    resolve: {
        alias: {
            "@": path.resolve(__dirname, "./src"),
        },
    },
}));
