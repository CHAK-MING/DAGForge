import { useState } from "react";
import { Toaster } from "@/components/ui/toaster";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import { ThemeProvider } from "@/components/ThemeProvider";
import { I18nProvider } from "@/contexts/I18nContext";
import { ErrorBoundary } from "@/components/ErrorBoundary";
import { CommandPalette } from "@/components/CommandPalette";
import { useHotkeys } from "react-hotkeys-hook";
import Dashboard from "./pages/Dashboard";
import DAGs from "./pages/DAGs";
import DAGDetail from "./pages/DAGDetail";
import History from "./pages/History";
import Settings from "./pages/Settings";
import NotFound from "./pages/NotFound";

const queryClient = new QueryClient({
    defaultOptions: {
        queries: {
            staleTime: 5000,
            retry: 2,
            refetchOnWindowFocus: false,
        },
    },
});

const AppRoutes = () => {
    const [commandOpen, setCommandOpen] = useState(false);

    // Global keyboard shortcuts
    useHotkeys('mod+k', (e) => {
        e.preventDefault();
        setCommandOpen(true);
    }, { enableOnFormTags: true });

    useHotkeys('mod+/', (e) => {
        e.preventDefault();
        setCommandOpen(true);
    }, { enableOnFormTags: true });

    return (
        <>
            <CommandPalette open={commandOpen} onOpenChange={setCommandOpen} />
            <Routes>
                <Route path="/" element={<Dashboard />} />
                <Route path="/dags" element={<DAGs />} />
                <Route path="/dags/:id" element={<DAGDetail />} />
                <Route path="/history" element={<History />} />
                <Route path="/settings" element={<Settings />} />
                <Route path="*" element={<NotFound />} />
            </Routes>
        </>
    );
};

const App = () => (
    <ErrorBoundary>
        <QueryClientProvider client={queryClient}>
            <ThemeProvider attribute="class" defaultTheme="system" enableSystem>
                <I18nProvider>
                    <TooltipProvider>
                        <Toaster />
                        <Sonner />
                        <BrowserRouter future={{ v7_startTransition: true, v7_relativeSplatPath: true }}>
                            <AppRoutes />
                        </BrowserRouter>
                    </TooltipProvider>
                </I18nProvider>
            </ThemeProvider>
        </QueryClientProvider>
    </ErrorBoundary>
);

export default App;
