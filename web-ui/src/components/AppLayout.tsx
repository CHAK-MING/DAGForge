import { ReactNode, useState, useCallback } from "react";
import { SidebarProvider, SidebarTrigger, SidebarInset } from "@/components/ui/sidebar";
import { AppSidebar } from "@/components/AppSidebar";
import { ThemeToggle } from "@/components/ThemeToggle";
import { LanguageToggle } from "@/components/LanguageToggle";
import { Bell, CheckCircle2, XCircle, AlertCircle } from "lucide-react";
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuTrigger,
    DropdownMenuSeparator,
} from "@/components/ui/dropdown-menu";
import { Button } from "@/components/ui/button";
import { useI18n } from "@/contexts/I18nContext";
import { useWebSocket } from "@/hooks/useWebSocket";
import type { WebSocketEventData } from "@/lib/websocket";

interface AppLayoutProps {
    readonly children: ReactNode;
    readonly title?: string;
    readonly subtitle?: string;
}

interface Notification {
    id: string;
    type: "success" | "error" | "info";
    message: string;
    time: Date;
}

export function AppLayout({ children, title, subtitle }: AppLayoutProps) {
    const { t } = useI18n();
    const MAX_NOTIFICATIONS = 20;
    const [notifications, setNotifications] = useState<Notification[]>([]);

    const handleRunCompleted = useCallback((data: WebSocketEventData) => {
        if (data.type !== "dag_run_completed") return;
        const isError = data.status === "failed";
        const isSuccess = data.status === "success";
        const notif: Notification = {
            id: Date.now().toString(),
            type: isError ? "error" : isSuccess ? "success" : "info",
            message: `DAG run ${isSuccess ? "completed" : isError ? "failed" : "finished"}`,
            time: new Date(),
        };
        setNotifications(prev => [notif, ...prev].slice(0, MAX_NOTIFICATIONS));
    }, []);

    useWebSocket("dag_run_completed", handleRunCompleted);

    const clearNotifications = () => setNotifications([]);

    const notificationIcon = (type: string) => {
        switch (type) {
            case "success": return <CheckCircle2 className="h-4 w-4 text-success" />;
            case "error": return <XCircle className="h-4 w-4 text-destructive" />;
            default: return <AlertCircle className="h-4 w-4 text-muted-foreground" />;
        }
    };

    return (
        <SidebarProvider>
            <div className="flex min-h-screen w-full bg-[radial-gradient(ellipse_at_top,_var(--tw-gradient-stops))] from-primary/5 via-background to-background text-foreground">
                <AppSidebar />
                <SidebarInset className="flex flex-1 flex-col bg-transparent">
                    <header className="sticky top-0 z-10 flex h-16 items-center justify-between border-b border-border bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60 px-6">
                        <div className="flex items-center gap-4">
                            <SidebarTrigger className="h-9 w-9" />
                            {title && (
                                <div className="flex flex-col">
                                    <h1 className="text-lg font-semibold text-foreground">{title}</h1>
                                    {subtitle && (
                                        <p className="text-sm text-muted-foreground">{subtitle}</p>
                                    )}
                                </div>
                            )}
                        </div>

                        <div className="flex items-center gap-3">
                            <DropdownMenu>
                                <DropdownMenuTrigger asChild>
                                    <Button variant="ghost" size="icon" className="relative h-9 w-9" aria-label={t.settings.notifications}>
                                        <Bell className="h-5 w-5" />
                                        {notifications.length > 0 && (
                                            <span className="absolute -top-0.5 -right-0.5 h-4 w-4 rounded-full bg-destructive text-[10px] font-medium text-destructive-foreground flex items-center justify-center">
                                                {notifications.length > 9 ? "9+" : notifications.length}
                                            </span>
                                        )}
                                    </Button>
                                </DropdownMenuTrigger>
                                <DropdownMenuContent align="end" className="w-80">
                                    <div className="flex items-center justify-between px-3 py-2">
                                        <span className="font-medium">{t.settings.notifications}</span>
                                        {notifications.length > 0 && (
                                            <button onClick={clearNotifications} className="text-xs text-muted-foreground hover:text-foreground" aria-label={t.settings.clearAll}>
                                                {t.settings.clearAll}
                                            </button>
                                        )}
                                    </div>
                                    <DropdownMenuSeparator />
                                    {notifications.length === 0 ? (
                                        <div className="px-3 py-6 text-center text-sm text-muted-foreground">
                                            {t.settings.noNotifications}
                                        </div>
                                    ) : (
                                        <div className="max-h-80 overflow-y-auto">
                                            {notifications.map((n) => (
                                                <DropdownMenuItem key={n.id} className="flex items-start gap-2 px-3 py-2">
                                                    {notificationIcon(n.type)}
                                                    <div className="flex-1 min-w-0">
                                                        <p className="text-sm truncate">{n.message}</p>
                                                        <p className="text-xs text-muted-foreground">
                                                            {n.time.toLocaleTimeString()}
                                                        </p>
                                                    </div>
                                                </DropdownMenuItem>
                                            ))}
                                        </div>
                                    )}
                                </DropdownMenuContent>
                            </DropdownMenu>
                            <LanguageToggle />
                            <ThemeToggle />
                        </div>
                    </header>

                    <main className="flex-1 p-6">
                        {children}
                    </main>
                </SidebarInset>
            </div>
        </SidebarProvider>
    );
}
