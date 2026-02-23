import { AppLayout } from "@/components/AppLayout";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Separator } from "@/components/ui/separator";
import { Server, Database, CheckCircle2, XCircle, Palette, RefreshCw, Info } from "lucide-react";
import { useI18n } from "@/contexts/I18nContext";
import { useSystemStatus, useSystemHealth } from "@/hooks/useSystemData";
import { useTheme } from "@/components/ThemeProvider";
import { API_BASE } from "@/lib/config";
import { useState, useEffect } from "react";
import { toast } from "sonner";

export default function Settings() {
    const { t, locale, setLocale } = useI18n();
    const { theme, setTheme } = useTheme();
    const { data: status } = useSystemStatus();
    const { data: health } = useSystemHealth();

    // Local storage for user preferences
    const [refreshInterval, setRefreshInterval] = useState(() => {
      return localStorage.getItem("refreshInterval") || "5000";
    });
    const [notifyOnFailure, setNotifyOnFailure] = useState(() => {
      return localStorage.getItem("notifyOnFailure") === "true";
    });

    useEffect(() => {
      localStorage.setItem("refreshInterval", refreshInterval);
    }, [refreshInterval]);

    useEffect(() => {
      localStorage.setItem("notifyOnFailure", String(notifyOnFailure));
    }, [notifyOnFailure]);

    const handleThemeChange = (newTheme: string) => {
      setTheme(newTheme as "light" | "dark" | "system");
      toast.success(t.settings.themeUpdated);
    };

    const handleLanguageChange = (newLocale: string) => {
      setLocale(newLocale as "zh" | "en");
      toast.success(newLocale === "zh" ? t.settings.languageSwitchedZh : t.settings.languageSwitchedEn);
    };

    const handleRefreshIntervalChange = (interval: string) => {
      setRefreshInterval(interval);
      if (interval === "0") {
        toast.success(t.settings.refreshDisabled);
      } else {
        toast.success(t.settings.refreshIntervalSet.replace('{seconds}', String(parseInt(interval) / 1000)));
      }
    };

    return (
        <AppLayout title={t.settings.title} subtitle={t.settings.subtitle}>
            <div className="grid gap-6 md:grid-cols-2">
                {/* Appearance Settings */}
                <Card>
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2">
                            <Palette className="h-5 w-5" />
                            {t.settings.appearance}
                        </CardTitle>
                        <CardDescription>{t.settings.appearanceDesc}</CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-4">
                        <div className="flex items-center justify-between">
                            <div className="space-y-0.5">
                                <Label>{t.settings.theme}</Label>
                                <p className="text-sm text-muted-foreground">{t.settings.themeDesc}</p>
                            </div>
                            <Select value={theme} onValueChange={handleThemeChange}>
                                <SelectTrigger className="w-32">
                                    <SelectValue />
                                </SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="light">{t.settings.themeLight}</SelectItem>
                                    <SelectItem value="dark">{t.settings.themeDark}</SelectItem>
                                    <SelectItem value="system">{t.settings.themeSystem}</SelectItem>
                                </SelectContent>
                            </Select>
                        </div>

                        <Separator />

                        <div className="flex items-center justify-between">
                            <div className="space-y-0.5">
                                <Label>{t.settings.language}</Label>
                                <p className="text-sm text-muted-foreground">{t.settings.selectLanguage}</p>
                            </div>
                            <Select value={locale} onValueChange={handleLanguageChange}>
                                <SelectTrigger className="w-32">
                                    <SelectValue />
                                </SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="zh">{t.settings.zh}</SelectItem>
                                    <SelectItem value="en">{t.settings.en}</SelectItem>
                                </SelectContent>
                            </Select>
                        </div>
                    </CardContent>
                </Card>

                {/* Data Refresh Settings */}
                <Card>
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2">
                            <RefreshCw className="h-5 w-5" />
                            {t.settings.dataRefresh}
                        </CardTitle>
                        <CardDescription>{t.settings.dataRefreshDesc}</CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-4">
                        <div className="flex items-center justify-between">
                            <div className="space-y-0.5">
                                <Label>{t.settings.refreshInterval}</Label>
                                <p className="text-sm text-muted-foreground">{t.settings.refreshIntervalDesc}</p>
                            </div>
                            <Select value={refreshInterval} onValueChange={handleRefreshIntervalChange}>
                                <SelectTrigger className="w-32">
                                    <SelectValue />
                                </SelectTrigger>
                                <SelectContent>
                                    <SelectItem value="3000">{t.settings.interval3s}</SelectItem>
                                    <SelectItem value="5000">{t.settings.interval5s}</SelectItem>
                                    <SelectItem value="10000">{t.settings.interval10s}</SelectItem>
                                    <SelectItem value="30000">{t.settings.interval30s}</SelectItem>
                                    <SelectItem value="0">{t.settings.intervalOff}</SelectItem>
                                </SelectContent>
                            </Select>
                        </div>

                        <Separator />

                        <div className="flex items-center justify-between">
                            <div className="space-y-0.5">
                                <Label htmlFor="notify-failure">{t.settings.notifyOnFailure}</Label>
                                <p className="text-sm text-muted-foreground">{t.settings.notifyOnFailureDesc}</p>
                            </div>
                            <Switch
                                id="notify-failure"
                                checked={notifyOnFailure}
                                onCheckedChange={(checked) => {
                                    setNotifyOnFailure(checked);
                                    toast.success(checked ? t.settings.notifyEnabled : t.settings.notifyDisabled);
                                }}
                            />
                        </div>
                    </CardContent>
                </Card>

                {/* System Status */}
                <Card>
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2">
                            <Server className="h-5 w-5" />
                            {t.settings.systemStatus}
                        </CardTitle>
                        <CardDescription>{t.settings.systemStatusDesc}</CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-4">
                        <div className="flex items-center justify-between">
                            <span className="text-sm text-muted-foreground">{t.settings.healthStatus}</span>
                            <Badge variant={health?.status === "healthy" ? "default" : "destructive"}>
                                {health?.status === "healthy" ? (
                                    <><CheckCircle2 className="h-3 w-3 mr-1" /> {t.settings.healthy}</>
                                ) : (
                                    <><XCircle className="h-3 w-3 mr-1" /> {t.settings.unhealthy}</>
                                )}
                            </Badge>
                        </div>
                        <div className="flex items-center justify-between">
                            <span className="text-sm text-muted-foreground">{t.settings.lastUpdated}</span>
                            <span className="text-sm font-medium">
                                {status?.timestamp ? new Date(status.timestamp).toLocaleString() : "-"}
                            </span>
                        </div>
                    </CardContent>
                </Card>

                {/* Resource Stats */}
                <Card>
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2">
                            <Database className="h-5 w-5" />
                            {t.settings.resourceStats}
                        </CardTitle>
                        <CardDescription>{t.settings.resourceStatsDesc}</CardDescription>
                    </CardHeader>
                    <CardContent className="space-y-4">
                        <div className="flex items-center justify-between">
                            <span className="text-sm text-muted-foreground">{t.settings.totalDAGs}</span>
                            <span className="text-sm font-medium">{status?.dag_count ?? 0}</span>
                        </div>
                        <div className="flex items-center justify-between">
                            <span className="text-sm text-muted-foreground">{t.settings.activeRuns}</span>
                            <span className="text-sm font-medium">{status?.active_runs ? t.common.yes : t.common.no}</span>
                        </div>
                    </CardContent>
                </Card>

                {/* System Information */}
                <Card className="md:col-span-2">
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2">
                            <Info className="h-5 w-5" />
                            {t.settings.systemInfo}
                        </CardTitle>
                        <CardDescription>{t.settings.systemInfoDesc}</CardDescription>
                    </CardHeader>
                    <CardContent>
                        <div className="grid gap-4 md:grid-cols-3">
                            <div className="space-y-1">
                                <p className="text-sm font-medium">{t.settings.webUIVersion}</p>
                                <p className="text-2xl font-bold">v1.0.0</p>
                            </div>
                            <div className="space-y-1">
                                <p className="text-sm font-medium">{t.settings.apiEndpoint}</p>
                                <p className="text-sm text-muted-foreground font-mono break-all">{API_BASE}</p>
                            </div>
                            <div className="space-y-1">
                                <p className="text-sm font-medium">{t.settings.buildTime}</p>
                                <p className="text-sm text-muted-foreground">2025-02-13</p>
                            </div>
                        </div>
                    </CardContent>
                </Card>

            </div>
        </AppLayout>
    );
}
