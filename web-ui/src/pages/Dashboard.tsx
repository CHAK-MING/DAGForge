import { AppLayout } from "@/components/AppLayout";
import { StatCard } from "@/components/StatCard";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { RunSparkline } from "@/components/RunSparkline";
import {
  GitBranch,
  Play,
  CheckCircle2,
  Clock,
  ArrowRight,
  AlertCircle,
  TrendingUp,
  Search,
  LayoutGrid,
  List,
  ChevronDown,
  ChevronUp,
} from "lucide-react";
import { useNavigate } from "react-router-dom";
import { useI18n } from "@/contexts/I18nContext";
import { useDAGsQuery, useTriggerDAGMutation } from "@/hooks/useDAGQueries";
import { useSystemStatus, useSystemHealth, useGlobalHistory } from "@/hooks/useSystemData";
import { useState, useMemo } from "react";
import type { RunRecord } from "@/lib/api";

const computeDashboardStats = (globalHistory: RunRecord[]) => {
  const recentRuns = globalHistory.slice(0, 50);
  const failedCount = recentRuns.filter(r => r.state === 'failed').length;
  const successCount = recentRuns.filter(r => r.state === 'success').length;
  const totalCompleted = failedCount + successCount;
  const successRate = totalCompleted > 0 ? Math.round((successCount / totalCompleted) * 100) : 100;

  const dagRunsMap = new Map<string, RunRecord[]>();
  globalHistory.forEach(run => {
    const dagId = run.dag_id;
    if (!dagRunsMap.has(dagId)) {
      dagRunsMap.set(dagId, []);
    }
    dagRunsMap.get(dagId)!.push(run);
  });

  const dagHealthMap = new Map<string, "healthy" | "failed">();
  dagRunsMap.forEach((runs, dagId) => {
    const hasRecentFailure = runs.slice(0, 5).some(r => r.state === 'failed');
    dagHealthMap.set(dagId, hasRecentFailure ? "failed" : "healthy");
  });

  const queueBacklog = globalHistory.filter(r => r.state === 'running').length;

  return {
    metrics: { failedCount, successCount, successRate, totalCompleted },
    dagRunsMap,
    dagHealthMap,
    queueBacklog
  };
};

export default function Dashboard() {
  const navigate = useNavigate();
  const { t, tf } = useI18n();
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [searchQuery, setSearchQuery] = useState("");
  const [statusFilter, setStatusFilter] = useState<"all" | "healthy" | "failed">("all");
  const [viewMode, setViewMode] = useState<"card" | "table">("card");
  const [showAdvancedMetrics, setShowAdvancedMetrics] = useState(false);

  const refetchInterval = autoRefresh ? 5000 : false;

  // Use React Query with automatic refetching
  const { data: dags = [], isLoading: dagsLoading } = useDAGsQuery(refetchInterval);
  const { data: status } = useSystemStatus(refetchInterval);
  const { data: health } = useSystemHealth(refetchInterval);

  const { data: stats } = useGlobalHistory(autoRefresh ? 10000 : false, {
    select: computeDashboardStats
  });
  const triggerDAGMutation = useTriggerDAGMutation();

  const { metrics, dagRunsMap, dagHealthMap, queueBacklog } = stats || {
    metrics: { failedCount: 0, successCount: 0, successRate: 100, totalCompleted: 0 },
    dagRunsMap: new Map(),
    dagHealthMap: new Map(),
    queueBacklog: 0
  };

  // Filter and search DAGs
  const filteredDAGs = useMemo(() => {
    return dags.filter(dag => {
      // Search filter
      const matchesSearch = searchQuery === "" ||
        dag.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
        dag.description?.toLowerCase().includes(searchQuery.toLowerCase());

      // Status filter
      const dagHealth = dagHealthMap.get(dag.dag_id) || "healthy";
      const matchesStatus = statusFilter === "all" ||
        (statusFilter === "healthy" && dagHealth === "healthy") ||
        (statusFilter === "failed" && dagHealth === "failed");

      return matchesSearch && matchesStatus;
    });
  }, [dags, searchQuery, statusFilter, dagHealthMap]);

  const handleTriggerDAG = (dagId: string) => {
    triggerDAGMutation.mutate(dagId);
  };

  const totalTasks = dags.reduce((sum, dag) => sum + (dag.tasks?.length || 0), 0);
  const activeRuns = status?.active_runs
    ? (typeof status.active_runs === 'boolean'
      ? (status.active_runs ? 1 : 0)
      : status.active_runs)
    : 0;
  const isHealthy = health?.status === 'healthy';

  return (
    <AppLayout title={t.dashboard.title} subtitle={t.dashboard.subtitle}>
      {/* Auto-refresh control */}
      <div className="flex justify-end mb-4">
        <div className="flex items-center gap-2 px-3 py-2 rounded-lg border border-border bg-card">
          <Switch
            id="auto-refresh"
            checked={autoRefresh}
            onCheckedChange={setAutoRefresh}
          />
          <Label htmlFor="auto-refresh" className="text-sm cursor-pointer">
            {autoRefresh ? "自动刷新 (5s)" : "已暂停刷新"}
          </Label>
        </div>
      </div>

      {/* Core Metrics - Always visible */}
      <div className="grid gap-5 md:grid-cols-3 mb-6">
        <StatCard
          title={t.dashboard.dagCount}
          value={status?.dag_count ?? dags.length}
          icon={GitBranch}
        />
        {/* O&M Failed Runs Alert */}
        <StatCard
          title={t.dashboard.failedTasks}
          value={metrics.failedCount}
          icon={AlertCircle}
          variant={metrics.failedCount > 0 ? "destructive" : "success"}
        />
        {/* O&M Success Rate Indicator */}
        <StatCard
          title={t.dashboard.successRate}
          value={`${metrics.successRate}%`}
          icon={TrendingUp}
          variant={metrics.successRate >= 90 ? "success" : metrics.successRate >= 70 ? "warning" : "destructive"}
        />
      </div>

      {/* Advanced Metrics Toggle */}
      <div className="flex justify-center mb-6">
        <Button
          variant="ghost"
          size="sm"
          onClick={() => setShowAdvancedMetrics(!showAdvancedMetrics)}
          className="text-muted-foreground hover:text-foreground"
        >
          {showAdvancedMetrics ? (
            <>
              {t.dashboard.hideMetrics} <ChevronUp className="ml-2 h-4 w-4" />
            </>
          ) : (
            <>
              {t.dashboard.moreMetrics} <ChevronDown className="ml-2 h-4 w-4" />
            </>
          )}
        </Button>
      </div>

      {/* Advanced Metrics - Collapsible */}
      {showAdvancedMetrics && (
        <div className="grid gap-4 md:grid-cols-2 mb-8">
          <StatCard
            title={t.dashboard.activeRuns}
            value={activeRuns}
            icon={Play}
            variant="default"
          />
          <StatCard
            title={t.dashboard.totalTasks}
            value={totalTasks}
            icon={Clock}
            variant="success"
          />
        </div>
      )}

      <div className="mb-6 space-y-4">
        <div className="flex items-center justify-between">
          <h2 className="text-xl font-semibold text-foreground">{t.dashboard.dags}</h2>
          <div className="flex items-center gap-2">
            {/* View Mode Toggle */}
            <div className="flex border border-border rounded-lg p-1">
              <Button
                variant={viewMode === "card" ? "secondary" : "ghost"}
                size="sm"
                onClick={() => setViewMode("card")}
                className="h-8 px-3"
              >
                <LayoutGrid className="h-4 w-4" />
              </Button>
              <Button
                variant={viewMode === "table" ? "secondary" : "ghost"}
                size="sm"
                onClick={() => setViewMode("table")}
                className="h-8 px-3"
              >
                <List className="h-4 w-4" />
              </Button>
            </div>
            <Button variant="outline" onClick={() => navigate("/dags")}>
              {t.common.viewAll}
              <ArrowRight className="ml-2 h-4 w-4" />
            </Button>
          </div>
        </div>

        {/* Search and Filter */}
        <div className="flex items-center gap-3">
          <div className="relative flex-1 max-w-md">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder={t.dashboard.searchPlaceholder}
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-9"
            />
          </div>
          <div className="flex gap-2">
            <Badge
              variant={statusFilter === "all" ? "default" : "outline"}
              className="cursor-pointer"
              onClick={() => setStatusFilter("all")}
            >
              {t.dashboard.allDags} ({dags.length})
            </Badge>
            <Badge
              variant={statusFilter === "healthy" ? "default" : "outline"}
              className="cursor-pointer"
              onClick={() => setStatusFilter("healthy")}
            >
              {t.dashboard.healthy}
            </Badge>
            <Badge
              variant={statusFilter === "failed" ? "destructive" : "outline"}
              className="cursor-pointer"
              onClick={() => setStatusFilter("failed")}
            >
              {t.dashboard.abnormal}
            </Badge>
          </div>
        </div>
      </div>

      {dagsLoading ? (
        <div className="text-center py-8 text-muted-foreground">{t.common.loading}</div>
      ) : viewMode === "card" ? (
        <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
          {filteredDAGs.slice(0, 6).map((dag) => {
            const dagRuns = dagRunsMap.get(dag.dag_id) || [];
            const dagHealth = dagHealthMap.get(dag.dag_id) || "healthy";
            const recentRun = dagRuns[0];

            return (
              <Card
                key={dag.dag_id}
                className="cursor-pointer bg-card/60 backdrop-blur supports-[backdrop-filter]:bg-background/60 hover:shadow-md hover:border-primary/40 transform hover:-translate-y-1 transition-all duration-300"
                onClick={() => navigate(`/dags/${dag.dag_id}`)}
              >
                <CardHeader className="pb-2">
                  <div className="flex items-start justify-between">
                    <CardTitle className="text-base">{dag.name}</CardTitle>
                    {dagHealth === "failed" && (
                      <Badge variant="destructive" className="text-xs">
                        异常
                      </Badge>
                    )}
                  </div>
                </CardHeader>
                <CardContent>
                  <p className="text-sm text-muted-foreground mb-3 line-clamp-2">
                    {dag.description || t.dags.noDescription}
                  </p>

                  {/* Sparkline */}
                  <div className="mb-3">
                    <div className="flex items-center justify-between mb-1">
                      <span className="text-xs text-muted-foreground">{t.dashboard.runHistory}</span>
                      <span className="text-xs text-muted-foreground">
                        {dagRuns.length > 0 ? `${dagRuns.length} ${t.dashboard.runs}` : t.dashboard.noRecords}
                      </span>
                    </div>
                    <RunSparkline runs={dagRuns} maxBars={10} />
                  </div>

                  <div className="flex items-center justify-between gap-2">
                    <span className="text-xs text-muted-foreground">
                      {tf(t.dags.taskCount, { count: dag.tasks?.length || 0 })}
                    </span>
                    <div className="flex gap-1">
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={(e) => {
                          e.stopPropagation();
                          handleTriggerDAG(dag.dag_id);
                        }}
                        className="h-7"
                      >
                        <Play className="h-3 w-3 mr-1" />
                        {t.common.run}
                      </Button>
                    </div>
                  </div>
                </CardContent>
              </Card>
            );
          })}
        </div>
      ) : (
        /* Table View */
        <div className="border rounded-lg">
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead className="bg-muted/50">
                <tr>
                  <th className="text-left p-3 text-sm font-medium">名称</th>
                  <th className="text-left p-3 text-sm font-medium">任务数</th>
                  <th className="text-left p-3 text-sm font-medium">运行历史</th>
                  <th className="text-left p-3 text-sm font-medium">状态</th>
                  <th className="text-right p-3 text-sm font-medium">操作</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-border">
                {filteredDAGs.slice(0, 10).map((dag) => {
                  const dagRuns = dagRunsMap.get(dag.dag_id) || [];
                  const dagHealth = dagHealthMap.get(dag.dag_id) || "healthy";
                  const recentRun = dagRuns[0];

                  return (
                    <tr
                      key={dag.dag_id}
                      className="hover:bg-accent cursor-pointer transition-colors"
                      onClick={() => navigate(`/dags/${dag.dag_id}`)}
                    >
                      <td className="p-3">
                        <div>
                          <div className="font-medium">{dag.name}</div>
                          <div className="text-xs text-muted-foreground line-clamp-1">
                            {dag.description || t.dags.noDescription}
                          </div>
                        </div>
                      </td>
                      <td className="p-3 text-sm">{dag.tasks?.length || 0}</td>
                      <td className="p-3">
                        <RunSparkline runs={dagRuns} maxBars={10} />
                      </td>
                      <td className="p-3">
                        <Badge variant={dagHealth === "failed" ? "destructive" : "secondary"}>
                          {dagHealth === "failed" ? t.dashboard.abnormal : t.dashboard.healthy}
                        </Badge>
                      </td>
                      <td className="p-3">
                        <div className="flex justify-end gap-1">
                          <Button
                            size="sm"
                            variant="outline"
                            onClick={(e) => {
                              e.stopPropagation();
                              handleTriggerDAG(dag.dag_id);
                            }}
                            className="h-7"
                          >
                            <Play className="h-3 w-3 mr-1" />
                            运行
                          </Button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {!dagsLoading && filteredDAGs.length === 0 && (
        <div className="text-center py-8 text-muted-foreground">
          {searchQuery || statusFilter !== "all"
            ? t.dashboard.noMatchingDags
            : t.common.noData}
        </div>
      )}

      <div className="grid gap-6 md:grid-cols-2 mt-8">
        <Card>
          <CardHeader>
            <CardTitle className="text-base font-semibold">{t.dashboard.systemStatus}</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {[
              {
                label: t.dashboard.schedulerEngine,
                status: isHealthy ? t.dashboard.running : t.dashboard.stopped,
                healthy: isHealthy
              },
              {
                label: t.dashboard.dagCount,
                status: tf(t.dags.totalCount, { count: status?.dag_count ?? dags.length }),
                healthy: true
              },
              {
                label: t.dashboard.activeRuns,
                status: String(activeRuns),
                healthy: true
              },
              {
                label: t.dashboard.queueBacklog,
                status: `${queueBacklog} ${t.dashboard.runningInstances}`,
                healthy: queueBacklog < 5
              },
              {
                label: t.dashboard.recentSuccessRate,
                status: `${metrics.successRate}%`,
                healthy: metrics.successRate >= 90
              },
            ].map((item, i) => (
              <div key={i} className="flex items-center justify-between">
                <span className="text-sm text-muted-foreground">{item.label}</span>
                <div className="flex items-center gap-2">
                  <div className={`h-2 w-2 rounded-full ${item.healthy ? "bg-success" : "bg-destructive"}`} />
                  <span className={`text-sm font-medium ${item.healthy ? "text-success" : "text-destructive"}`}>
                    {item.status}
                  </span>
                </div>
              </div>
            ))}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-base font-semibold">{t.dashboard.quickActions}</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {dags.slice(0, 5).map((dag) => (
              <div
                key={dag.dag_id}
                className="flex items-center justify-between p-3 rounded-lg border border-border"
              >
                <div>
                  <span className="font-medium">{dag.name}</span>
                  <span className="text-sm text-muted-foreground ml-2">
                    {tf(t.dags.taskCount, { count: dag.tasks?.length || 0 })}
                  </span>
                </div>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => handleTriggerDAG(dag.dag_id)}
                >
                  <Play className="h-3 w-3 mr-1" />
                  {t.common.run}
                </Button>
              </div>
            ))}
            {dags.length === 0 && (
              <p className="text-muted-foreground text-sm text-center py-4">
                {t.common.noData}
              </p>
            )}
          </CardContent>
        </Card>
      </div>
    </AppLayout>
  );
}
