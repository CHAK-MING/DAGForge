import { useState } from "react";
import { AppLayout } from "@/components/AppLayout";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Search, Play, Pause, RefreshCw } from "lucide-react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { useI18n } from "@/contexts/I18nContext";
import {
  useDAGsQuery,
  usePauseDAGMutation,
  useTriggerDAGMutation,
  useUnpauseDAGMutation,
} from "@/hooks/useDAGQueries";

export default function DAGs() {
  const navigate = useNavigate();
  const { t, tf } = useI18n();
  const [searchParams] = useSearchParams();
  const [searchQuery, setSearchQuery] = useState(searchParams.get("search") || "");

  const { data: dags = [], isLoading: loading, refetch: fetchDAGs, isRefetching } = useDAGsQuery();
  const triggerDAGMutation = useTriggerDAGMutation();
  const pauseDAGMutation = usePauseDAGMutation();
  const unpauseDAGMutation = useUnpauseDAGMutation();
  const isRefreshing = loading || isRefetching;

  const filteredDAGs = dags.filter((dag) => {
    return (
      dag.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
      dag.description?.toLowerCase().includes(searchQuery.toLowerCase())
    );
  });

  const handleTriggerDAG = (id: string) => {
    triggerDAGMutation.mutate(id);
  };

  const handleViewDAG = (id: string) => {
    navigate(`/dags/${id}`);
  };

  const handleTogglePause = (dagId: string, isPaused: boolean) => {
    if (isPaused) {
      unpauseDAGMutation.mutate(dagId);
      return;
    }
    pauseDAGMutation.mutate(dagId);
  };

  return (
    <AppLayout title={t.dags.title} subtitle={t.dags.subtitle}>
      <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4 mb-6">
        <div className="relative flex-1 sm:max-w-xs">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder={t.dags.searchPlaceholder}
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="pl-9"
          />
        </div>

        <Button variant="outline" onClick={() => fetchDAGs()} disabled={isRefreshing}>
          <RefreshCw className={`mr-2 h-4 w-4 ${isRefreshing ? "animate-spin" : ""}`} />
          {t.common.refresh}
        </Button>
      </div>

      <p className="text-sm text-muted-foreground mb-4">{tf(t.dags.totalCount, { count: filteredDAGs.length })}</p>

      {loading && dags.length === 0 ? (
        <div className="text-center py-8 text-muted-foreground">{t.common.loading}</div>
      ) : (
        <div className="grid gap-4 sm:grid-cols-1 lg:grid-cols-2">
          {filteredDAGs.map((dag) => {
            return (
              <Card
                key={dag.dag_id}
                className="cursor-pointer hover:bg-accent/30 transition-colors"
                onClick={() => handleViewDAG(dag.dag_id)}
              >
                <CardHeader className="pb-2">
                  <div className="flex items-center justify-between gap-2">
                    <CardTitle className="text-base">{dag.name}</CardTitle>
                    {dag.is_paused && (
                      <Badge variant="outline" className="text-xs">
                        {t.common.paused}
                      </Badge>
                    )}
                  </div>
                </CardHeader>
                <CardContent>
                  <p className="text-sm text-muted-foreground mb-3 line-clamp-2">
                    {dag.description || t.dags.noDescription}
                  </p>
                  <div className="flex items-center justify-between">
                    <div className="text-xs text-muted-foreground space-x-3">
                      <span>{tf(t.dags.taskCount, { count: dag.tasks?.length || 0 })}</span>
                      {dag.cron && <span className="font-mono">{dag.cron}</span>}
                    </div>
                    <div className="flex items-center gap-2">
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={(e) => {
                          e.stopPropagation();
                          handleTogglePause(dag.dag_id, dag.is_paused);
                        }}
                      >
                        <Pause className="h-3 w-3 mr-1" />
                        {dag.is_paused ? t.common.resume : t.common.pause}
                      </Button>
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={(e) => {
                          e.stopPropagation();
                          handleTriggerDAG(dag.dag_id);
                        }}
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
      )}

      {filteredDAGs.length === 0 && !loading && (
        <div className="flex flex-col items-center justify-center py-12 text-center">
          <div className="rounded-full bg-muted p-4 mb-4">
            <Search className="h-8 w-8 text-muted-foreground" />
          </div>
          <h3 className="text-lg font-medium">{t.dags.notFound}</h3>
          <p className="text-muted-foreground mt-1">{t.dags.adjustSearch}</p>
        </div>
      )}
    </AppLayout>
  );
}
