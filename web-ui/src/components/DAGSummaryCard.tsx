import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Play, Pause } from "lucide-react";
import { RunSparkline } from "@/components/RunSparkline";
import { useI18n } from "@/contexts/I18nContext";
import type { DAGInfo, RunRecord } from "@/lib/api";

interface DAGSummaryCardProps {
  dag: DAGInfo;
  runs?: RunRecord[];
  health?: "healthy" | "failed";
  onOpen: (dagId: string) => void;
  onTrigger: (dagId: string) => void;
  onTogglePause?: (dagId: string, isPaused: boolean) => void;
}

export function DAGSummaryCard({
  dag,
  runs = [],
  health = "healthy",
  onOpen,
  onTrigger,
  onTogglePause,
}: DAGSummaryCardProps) {
  const { t, tf } = useI18n();

  return (
    <Card
      className="cursor-pointer bg-card/60 backdrop-blur supports-[backdrop-filter]:bg-background/60 hover:shadow-md hover:border-primary/40 transform hover:-translate-y-1 transition-all duration-300"
      onClick={() => onOpen(dag.dag_id)}
    >
      <CardHeader className="pb-2">
        <div className="flex items-start justify-between gap-2">
          <div className="min-w-0">
            <CardTitle className="text-base truncate">{dag.name}</CardTitle>
            <p className="text-xs text-muted-foreground font-mono mt-1 truncate">{dag.dag_id}</p>
          </div>
          <div className="flex items-center gap-2">
            {health === "failed" && (
              <Badge variant="destructive" className="text-xs">
                {t.dashboard.abnormal}
              </Badge>
            )}
            {!dag.cron && (
              <Badge variant="secondary" className="text-xs">
                {t.common.manualOnly}
              </Badge>
            )}
            {dag.is_paused && (
              <Badge variant="outline" className="text-xs">
                {t.common.paused}
              </Badge>
            )}
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <p className="text-sm text-muted-foreground mb-3 line-clamp-2">
          {dag.description || t.dags.noDescription}
        </p>

        <div className="mb-3">
          <div className="flex items-center justify-between mb-1">
            <span className="text-xs text-muted-foreground">{t.dashboard.runHistory}</span>
            <span className="text-xs text-muted-foreground">
              {runs.length > 0 ? `${runs.length} ${t.dashboard.runs}` : t.dashboard.noRecords}
            </span>
          </div>
          <RunSparkline runs={runs} maxBars={10} />
        </div>

        <div className="flex items-center justify-between text-xs text-muted-foreground mb-3 gap-2">
          <span>{tf(t.dags.taskCount, { count: dag.tasks?.length || 0 })}</span>
          {dag.cron ? (
            <span className="font-mono truncate">{dag.cron}</span>
          ) : (
            <span>{t.common.manualOnly}</span>
          )}
        </div>

        <div className="flex items-center justify-end gap-2">
          {onTogglePause && (
            <Button
              size="sm"
              variant="outline"
              onClick={(e) => {
                e.stopPropagation();
                onTogglePause(dag.dag_id, dag.is_paused);
              }}
            >
              <Pause className="h-3 w-3 mr-1" />
              {dag.is_paused ? t.common.resume : t.common.pause}
            </Button>
          )}
          <Button
            size="sm"
            variant="outline"
            onClick={(e) => {
              e.stopPropagation();
              onTrigger(dag.dag_id);
            }}
          >
            <Play className="h-3 w-3 mr-1" />
            {t.common.run}
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}
