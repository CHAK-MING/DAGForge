import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { DAGStatusBadge, DAGStatusIcon, formatTime } from "@/lib/status";
import { Clock, Hand } from "lucide-react";
import { useI18n } from "@/contexts/I18nContext";
import { RunRecord } from "@/lib/api";

interface RunHistoryTabProps {
  runs: RunRecord[];
  selectedRunId?: string;
  onSelectRun: (runId: string) => void;
}

const getTriggerLabel = (type: string) => {
  switch (type) {
    case "schedule": return "定时";
    case "manual": return "手动";
    default: return "API";
  }
};

const getTriggerIcon = (type: string) => {
  return type === "schedule" ? Clock : Hand;
};

export function RunHistoryTab({ runs, selectedRunId, onSelectRun }: RunHistoryTabProps) {
  const { t, tf } = useI18n();

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">{t.dagDetail.runInstances}</CardTitle>
      </CardHeader>
      <CardContent>
        {runs.length > 0 ? (
          <div className="space-y-3">
            {runs.map((run, index) => {
              const TriggerIcon = getTriggerIcon(run.trigger_type || "manual");
              const runNumber = runs.length - index;

              return (
                <div
                  key={run.dag_run_id}
                  className={`flex items-center justify-between p-4 rounded-lg border transition-colors cursor-pointer ${
                    selectedRunId === run.dag_run_id
                      ? "border-primary bg-primary/5"
                      : "border-border hover:bg-accent/30"
                  }`}
                  onClick={() => onSelectRun(run.dag_run_id)}
                >
                  <div className="flex items-center gap-4">
                    <DAGStatusIcon status={run.state} />
                    <div>
                      <div className="flex items-center gap-2">
                        <span className="font-medium">Run #{runNumber}</span>
                        <DAGStatusBadge status={run.state} />
                        <Badge variant="outline" className="text-xs gap-1">
                          <TriggerIcon className="h-3 w-3" />
                          {getTriggerLabel(run.trigger_type || "manual")}
                        </Badge>
                      </div>
                      <p className="text-sm text-muted-foreground mt-1">
                        {t.dagDetail.started}: {formatTime(run.started_at)}
                        {run.finished_at && ` • ${t.dagDetail.finished}: ${formatTime(run.finished_at)}`}
                      </p>
                    </div>
                  </div>
                  <div className="text-sm text-muted-foreground">
                    {tf(t.dags.taskCount, { count: run.total_tasks || 0 })}
                  </div>
                </div>
              );
            })}
          </div>
        ) : (
          <div className="text-center py-8 text-muted-foreground">
            {t.dagDetail.noRuns}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
