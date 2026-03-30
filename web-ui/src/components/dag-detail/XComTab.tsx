import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { RunRecord, XComValue } from "@/lib/api";
import { useI18n } from "@/contexts/I18nContext";

interface XComTabProps {
  runs: RunRecord[];
  selectedRunId?: string;
  xcomData: { [taskId: string]: XComValue };
  onSelectRun: (runId: string) => void;
}

export function XComTab({
  runs,
  selectedRunId,
  xcomData,
  onSelectRun,
}: XComTabProps) {
  const { t } = useI18n();
  const selectedRun = runs.find(r => r.dag_run_id === selectedRunId);

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <div>
            <CardTitle className="text-base">{t.dagDetail.xcomData}</CardTitle>
            <p className="text-sm text-muted-foreground mt-1">
              {t.dagDetail.xcomDescription}
            </p>
          </div>
          {runs.length > 0 && (
            <Select
              value={selectedRunId || ""}
              onValueChange={onSelectRun}
            >
              <SelectTrigger className="w-48">
                <SelectValue placeholder={t.dagDetail.selectRun} />
              </SelectTrigger>
              <SelectContent>
                {runs.map((run, index) => {
                  const num = runs.length - index;
                  return (
                    <SelectItem key={run.dag_run_id} value={run.dag_run_id}>
                      Run #{num} - {t.runStatus[run.state]}
                    </SelectItem>
                  );
                })}
              </SelectContent>
            </Select>
          )}
        </div>
      </CardHeader>
      <CardContent>
        {!selectedRun ? (
          <div className="text-center py-8 text-muted-foreground">
            {t.dagDetail.selectRunToView}
          </div>
        ) : Object.keys(xcomData).length > 0 ? (
          <div className="space-y-4">
            {Object.entries(xcomData).map(([taskId, values]) => (
              <div key={taskId} className="border rounded-lg p-4">
                <div className="flex items-center gap-2 mb-3">
                  <Badge variant="outline">{taskId}</Badge>
                  <span className="text-sm text-muted-foreground">
                    {Object.keys(values).length} {t.dagDetail.variables}
                  </span>
                </div>
                <div className="bg-muted/50 rounded p-3 font-mono text-sm overflow-x-auto">
                  <pre>{JSON.stringify(values, null, 2)}</pre>
                </div>
              </div>
            ))}
          </div>
        ) : (
          <div className="text-center py-8 text-muted-foreground">
            {t.dagDetail.noXcom}
          </div>
        )}
      </CardContent>
    </Card>
  );
}
