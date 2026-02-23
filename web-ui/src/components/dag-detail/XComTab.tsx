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
  const selectedRun = runs.find(r => r.dag_run_id === selectedRunId);

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <div>
            <CardTitle className="text-base">XCom 跨任务数据</CardTitle>
            <p className="text-sm text-muted-foreground mt-1">
              任务间传递的数据值
            </p>
          </div>
          {runs.length > 0 && (
            <Select
              value={selectedRunId || ""}
              onValueChange={onSelectRun}
            >
              <SelectTrigger className="w-48">
                <SelectValue placeholder="选择运行实例" />
              </SelectTrigger>
              <SelectContent>
                {runs.map((run, index) => {
                  const num = runs.length - index;
                  return (
                    <SelectItem key={run.dag_run_id} value={run.dag_run_id}>
                      Run #{num} - {run.state === "success" ? "成功" : run.state === "failed" ? "失败" : "运行中"}
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
            请选择一个运行实例查看 XCom 数据
          </div>
        ) : Object.keys(xcomData).length > 0 ? (
          <div className="space-y-4">
            {Object.entries(xcomData).map(([taskId, values]) => (
              <div key={taskId} className="border rounded-lg p-4">
                <div className="flex items-center gap-2 mb-3">
                  <Badge variant="outline">{taskId}</Badge>
                  <span className="text-sm text-muted-foreground">
                    {Object.keys(values).length} 个键值
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
            该运行实例没有 XCom 数据
          </div>
        )}
      </CardContent>
    </Card>
  );
}
