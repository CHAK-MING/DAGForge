import { lazy, Suspense, useMemo } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { TaskConfig, RunRecord, TaskRunRecord } from "@/lib/api";

const DAGFlow = lazy(() => import("@/components/DAGFlow").then(m => ({ default: m.DAGFlow })));

interface TaskDefinition extends TaskConfig {
  dagId: string;
}

interface FlowGraphTabProps {
  taskDefinitions: TaskDefinition[];
  runs: RunRecord[];
  selectedRunId?: string;
  taskInstances: TaskRunRecord[];
  onSelectRun: (runId: string) => void;
}

export function FlowGraphTab({
  taskDefinitions,
  runs,
  selectedRunId,
  taskInstances,
  onSelectRun,
}: FlowGraphTabProps) {
  const selectedRun = runs.find(r => r.dag_run_id === selectedRunId);
  const runNumber = selectedRun ? runs.length - runs.indexOf(selectedRun) : 0;

  const taskInstanceById = useMemo(
    () => new Map(taskInstances.map(t => [t.task_id, t])),
    [taskInstances]
  );

  const taskDependencies = useMemo(
    () => taskDefinitions.flatMap((task) =>
      task.dependencies.map((dep) => ({ from: dep.task, to: task.task_id, label: dep.label }))
    ),
    [taskDefinitions]
  );

  const flowTasks = useMemo(() => taskDefinitions.map((td) => {
    let status: "pending" | "running" | "retrying" | "success" | "failed" | "upstream_failed" | "skipped" = "pending";

    if (selectedRun) {
      const taskInstance = taskInstanceById.get(td.task_id);
      if (taskInstance) {
        const taskState = taskInstance.state;
        if (taskState === "success") status = "success";
        else if (taskState === "failed") status = "failed";
        else if (taskState === "upstream_failed") status = "upstream_failed";
        else if (taskState === "skipped") status = "skipped";
        else if (taskState === "running") status = "running";
        else if (taskState === "retrying") status = "retrying";
        else if (taskState === "pending") status = "pending";
      } else if (selectedRun.state === "running") {
        status = "pending";
      }
    }

    return {
      id: td.task_id,
      name: td.name,
      status,
      duration: undefined,
      executor: "shell" as const,
    };
  }), [taskDefinitions, selectedRun, taskInstanceById]);

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between">
          <div>
            <CardTitle className="text-base">任务依赖关系图</CardTitle>
            <p className="text-sm text-muted-foreground mt-1">
              {selectedRun ? `显示 Run #${runNumber} 的状态` : "选择运行实例查看状态"}
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
      <CardContent className="p-0">
        <Suspense fallback={<Skeleton className="h-[400px] w-full" />}>
          <DAGFlow
            tasks={flowTasks}
            dependencies={taskDependencies}
            className="border-0 rounded-t-none"
          />
        </Suspense>
      </CardContent>
    </Card>
  );
}
