import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { executorIcons, executorLabels } from "@/lib/status";
import { TaskConfig } from "@/lib/api";

interface TaskDefinition extends TaskConfig {
  dagId: string;
}

interface TaskDefinitionsTabProps {
  tasks: TaskDefinition[];
}

export function TaskDefinitionsTab({ tasks }: TaskDefinitionsTabProps) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-base">任务定义</CardTitle>
      </CardHeader>
      <CardContent>
        <div className="space-y-3">
          {tasks.map((task, index) => {
            const ExecIcon = executorIcons["shell"];
            const dependsTasks = task.dependencies
              .map((d) => tasks.find((t) => t.task_id === d.task)?.name)
              .filter(Boolean);

            return (
              <div
                key={task.task_id}
                className="flex items-center justify-between p-4 rounded-lg border border-border bg-card"
              >
                <div className="flex items-center gap-4">
                  <span className="text-sm text-muted-foreground w-6">{index + 1}</span>
                  <div>
                    <span className="font-medium">{task.name}</span>
                    <div className="flex items-center gap-2 mt-1 flex-wrap">
                      <Badge variant="secondary" className="text-xs">
                        <ExecIcon className="mr-1 h-3 w-3" />
                        {executorLabels["shell"]}
                      </Badge>
                      {dependsTasks.length > 0 && (
                        <Badge variant="outline" className="text-xs">
                          依赖: {dependsTasks.join(", ")}
                        </Badge>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            );
          })}
          {tasks.length === 0 && (
            <div className="text-center py-8 text-muted-foreground">
              暂无任务
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
}
