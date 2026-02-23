import { useEffect, useState, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { Command } from "cmdk";
import { useDAGsQuery, useRunsQuery } from "@/hooks/useDAGQueries";
import {
  Search,
  GitBranch,
  Play,
  History,
  Settings,
  FileText,
} from "lucide-react";
import { Dialog, DialogContent } from "@/components/ui/dialog";
import { Badge } from "@/components/ui/badge";
import { useI18n } from "@/contexts/I18nContext";

interface CommandPaletteProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function CommandPalette({ open, onOpenChange }: CommandPaletteProps) {
  const navigate = useNavigate();
  const { t } = useI18n();
  const [search, setSearch] = useState("");

  const { data: dags = [] } = useDAGsQuery();
  const { data: recentRuns = [] } = useRunsQuery(undefined, 0);

  const handleSelect = useCallback((callback: () => void) => {
    onOpenChange(false);
    callback();
  }, [onOpenChange]);

  // Reset search when dialog closes
  useEffect(() => {
    if (!open) {
      setSearch("");
    }
  }, [open]);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="p-0 gap-0 max-w-2xl">
        <Command className="rounded-lg border-0 shadow-lg">
          <div className="flex items-center border-b px-3">
            <Search className="mr-2 h-4 w-4 shrink-0 opacity-50" />
            <Command.Input
              placeholder={t.commandPalette?.placeholder || "搜索 DAG、任务、历史记录..."}
              className="flex h-11 w-full rounded-md bg-transparent py-3 text-sm outline-none placeholder:text-muted-foreground disabled:cursor-not-allowed disabled:opacity-50"
              value={search}
              onValueChange={setSearch}
            />
          </div>

          <Command.List className="max-h-[400px] overflow-y-auto p-2">
            <Command.Empty className="py-6 text-center text-sm text-muted-foreground">
              {t.commandPalette?.noResults || "未找到结果"}
            </Command.Empty>

            {/* Navigation Commands */}
            <Command.Group heading={t.commandPalette?.navigation || "导航"}>
              <Command.Item
                onSelect={() => handleSelect(() => navigate("/"))}
                className="flex items-center gap-3 px-3 py-2 rounded-md cursor-pointer hover:bg-accent data-[selected=true]:bg-accent"
              >
                <GitBranch className="h-4 w-4" />
                <span>{t.dashboard?.title || "仪表板"}</span>
              </Command.Item>
              <Command.Item
                onSelect={() => handleSelect(() => navigate("/dags"))}
                className="flex items-center gap-3 px-3 py-2 rounded-md cursor-pointer hover:bg-accent data-[selected=true]:bg-accent"
              >
                <FileText className="h-4 w-4" />
                <span>{t.dags?.title || "DAG 列表"}</span>
              </Command.Item>
              <Command.Item
                onSelect={() => handleSelect(() => navigate("/history"))}
                className="flex items-center gap-3 px-3 py-2 rounded-md cursor-pointer hover:bg-accent data-[selected=true]:bg-accent"
              >
                <History className="h-4 w-4" />
                <span>{t.history?.title || "运行历史"}</span>
              </Command.Item>
              <Command.Item
                onSelect={() => handleSelect(() => navigate("/settings"))}
                className="flex items-center gap-3 px-3 py-2 rounded-md cursor-pointer hover:bg-accent data-[selected=true]:bg-accent"
              >
                <Settings className="h-4 w-4" />
                <span>{t.settings?.title || "设置"}</span>
              </Command.Item>
            </Command.Group>

            {/* DAG Commands */}
            {dags.length > 0 && (
              <Command.Group heading="DAGs">
                {dags.slice(0, 10).map((dag) => (
                  <Command.Item
                    key={dag.dag_id}
                    value={`dag-${dag.dag_id}-${dag.name}`}
                    onSelect={() => handleSelect(() => navigate(`/dags/${dag.dag_id}`))}
                    className="flex items-center justify-between gap-3 px-3 py-2 rounded-md cursor-pointer hover:bg-accent data-[selected=true]:bg-accent"
                  >
                    <div className="flex items-center gap-3">
                      <GitBranch className="h-4 w-4 text-muted-foreground" />
                      <div>
                        <div className="font-medium">{dag.name}</div>
                        <div className="text-xs text-muted-foreground line-clamp-1">
                          {dag.description || "无描述"}
                        </div>
                      </div>
                    </div>
                    <Badge variant="secondary" className="text-xs">
                      {dag.tasks?.length || 0} 任务
                    </Badge>
                  </Command.Item>
                ))}
              </Command.Group>
            )}

            {/* Recent Runs */}
            {recentRuns.length > 0 && (
              <Command.Group heading={t.commandPalette?.recentRuns || "最近运行"}>
                {recentRuns.slice(0, 5).map((run) => (
                  <Command.Item
                    key={run.dag_run_id}
                    value={`run-${run.dag_run_id}-${run.dag_name || run.dag_id}`}
                    onSelect={() => handleSelect(() => navigate(`/dags/${run.dag_id}`))}
                    className="flex items-center justify-between gap-3 px-3 py-2 rounded-md cursor-pointer hover:bg-accent data-[selected=true]:bg-accent"
                  >
                    <div className="flex items-center gap-3">
                      <Play className="h-4 w-4 text-muted-foreground" />
                      <div>
                        <div className="font-medium">{run.dag_name || run.dag_id}</div>
                        <div className="text-xs text-muted-foreground">
                          {new Date(run.started_at).toLocaleString()}
                        </div>
                      </div>
                    </div>
                    <Badge
                      variant={
                        run.state === "success"
                          ? "default"
                          : run.state === "failed"
                          ? "destructive"
                          : "secondary"
                      }
                      className="text-xs"
                    >
                      {run.state}
                    </Badge>
                  </Command.Item>
                ))}
              </Command.Group>
            )}
          </Command.List>

          <div className="border-t px-3 py-2 text-xs text-muted-foreground">
            <div className="flex items-center justify-between">
              <span>快捷键提示</span>
              <div className="flex items-center gap-2">
                <kbd className="px-2 py-1 bg-muted rounded text-xs">↑↓</kbd>
                <span>导航</span>
                <kbd className="px-2 py-1 bg-muted rounded text-xs ml-2">Enter</kbd>
                <span>选择</span>
                <kbd className="px-2 py-1 bg-muted rounded text-xs ml-2">Esc</kbd>
                <span>关闭</span>
              </div>
            </div>
          </div>
        </Command>
      </DialogContent>
    </Dialog>
  );
}
