import { useCallback, useMemo, useState, useEffect } from "react";
import ReactFlow, {
  Node,
  Edge,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  Position,
  MarkerType,
} from "reactflow";
import "reactflow/dist/style.css";
import { cn } from "@/lib/utils";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Badge } from "@/components/ui/badge";
import { useI18n } from "@/contexts/I18nContext";

type FlowTaskStatus =
  | "success"
  | "failed"
  | "upstream_failed"
  | "running"
  | "retrying"
  | "pending"
  | "skipped";

interface Task {
  id: string;
  name: string;
  status: FlowTaskStatus;
  duration?: string;
  executor?: string;
  dependsOn?: string[];
}

interface FlowDependency {
  from: string;
  to: string;
  label?: string;
}

interface DAGFlowProps {
  tasks: Task[];
  dependencies?: FlowDependency[];
  className?: string;
  onTaskClick?: (task: Task) => void;
}

const statusColors: Record<FlowTaskStatus, { bg: string; border: string; text: string }> = {
  success: {
    bg: "hsl(142 71% 45% / 0.15)",
    border: "hsl(142 71% 45%)",
    text: "hsl(142 71% 35%)",
  },
  failed: {
    bg: "hsl(0 84% 60% / 0.15)",
    border: "hsl(0 84% 60%)",
    text: "hsl(0 84% 40%)",
  },
  upstream_failed: {
    bg: "hsl(24 95% 53% / 0.15)",
    border: "hsl(24 95% 53%)",
    text: "hsl(24 95% 38%)",
  },
  running: {
    bg: "hsl(199 89% 48% / 0.15)",
    border: "hsl(199 89% 48%)",
    text: "hsl(199 89% 38%)",
  },
  retrying: {
    bg: "hsl(45 93% 47% / 0.15)",
    border: "hsl(45 93% 47%)",
    text: "hsl(45 93% 32%)",
  },
  pending: {
    bg: "hsl(38 92% 50% / 0.15)",
    border: "hsl(38 92% 50%)",
    text: "hsl(38 92% 35%)",
  },
  skipped: {
    bg: "hsl(215 16% 47% / 0.15)",
    border: "hsl(215 16% 47%)",
    text: "hsl(215 16% 37%)",
  },
};

function useStatusLabels(): Record<FlowTaskStatus, string> {
  const { t } = useI18n();
  return {
    success: t.runStatus.success,
    failed: t.runStatus.failed,
    upstream_failed: t.runStatus.upstreamFailed,
    running: t.runStatus.running,
    retrying: t.runStatus.retrying,
    pending: t.runStatus.pending,
    skipped: t.runStatus.skipped,
  };
}

export function DAGFlow({ tasks, dependencies, className, onTaskClick }: DAGFlowProps) {
  const { t } = useI18n();
  const statusLabels = useStatusLabels();
  const [selectedTask, setSelectedTask] = useState<Task | null>(null);

  const normalizedDependencies = useMemo<FlowDependency[]>(() => {
    if (dependencies && dependencies.length > 0) {
      return dependencies;
    }

    return tasks.flatMap((task) =>
      (task.dependsOn || []).map((dep) => ({ from: dep, to: task.id }))
    );
  }, [tasks, dependencies]);

  // Build adjacency lists and compute topological layers
  const { nodePositions } = useMemo(() => {
    const nodeWidth = 180;
    const nodeHeight = 70;
    const horizontalGap = 120;
    const verticalGap = 100;

    // Build dependency graph
    const taskMap = new Map(tasks.map(t => [t.id, t]));
    const inDegree = new Map<string, number>();
    const children = new Map<string, string[]>();
    const parents = new Map<string, string[]>();

    // Initialize
    for (const task of tasks) {
      inDegree.set(task.id, 0);
      children.set(task.id, []);
      parents.set(task.id, []);
    }

    for (const { from, to } of normalizedDependencies) {
      if (taskMap.has(from) && taskMap.has(to)) {
        children.get(from)!.push(to);
        parents.get(to)!.push(from);
        inDegree.set(to, (inDegree.get(to) || 0) + 1);
      }
    }

    // Compute layers using longest path (ensures proper DAG depth)
    const layer = new Map<string, number>();
    const queue: string[] = [];

    // Start with nodes that have no parents
    for (const task of tasks) {
      if ((inDegree.get(task.id) || 0) === 0) {
        layer.set(task.id, 0);
        queue.push(task.id);
      }
    }

    // BFS to compute max layer for each node
    while (queue.length > 0) {
      const nodeId = queue.shift()!;
      const currentLayer = layer.get(nodeId)!;
      for (const child of children.get(nodeId) || []) {
        const newLayer = currentLayer + 1;
        if (!layer.has(child) || layer.get(child)! < newLayer) {
          layer.set(child, newLayer);
        }
        // Decrease in-degree and add to queue when all parents processed
        const newDegree = (inDegree.get(child) || 1) - 1;
        inDegree.set(child, newDegree);
        if (newDegree === 0) {
          queue.push(child);
        }
      }
    }

    // Handle disconnected nodes (no dependencies)
    for (const task of tasks) {
      if (!layer.has(task.id)) {
        layer.set(task.id, 0);
      }
    }

    // Group nodes by layer
    const layers = new Map<number, string[]>();
    for (const [nodeId, layerIdx] of layer) {
      if (!layers.has(layerIdx)) {
        layers.set(layerIdx, []);
      }
      layers.get(layerIdx)!.push(nodeId);
    }

    // Calculate positions
    const positions = new Map<string, { x: number; y: number }>();

    // If dependency data is unavailable, avoid fake linear structure and
    // render a compact grid so users don't infer non-existent dependencies.
    const maxLayer = Math.max(...Array.from(layers.keys()), 0);
    if (maxLayer === 0) {
      const columns = Math.max(1, Math.ceil(Math.sqrt(tasks.length)));
      for (const [index, task] of tasks.entries()) {
        const col = index % columns;
        const row = Math.floor(index / columns);
        positions.set(task.id, {
          x: col * (nodeWidth + horizontalGap) + 50,
          y: row * (nodeHeight + verticalGap) + 50,
        });
      }
      return { nodePositions: positions };
    }

    const maxNodesInLayer = Math.max(...Array.from(layers.values()).map(l => l.length), 1);

    for (const [layerIdx, nodeIds] of layers) {
      const layerHeight = nodeIds.length * (nodeHeight + verticalGap);
      const totalHeight = maxNodesInLayer * (nodeHeight + verticalGap);
      const startY = (totalHeight - layerHeight) / 2 + 50;

      nodeIds.forEach((nodeId, idx) => {
        positions.set(nodeId, {
          x: layerIdx * (nodeWidth + horizontalGap) + 50,
          y: startY + idx * (nodeHeight + verticalGap),
        });
      });
    }

    return { nodePositions: positions };
  }, [tasks, normalizedDependencies]);

  const initialNodes: Node[] = useMemo(() => {
    const nodeWidth = 180;

    return tasks.map((task) => {
      const colors = statusColors[task.status];
      const isRunning = task.status === "running";
      const position = nodePositions.get(task.id) || { x: 50, y: 50 };

      return {
        id: task.id,
        data: {
          label: (
            <div className="text-center cursor-pointer">
              <div className="font-semibold text-[13px] tracking-wide mb-1.5 truncate">{task.name}</div>
              <div
                className="text-[11px] font-medium px-2.5 py-0.5 rounded-full inline-block"
                style={{
                  backgroundColor: colors.bg,
                  color: colors.text,
                  border: `1px solid ${colors.border}40`,
                }}
              >
                {statusLabels[task.status]}
              </div>
              {task.duration && (
                <div className="text-xs text-muted-foreground mt-1">
                  {task.duration}
                </div>
              )}
            </div>
          ),
          task,
        },
        position,
        style: {
          background: "hsl(var(--card) / 0.7)",
          backdropFilter: "blur(12px)",
          border: `1px solid ${colors.border}`,
          borderRadius: "10px",
          padding: "16px",
          width: nodeWidth,
          boxShadow: isRunning
            ? `0 0 0 1px ${colors.border}55, 0 8px 16px -4px ${colors.border}33`
            : `0 4px 12px -2px hsl(var(--foreground) / 0.05)`,
          color: "hsl(var(--foreground))",
          animation: isRunning ? "node-pulse 2s ease-in-out infinite" : undefined,
          transition: "all 0.3s ease",
        },
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
      };
    });
  }, [tasks, nodePositions]);

  const initialEdges: Edge[] = useMemo(() => {
    if (normalizedDependencies.length === 0) {
      return [];
    }

    return normalizedDependencies.map((dep, index) => {
      const targetTask = tasks.find((t) => t.id === dep.to);
      return {
        id: `e${dep.from}-${dep.to}-${index}`,
        source: dep.from,
        target: dep.to,
        type: "smoothstep",
        animated: targetTask?.status === "running",
        label: dep.label || undefined,
        labelStyle: dep.label
          ? {
            fill: "hsl(var(--foreground))",
            fontSize: 11,
            fontWeight: 600,
          }
          : undefined,
        labelBgStyle: dep.label
          ? {
            fill: "hsl(var(--card))",
            fillOpacity: 0.9,
            stroke: "hsl(var(--border))",
            strokeWidth: 1,
          }
          : undefined,
        labelBgPadding: dep.label ? [6, 2] : undefined,
        labelBgBorderRadius: dep.label ? 4 : undefined,
        style: {
          stroke: "hsl(var(--primary))",
          strokeWidth: 2,
        },
        markerEnd: {
          type: MarkerType.ArrowClosed,
          color: "hsl(var(--primary))",
        },
      };
    });
  }, [tasks, dependencies]);

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  useEffect(() => {
    setNodes(initialNodes);
  }, [initialNodes, setNodes]);

  useEffect(() => {
    setEdges(initialEdges);
  }, [initialEdges, setEdges]);

  const handleNodeClick = useCallback((_: React.MouseEvent, node: Node) => {
    const task = node.data.task as Task;
    if (onTaskClick) {
      onTaskClick(task);
    } else {
      setSelectedTask(task);
    }
  }, [onTaskClick]);

  return (
    <>
      <div className={cn("h-[400px] w-full rounded-lg border border-border bg-card", className)}>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          onNodesChange={onNodesChange}
          onEdgesChange={onEdgesChange}
          onNodeClick={handleNodeClick}
          fitView
          attributionPosition="bottom-left"
          proOptions={{ hideAttribution: true }}
        >
          <Background color="hsl(var(--muted-foreground))" gap={20} size={1} />
          <Controls
            className="bg-card border border-border rounded-lg"
            showInteractive={false}
          />
          <MiniMap
            className="bg-card border border-border rounded-lg"
            nodeColor={(node) => {
              const task = tasks.find((t) => t.id === node.id);
              if (!task) return "hsl(var(--muted))";
              return statusColors[task.status].border;
            }}
            maskColor="hsl(var(--background) / 0.8)"
          />
        </ReactFlow>
      </div>

      <Dialog open={!!selectedTask} onOpenChange={(open) => !open && setSelectedTask(null)}>
        <DialogContent className="sm:max-w-lg">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              {selectedTask?.name}
              <Badge
                variant="outline"
                className="ml-2"
                style={{
                  backgroundColor: selectedTask ? statusColors[selectedTask.status].bg : undefined,
                  color: selectedTask ? statusColors[selectedTask.status].text : undefined,
                  borderColor: selectedTask ? statusColors[selectedTask.status].border : undefined,
                }}
              >
                {selectedTask && statusLabels[selectedTask.status]}
              </Badge>
            </DialogTitle>
            <DialogDescription>
              {t.dagDetail.taskDefinitions}
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-4">
            {selectedTask?.executor && (
              <div className="flex items-center gap-2">
                <span className="text-sm text-muted-foreground">{t.dagDetail.executor}:</span>
                <Badge variant="secondary">{selectedTask.executor}</Badge>
              </div>
            )}
            {selectedTask?.dependsOn && selectedTask.dependsOn.length > 0 && (
              <div className="flex items-center gap-2 flex-wrap">
                <span className="text-sm text-muted-foreground">{t.dagDetail.dependsOn}:</span>
                {selectedTask.dependsOn.map((dep) => (
                  <Badge key={dep} variant="secondary">{dep}</Badge>
                ))}
              </div>
            )}
            {selectedTask?.duration && (
              <div className="flex items-center gap-2">
                <span className="text-sm text-muted-foreground">{t.dagDetail.executionTime}:</span>
                <span className="text-sm">{selectedTask.duration}</span>
              </div>
            )}
          </div>
        </DialogContent>
      </Dialog>
    </>
  );
}
