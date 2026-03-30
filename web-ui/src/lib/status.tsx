import {
    Clock,
    CheckCircle2,
    XCircle,
    MinusCircle,
    Loader2,
    Timer,
    Terminal,
    AlertTriangle,
    RotateCcw,
    PlayCircle,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { useI18n } from "@/contexts/I18nContext";
import {
    DAGDisplayStatus,
    dagStatusLabels,
    dagStatusColors,
    TaskState,
    taskStatusLabels,
    taskStatusColors,
    ExecutorType,
} from "@/types/dag";

export const triggerRuleLabels: Record<string, string> = {
    all_success: "All success",
    all_failed: "All failed",
    all_done: "All done",
    one_success: "One success",
    one_failed: "One failed",
    none_failed: "None failed",
    none_skipped: "None skipped",
    all_done_min_one_success: "All done + one success",
    all_skipped: "All skipped",
    one_done: "One done",
    none_failed_min_one_success: "None failed + one success",
    always: "Always",
};

export const executorLabels: Record<ExecutorType, string> = {
    shell: "Shell",
    docker: "Docker",
    sensor: "Sensor",
    noop: "Noop",
};

export const sensorTypeLabels: Record<string, string> = {
    file: "File sensor",
    http: "HTTP sensor",
    command: "Command sensor",
};

export const dagStatusIcons: Record<DAGDisplayStatus, React.ElementType> = {
    running: Loader2,
    success: CheckCircle2,
    failed: XCircle,
    no_run: Clock,
    inactive: Timer,
};

export const taskStatusIcons: Record<TaskState, React.ElementType> = {
    pending: Clock,
    ready: PlayCircle,
    running: Loader2,
    success: CheckCircle2,
    failed: XCircle,
    upstream_failed: AlertTriangle,
    retrying: RotateCcw,
    skipped: MinusCircle,
};

export const executorIcons: Record<ExecutorType, React.ElementType> = {
    shell: Terminal,
    docker: Terminal,
    sensor: Timer,
    noop: MinusCircle,
};

interface DAGStatusBadgeProps {
    status: DAGDisplayStatus;
    className?: string;
}

export function DAGStatusBadge({ status, className }: DAGStatusBadgeProps) {
    const { t } = useI18n();
    const Icon = dagStatusIcons[status];
    const labels: Record<DAGDisplayStatus, string> = {
        running: t.runStatus.running,
        success: t.runStatus.success,
        failed: t.runStatus.failed,
        no_run: t.runStatus.noRun,
        inactive: t.runStatus.inactive,
    };
    return (
        <Badge
            variant="outline"
            className={cn(
                "flex items-center gap-1.5 px-2.5 py-1 border",
                dagStatusColors[status],
                className
            )}
        >
            <Icon className={cn("h-3.5 w-3.5", status === "running" && "animate-spin")} />
            {labels[status] ?? dagStatusLabels[status]}
        </Badge>
    );
}

interface TaskStatusBadgeProps {
    status: TaskState;
    className?: string;
}

export function TaskStatusBadge({ status, className }: TaskStatusBadgeProps) {
    const { t } = useI18n();
    const Icon = taskStatusIcons[status];
    const labels: Record<TaskState, string> = {
        pending: t.runStatus.pending,
        ready: t.runStatus.ready,
        running: t.runStatus.running,
        success: t.runStatus.success,
        failed: t.runStatus.failed,
        upstream_failed: t.runStatus.upstreamFailed,
        retrying: t.runStatus.retrying,
        skipped: t.runStatus.skipped,
    };
    return (
        <Badge
            variant="outline"
            className={cn(
                "flex items-center gap-1.5",
                taskStatusColors[status],
                className
            )}
        >
            <Icon className={cn("h-3 w-3", (status === "running" || status === "retrying") && "animate-spin")} />
            {labels[status] ?? taskStatusLabels[status]}
        </Badge>
    );
}

interface DAGStatusIconProps {
    status: DAGDisplayStatus;
    className?: string;
}

export function DAGStatusIcon({ status, className }: DAGStatusIconProps) {
    const Icon = dagStatusIcons[status];
    const colorClass = status === "success" ? "text-success" :
        status === "failed" ? "text-destructive" :
            status === "running" ? "text-primary" : "text-muted-foreground";
    return (
        <Icon className={cn(
            "h-5 w-5",
            colorClass,
            status === "running" && "animate-spin",
            className
        )} />
    );
}

interface TaskStatusIconProps {
    status: TaskState;
    className?: string;
}

export function TaskStatusIcon({ status, className }: TaskStatusIconProps) {
    const Icon = taskStatusIcons[status];
    const colorClass = status === "success" ? "text-success" :
        (status === "failed" || status === "upstream_failed") ? "text-destructive" :
            status === "running" ? "text-primary" :
                status === "ready" ? "text-amber-600" : "text-muted-foreground";
    return (
        <Icon className={cn(
            "h-5 w-5",
            colorClass,
            status === "running" && "animate-spin",
            className
        )} />
    );
}

export function formatDuration(ms: number): string {
    if (ms < 0) return "-";
    const seconds = Math.floor(ms / 1000);
    const minutes = Math.floor(seconds / 60);
    const hours = Math.floor(minutes / 60);
    if (hours > 0) return `${hours}h ${minutes % 60}m`;
    if (minutes > 0) return `${minutes}m ${seconds % 60}s`;
    return `${seconds}s`;
}

export function formatTime(isoString: string): string {
    if (!isoString || isoString === "-") return "-";
    try {
        return new Date(isoString).toLocaleString();
    } catch {
        return isoString;
    }
}
