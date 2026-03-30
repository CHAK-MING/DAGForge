export type ExecutorType = "shell" | "docker" | "sensor" | "noop";

export type DAGRunState = "running" | "success" | "failed";

export type DAGDisplayStatus = DAGRunState | "no_run" | "inactive";

export const dagStatusLabels: Record<DAGDisplayStatus, string> = {
    running: "Running",
    success: "Success",
    failed: "Failed",
    no_run: "Not run",
    inactive: "Inactive",
};

export const dagStatusColors: Record<DAGDisplayStatus, string> = {
    running: "status-running",
    success: "status-success",
    failed: "status-failed",
    no_run: "bg-muted text-muted-foreground border border-border",
    inactive: "bg-muted text-muted-foreground border border-border",
};

export type TaskState =
    | "pending"
    | "ready"
    | "running"
    | "success"
    | "failed"
    | "upstream_failed"
    | "retrying"
    | "skipped";

export const taskStatusLabels: Record<TaskState, string> = {
    pending: "Waiting on deps",
    ready: "Ready",
    running: "Running",
    success: "Success",
    failed: "Failed",
    upstream_failed: "Upstream failed",
    retrying: "Retrying",
    skipped: "Skipped",
};

export const taskStatusColors: Record<TaskState, string> = {
    pending: "bg-muted text-muted-foreground border-border",
    ready: "bg-amber-500/10 text-amber-700 border-amber-500/30",
    running: "bg-blue-500/10 text-blue-600 border-blue-500/30",
    success: "bg-success/10 text-success border-success/30",
    failed: "bg-destructive/10 text-destructive border-destructive/30",
    upstream_failed: "bg-orange-500/10 text-orange-600 border-orange-500/30",
    retrying: "bg-warning/10 text-warning border-warning/30",
    skipped: "bg-slate-500/10 text-slate-500 border-slate-500/30",
};
