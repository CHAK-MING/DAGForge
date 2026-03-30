import type { RunRecord } from "@/lib/api";

export interface DagHistoryOverview {
  dagRunsMap: Map<string, RunRecord[]>;
  dagHealthMap: Map<string, "healthy" | "failed">;
}

export function computeDagHistoryOverview(globalHistory: RunRecord[]): DagHistoryOverview {
  const dagRunsMap = new Map<string, RunRecord[]>();

  globalHistory.forEach((run) => {
    const dagId = run.dag_id;
    if (!dagRunsMap.has(dagId)) {
      dagRunsMap.set(dagId, []);
    }
    dagRunsMap.get(dagId)!.push(run);
  });

  const dagHealthMap = new Map<string, "healthy" | "failed">();
  dagRunsMap.forEach((runs, dagId) => {
    const hasRecentFailure = runs.slice(0, 5).some((run) => run.state === "failed");
    dagHealthMap.set(dagId, hasRecentFailure ? "failed" : "healthy");
  });

  return { dagRunsMap, dagHealthMap };
}
