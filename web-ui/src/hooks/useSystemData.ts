import { useQuery } from "@tanstack/react-query";
import { getStatus, getHealth, listHistory, SystemStatus, HealthStatus, RunRecord } from "@/lib/api";

export function useSystemStatus(refetchInterval: number | false = 5000) {
  return useQuery<SystemStatus>({
    queryKey: ['system-status'],
    queryFn: getStatus,
    refetchInterval,
    staleTime: 3000,
  });
}

export function useSystemHealth(refetchInterval: number | false = 5000) {
  return useQuery<HealthStatus>({
    queryKey: ['system-health'],
    queryFn: getHealth,
    refetchInterval,
    staleTime: 3000,
    retry: 1,
  });
}

export function useGlobalHistory<TData = RunRecord[]>(
  refetchInterval: number | false = 10000,
  options?: { select?: (data: RunRecord[]) => TData }
) {
  return useQuery({
    queryKey: ['global-history'],
    queryFn: () => listHistory(),
    refetchInterval,
    staleTime: 8000,
    select: options?.select,
  });
}
