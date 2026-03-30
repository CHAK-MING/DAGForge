import { useEffect, useMemo, useRef } from "react";
import { AppLayout } from "@/components/AppLayout";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Search, RefreshCw } from "lucide-react";
import { useLocation, useNavigate, useSearchParams } from "react-router-dom";
import { useI18n } from "@/contexts/I18nContext";
import { DAGSummaryCard } from "@/components/DAGSummaryCard";
import { computeDagHistoryOverview } from "@/lib/dag-overview";
import { matchesSearchQuery } from "@/lib/search";
import {
  useDAGsQuery,
  usePauseDAGMutation,
  useTriggerDAGMutation,
  useUnpauseDAGMutation,
} from "@/hooks/useDAGQueries";
import { useGlobalHistory } from "@/hooks/useSystemData";

const ITEMS_PER_PAGE = 20;
const SCROLL_STATE_KEY = "dagforge:dags:scroll-state";

export default function DAGs() {
  const navigate = useNavigate();
  const location = useLocation();
  const { t, tf } = useI18n();
  const [searchParams, setSearchParams] = useSearchParams();
  const restoredScrollRef = useRef(false);
  const searchQuery = searchParams.get("search") || "";
  const currentPageFromParams = Number.parseInt(searchParams.get("page") || "1", 10);

  const { data: availableDAGs = [], isLoading: loading, refetch: fetchDAGs, isRefetching } = useDAGsQuery();
  const { data: historyOverview } = useGlobalHistory(10000, {
    select: computeDagHistoryOverview,
  });
  const triggerDAGMutation = useTriggerDAGMutation();
  const pauseDAGMutation = usePauseDAGMutation();
  const unpauseDAGMutation = useUnpauseDAGMutation();
  const isRefreshing = loading || isRefetching;
  const dagRunsMap = historyOverview?.dagRunsMap ?? new Map();
  const dagHealthMap = historyOverview?.dagHealthMap ?? new Map();

  const filteredDAGs = useMemo(() => {
    return availableDAGs.filter((dag) => {
      return matchesSearchQuery(
        searchQuery,
        dag.dag_id,
        dag.name,
        dag.description,
        dag.cron
      );
    });
  }, [availableDAGs, searchQuery]);

  const totalPages = Math.max(1, Math.ceil(filteredDAGs.length / ITEMS_PER_PAGE));
  const currentPage = Number.isFinite(currentPageFromParams) && currentPageFromParams > 0
    ? Math.min(currentPageFromParams, totalPages)
    : 1;

  const pagedDAGs = useMemo(() => {
    const startIndex = (currentPage - 1) * ITEMS_PER_PAGE;
    return filteredDAGs.slice(startIndex, startIndex + ITEMS_PER_PAGE);
  }, [currentPage, filteredDAGs]);

  const updateSearchParams = (nextSearch: string, nextPage: number) => {
    const next = new URLSearchParams(searchParams);
    const normalizedSearch = nextSearch.trim();

    if (normalizedSearch) {
      next.set("search", normalizedSearch);
    } else {
      next.delete("search");
    }

    if (nextPage > 1) {
      next.set("page", String(nextPage));
    } else {
      next.delete("page");
    }

    setSearchParams(next, { replace: true });
  };

  useEffect(() => {
    if (currentPage !== currentPageFromParams && !(currentPage === 1 && !searchParams.get("page"))) {
      updateSearchParams(searchQuery, currentPage);
    }
  }, [currentPage, currentPageFromParams, searchParams, searchQuery]);

  useEffect(() => {
    if (loading || restoredScrollRef.current) {
      return;
    }

    const raw = sessionStorage.getItem(SCROLL_STATE_KEY);
    if (!raw) {
      restoredScrollRef.current = true;
      return;
    }

    try {
      const saved = JSON.parse(raw) as { path?: string; y?: number };
      if (saved.path === `${location.pathname}${location.search}` && typeof saved.y === "number") {
        requestAnimationFrame(() => window.scrollTo({ top: saved.y, behavior: "auto" }));
      }
    } catch {
      // Ignore malformed persisted scroll state.
    } finally {
      restoredScrollRef.current = true;
      sessionStorage.removeItem(SCROLL_STATE_KEY);
    }
  }, [loading, location.pathname, location.search]);

  const handleTriggerDAG = (id: string) => {
    triggerDAGMutation.mutate(id);
  };

  const handleViewDAG = (id: string) => {
    sessionStorage.setItem(
      SCROLL_STATE_KEY,
      JSON.stringify({
        path: `${location.pathname}${location.search}`,
        y: window.scrollY,
      })
    );
    navigate(`/dags/${id}`);
  };

  const handleTogglePause = (dagId: string, isPaused: boolean) => {
    if (isPaused) {
      unpauseDAGMutation.mutate(dagId);
      return;
    }
    pauseDAGMutation.mutate(dagId);
  };

  return (
    <AppLayout title={t.dags.title} subtitle={t.dags.subtitle}>
      <div className="flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4 mb-6">
        <div className="relative flex-1 sm:max-w-xs">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            placeholder={t.dags.searchPlaceholder}
            value={searchQuery}
            onChange={(e) => updateSearchParams(e.target.value, 1)}
            className="pl-9"
          />
        </div>

        <Button variant="outline" onClick={() => fetchDAGs()} disabled={isRefreshing}>
          <RefreshCw className={`mr-2 h-4 w-4 ${isRefreshing ? "animate-spin" : ""}`} />
          {t.common.refresh}
        </Button>
      </div>

      <p className="text-sm text-muted-foreground mb-4">{tf(t.dags.totalCount, { count: filteredDAGs.length })}</p>

      {loading && availableDAGs.length === 0 ? (
        <div className="text-center py-8 text-muted-foreground">{t.common.loading}</div>
      ) : (
        <>
          <div className="grid gap-4 sm:grid-cols-1 lg:grid-cols-2">
          {pagedDAGs.map((dag) => {
            const dagRuns = dagRunsMap.get(dag.dag_id) || [];
            const dagHealth = dagHealthMap.get(dag.dag_id) || "healthy";
            return (
              <DAGSummaryCard
                key={dag.dag_id}
                dag={dag}
                runs={dagRuns}
                health={dagHealth}
                onOpen={handleViewDAG}
                onTrigger={handleTriggerDAG}
                onTogglePause={handleTogglePause}
              />
            );
          })}
          </div>

          {filteredDAGs.length > ITEMS_PER_PAGE && (
            <div className="mt-6 flex flex-col items-center justify-between gap-3 border-t border-border/60 pt-4 sm:flex-row">
              <p className="text-sm text-muted-foreground">
                {tf(t.dags.pageInfo, { page: currentPage, total: totalPages })}
              </p>
              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => updateSearchParams(searchQuery, currentPage - 1)}
                  disabled={currentPage <= 1}
                >
                  {t.dags.prevPage}
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => updateSearchParams(searchQuery, currentPage + 1)}
                  disabled={currentPage >= totalPages}
                >
                  {t.dags.nextPage}
                </Button>
              </div>
            </div>
          )}
        </>
      )}

      {filteredDAGs.length === 0 && !loading && (
        <div className="flex flex-col items-center justify-center py-12 text-center">
          <div className="rounded-full bg-muted p-4 mb-4">
            <Search className="h-8 w-8 text-muted-foreground" />
          </div>
          <h3 className="text-lg font-medium">{t.dags.notFound}</h3>
          <p className="text-muted-foreground mt-1">{t.dags.adjustSearch}</p>
        </div>
      )}
    </AppLayout>
  );
}
