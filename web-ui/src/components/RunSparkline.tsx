import { useI18n } from "@/contexts/I18nContext";

interface RunSparklineProps {
  runs: Array<{ state: 'success' | 'failed' | 'running' }>;
  maxBars?: number;
}

export function RunSparkline({ runs, maxBars = 10 }: RunSparklineProps) {
  const { t } = useI18n();
  const recentRuns = runs.slice(0, maxBars);

  const getColor = (state: 'success' | 'failed' | 'running') => {
    switch (state) {
      case 'success':
        return 'bg-success';
      case 'failed':
        return 'bg-destructive';
      case 'running':
        return 'bg-primary';
      default:
        return 'bg-muted';
    }
  };

  const getTooltip = (state: 'success' | 'failed' | 'running') => {
    switch (state) {
      case 'success':
        return t.runStatus.success;
      case 'failed':
        return t.runStatus.failed;
      case 'running':
        return t.runStatus.running;
      default:
        return 'Unknown';
    }
  };

  if (recentRuns.length === 0) {
    return (
      <div className="flex gap-0.5">
        {Array.from({ length: maxBars }).map((_, i) => (
          <div
            key={i}
            className="w-1.5 h-4 rounded-sm bg-muted/30"
            title={t.dashboard.noRecords}
          />
        ))}
      </div>
    );
  }

  return (
    <div className="flex gap-0.5">
      {recentRuns.map((run, i) => (
        <div
          key={i}
          className={`w-1.5 h-4 rounded-sm ${getColor(run.state)} transition-all hover:h-5`}
          title={getTooltip(run.state)}
        />
      ))}
      {Array.from({ length: Math.max(0, maxBars - recentRuns.length) }).map((_, i) => (
        <div
          key={`empty-${i}`}
          className="w-1.5 h-4 rounded-sm bg-muted/30"
          title={t.dashboard.noRecords}
        />
      ))}
    </div>
  );
}
