import { useEffect, useRef } from 'react';
import { wsManager } from '@/lib/websocket';
import { WS_ENABLED } from '@/lib/config';

type TaskStatusChangedData = {
  type: 'task_status_changed';
  dag_id: string;
  run_id: string;
  task_id: string;
  status: string;
  timestamp: number;
};

type DAGRunCompletedData = {
  type: 'dag_run_completed';
  dag_id: string;
  run_id: string;
  status: string;
  timestamp: number;
};

type EventHandler<T> = (data: T) => void;

interface UseWebSocketEventsOptions {
  onTaskStatusChanged?: EventHandler<TaskStatusChangedData>;
  onDAGRunCompleted?: EventHandler<DAGRunCompletedData>;
  autoConnect?: boolean;
}

export function useWebSocketEvents(options: UseWebSocketEventsOptions = {}) {
  const { onTaskStatusChanged, onDAGRunCompleted, autoConnect = true } = options;
  const unsubscribersRef = useRef<Array<() => void>>([]);

  useEffect(() => {
    if (!autoConnect || !WS_ENABLED) return;

    wsManager.connect();

    const unsubscribers: Array<() => void> = [];

    if (onTaskStatusChanged) {
      const unsub = wsManager.on('task_status_changed', onTaskStatusChanged);
      unsubscribers.push(unsub);
    }

    if (onDAGRunCompleted) {
      const unsub = wsManager.on('dag_run_completed', onDAGRunCompleted);
      unsubscribers.push(unsub);
    }

    unsubscribersRef.current = unsubscribers;

    return () => {
      unsubscribers.forEach(unsub => unsub());
    };
  }, [onTaskStatusChanged, onDAGRunCompleted, autoConnect]);

  return {
    connect: () => {
      if (WS_ENABLED) {
        wsManager.connect();
      }
    },
    disconnect: () => wsManager.disconnect(),
  };
}
