import { WS_URL } from './config';

export type WebSocketEventType = 'task_status_changed' | 'dag_run_completed';

interface WebSocketMessage {
  type: 'event' | 'connected';
  event?: WebSocketEventType;
  data?: string;
  timestamp?: string;
}

interface TaskStatusChangedData {
  type: 'task_status_changed';
  dag_id: string;
  run_id: string;
  task_id: string;
  status: string;
  timestamp: number;
}

interface DAGRunCompletedData {
  type: 'dag_run_completed';
  dag_id: string;
  run_id: string;
  status: string;
  timestamp: number;
}

export type WebSocketEventData = TaskStatusChangedData | DAGRunCompletedData;
type EventHandler = (data: WebSocketEventData) => void;

export class WebSocketManager {
  private ws: WebSocket | null = null;
  private readonly handlers: Map<WebSocketEventType, Set<EventHandler>> = new Map();
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectAttempt = 0;
  private readonly url: string;

  private static readonly RECONNECT_BASE_MS = 1000;
  private static readonly RECONNECT_MAX_MS = 30000;

  constructor() {
    this.url = WS_URL;
  }

  connect(): void {
    if (this.ws?.readyState === WebSocket.OPEN ||
      this.ws?.readyState === WebSocket.CONNECTING) return;

    this.ws = new WebSocket(this.url);

    this.ws.onopen = () => {
      this.reconnectAttempt = 0;
      if (this.reconnectTimer) {
        clearTimeout(this.reconnectTimer);
        this.reconnectTimer = null;
      }
    };

    this.ws.onmessage = (event) => {
      try {
        const message: WebSocketMessage = JSON.parse(event.data);

        if (message.type === 'event' && message.event && message.data) {
          const data = JSON.parse(message.data);
          const handlers = this.handlers.get(message.event);
          if (handlers) {
            handlers.forEach(handler => handler(data));
          }
        }
      } catch {
        // Ignore parse errors
      }
    };

    this.ws.onerror = () => {
      // Connection error, will reconnect on close
    };

    this.ws.onclose = () => {
      this.ws = null;
      this.scheduleReconnect();
    };
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer) return;

    const delay = Math.min(
      WebSocketManager.RECONNECT_BASE_MS * Math.pow(2, this.reconnectAttempt),
      WebSocketManager.RECONNECT_MAX_MS
    );
    this.reconnectAttempt++;

    this.reconnectTimer = globalThis.setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, delay);
  }

  on(event: WebSocketEventType, handler: EventHandler): () => void {
    if (!this.handlers.has(event)) {
      this.handlers.set(event, new Set());
    }
    this.handlers.get(event)!.add(handler);

    return () => {
      const handlers = this.handlers.get(event);
      if (handlers) {
        handlers.delete(handler);
      }
    };
  }

  isConnected(): boolean {
    return this.ws?.readyState === WebSocket.OPEN;
  }

  disconnect(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }

    this.handlers.clear();
  }
}

export const wsManager = new WebSocketManager();
