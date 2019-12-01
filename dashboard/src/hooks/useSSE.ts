import { useEffect, useRef, useState } from "react";

export interface SSEEvent<T = unknown> {
  type: string;
  timestamp: number;
  data: T;
}

export function useSSE<T = unknown>(
  url: string,
  enabled = true
): { event: SSEEvent<T> | null; error: string | null; connected: boolean } {
  const [event, setEvent]       = useState<SSEEvent<T> | null>(null);
  const [error, setError]       = useState<string | null>(null);
  const [connected, setConnected] = useState(false);
  const esRef = useRef<EventSource | null>(null);

  useEffect(() => {
    if (!enabled) return;

    const es = new EventSource(url);
    esRef.current = es;

    es.onopen    = () => { setConnected(true); setError(null); };
    es.onmessage = (e) => {
      try {
        setEvent(JSON.parse(e.data) as SSEEvent<T>);
      } catch {
        setError("Failed to parse SSE event");
      }
    };
    es.onerror = () => {
      setConnected(false);
      setError("SSE connection lost — reconnecting...");
    };

    return () => {
      es.close();
      setConnected(false);
    };
  }, [url, enabled]);

  return { event, error, connected };
}
