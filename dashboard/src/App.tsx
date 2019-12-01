import React, { useEffect, useState } from "react";
import { BacklogPanel } from "./components/BacklogPanel";
import { OrderTable }   from "./components/OrderTable";
import { useSSE }       from "./hooks/useSSE";
import "./styles.css";

const API_BASE    = process.env.REACT_APP_API_URL || "http://localhost:8000";
const ROLES       = ["Supply Chain Planner", "Fulfillment Ops", "Finance", "Customer Service"];
const BUS         = ["All", "North America", "EMEA", "APAC", "Latin America"];

export default function App() {
  const [role, setRole]           = useState(ROLES[0]);
  const [bu, setBu]               = useState(BUS[0]);
  const [backlog, setBacklog]     = useState(null);
  const [backlogLoading, setBacklogLoading] = useState(true);

  const buParam = bu === "All" ? undefined : bu;

  // Real-time SSE push from API
  const sseUrl = `${API_BASE}/v1/stream/dashboard${buParam ? `?business_unit=${encodeURIComponent(buParam)}` : ""}`;
  const { event, connected } = useSSE(sseUrl);

  // Apply SSE push updates
  useEffect(() => {
    if (event?.type === "backlog_update") {
      setBacklog(event.data as any);
      setBacklogLoading(false);
    }
  }, [event]);

  // Initial fetch (before first SSE push arrives)
  useEffect(() => {
    setBacklogLoading(true);
    const params = buParam ? `?business_unit=${encodeURIComponent(buParam)}` : "";
    fetch(`${API_BASE}/v1/backlog/summary${params}`)
      .then((r) => r.json())
      .then((d) => { setBacklog(d); setBacklogLoading(false); })
      .catch(() => setBacklogLoading(false));
  }, [bu]);

  return (
    <div className="app">
      <header className="app-header">
        <div className="header-left">
          <h1>Order Analytics</h1>
          <span className={`connection-dot ${connected ? "dot-green" : "dot-red"}`} />
          <span className="connection-label">{connected ? "Live" : "Reconnecting..."}</span>
        </div>
        <div className="header-controls">
          <label>Role:
            <select value={role} onChange={(e) => setRole(e.target.value)}>
              {ROLES.map((r) => <option key={r}>{r}</option>)}
            </select>
          </label>
          <label>Business Unit:
            <select value={bu} onChange={(e) => setBu(e.target.value)}>
              {BUS.map((b) => <option key={b}>{b}</option>)}
            </select>
          </label>
        </div>
      </header>

      <main className="app-main">
        {(role === "Supply Chain Planner" || role === "Finance") && (
          <BacklogPanel summary={backlog} loading={backlogLoading} />
        )}
        {(role === "Fulfillment Ops" || role === "Customer Service" || role === "Finance") && (
          <OrderTable businessUnit={buParam} />
        )}
        {role === "Customer Service" && (
          <OrderTable businessUnit={buParam} />
        )}
      </main>
    </div>
  );
}
