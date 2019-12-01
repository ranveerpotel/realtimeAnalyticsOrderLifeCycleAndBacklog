import React, { useEffect, useState } from "react";

interface Order {
  order_id:        string;
  customer_name:   string;
  business_unit:   string;
  product_family:  string;
  order_status:    string;
  priority_tier:   number;
  total_value:     number;
  updated_at:      string;
  atp_feasible:    boolean;
  expected_ship_date?: string;
}

interface SearchResult {
  total:   number;
  orders:  Order[];
  facets:  Record<string, Record<string, number>>;
}

const STATUS_COLORS: Record<string, string> = {
  CREATED:       "#6366f1",
  ALLOCATED:     "#3b82f6",
  IN_PRODUCTION: "#f59e0b",
  SHIPPED:       "#22c55e",
  DELIVERED:     "#16a34a",
  CANCELLED:     "#ef4444",
};

const API_BASE = process.env.REACT_APP_API_URL || "http://localhost:8000";

export const OrderTable: React.FC<{ businessUnit?: string }> = ({ businessUnit }) => {
  const [result, setResult]   = useState<SearchResult | null>(null);
  const [query, setQuery]     = useState("");
  const [status, setStatus]   = useState("");
  const [page, setPage]       = useState(1);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);

    const params = new URLSearchParams({
      page:  String(page),
      size:  "25",
      ...(query        && { q:             query }),
      ...(status       && { status }),
      ...(businessUnit && { business_unit: businessUnit }),
    });

    fetch(`${API_BASE}/v1/orders/search?${params}`, { signal: controller.signal })
      .then((r) => r.json())
      .then((data) => { setResult(data); setLoading(false); })
      .catch((e) => { if (e.name !== "AbortError") setLoading(false); });

    return () => controller.abort();
  }, [query, status, page, businessUnit]);

  const formatDate = (iso: string) =>
    new Date(iso).toLocaleString("en-US", { dateStyle: "short", timeStyle: "short" });

  const priorityLabel = (t: number) => {
    const labels = ["", "P1-Critical", "P2-High", "P3-Medium", "P4-Low", "P5-Minimal"];
    return labels[t] ?? `P${t}`;
  };

  return (
    <div className="panel order-table-panel">
      <div className="panel-header">
        <h2>Orders {businessUnit ? `— ${businessUnit}` : ""}</h2>
        <div className="filters">
          <input
            type="text"
            placeholder="Search customer, order ID, SKU..."
            value={query}
            onChange={(e) => { setQuery(e.target.value); setPage(1); }}
            className="search-input"
          />
          <select value={status} onChange={(e) => { setStatus(e.target.value); setPage(1); }}>
            <option value="">All Statuses</option>
            {Object.keys(STATUS_COLORS).map((s) => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        </div>
      </div>

      {loading && <div className="loading-bar" />}

      {result && (
        <>
          <div className="result-count">
            {result.total.toLocaleString()} orders
          </div>
          <table className="data-table">
            <thead>
              <tr>
                <th>Order ID</th>
                <th>Customer</th>
                <th>Product Family</th>
                <th>Status</th>
                <th>Priority</th>
                <th>Value</th>
                <th>ATP</th>
                <th>Est. Ship</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody>
              {result.orders.map((o) => (
                <tr key={o.order_id}>
                  <td className="monospace">{o.order_id}</td>
                  <td>{o.customer_name}</td>
                  <td>{o.product_family}</td>
                  <td>
                    <span
                      className="status-badge"
                      style={{ backgroundColor: STATUS_COLORS[o.order_status] ?? "#888" }}
                    >
                      {o.order_status}
                    </span>
                  </td>
                  <td className={o.priority_tier <= 2 ? "text-danger bold" : ""}>
                    {priorityLabel(o.priority_tier)}
                  </td>
                  <td>
                    {new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", notation: "compact" })
                      .format(o.total_value)}
                  </td>
                  <td>
                    <span className={`dot ${o.atp_feasible ? "dot-green" : "dot-red"}`} />
                  </td>
                  <td>{o.expected_ship_date ? formatDate(o.expected_ship_date) : "—"}</td>
                  <td>{formatDate(o.updated_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>

          <div className="pagination">
            <button onClick={() => setPage((p) => Math.max(1, p - 1))} disabled={page === 1}>
              Previous
            </button>
            <span>Page {page}</span>
            <button
              onClick={() => setPage((p) => p + 1)}
              disabled={page * 25 >= result.total}
            >
              Next
            </button>
          </div>
        </>
      )}
    </div>
  );
};
