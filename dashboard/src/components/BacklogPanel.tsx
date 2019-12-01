import React, { useEffect, useState } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

interface ProductFamilyBacklog {
  product_family: string;
  demand:         number;
  supply:         number;
  gap:            number;
  order_count:    number;
}

interface BacklogSummary {
  business_unit:     string;
  total_unfulfilled: number;
  total_supply:      number;
  total_gap:         number;
  by_product_family: ProductFamilyBacklog[];
}

interface Props {
  summary: BacklogSummary | null;
  loading: boolean;
}

const GAP_THRESHOLD = 0;

export const BacklogPanel: React.FC<Props> = ({ summary, loading }) => {
  if (loading) {
    return <div className="panel loading">Loading backlog data...</div>;
  }
  if (!summary) {
    return <div className="panel empty">No backlog data available.</div>;
  }

  const chartData = summary.by_product_family.map((pf) => ({
    name:   pf.product_family,
    demand: Math.round(pf.demand / 1000),
    supply: Math.round(pf.supply / 1000),
    gap:    Math.round(pf.gap / 1000),
  }));

  const formatCurrency = (v: number) =>
    new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", notation: "compact" }).format(v);

  return (
    <div className="panel backlog-panel">
      <div className="panel-header">
        <h2>Backlog Summary — {summary.business_unit}</h2>
        <div className="kpi-row">
          <KpiCard label="Unfulfilled Demand" value={formatCurrency(summary.total_unfulfilled)} variant="warn" />
          <KpiCard label="Available Supply"   value={formatCurrency(summary.total_supply)}      variant="ok"   />
          <KpiCard label="Supply Gap"         value={formatCurrency(summary.total_gap)}         variant={summary.total_gap > 0 ? "danger" : "ok"} />
        </div>
      </div>

      <div className="chart-container">
        <h3>Demand vs Supply by Product Family ($K)</h3>
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={chartData} margin={{ top: 5, right: 20, left: 0, bottom: 60 }}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="name" angle={-35} textAnchor="end" interval={0} />
            <YAxis />
            <Tooltip formatter={(v: number) => `$${v}K`} />
            <Bar dataKey="demand" name="Unfulfilled Demand" fill="#ef4444" />
            <Bar dataKey="supply" name="Available Supply"   fill="#22c55e" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      <div className="product-table">
        <table>
          <thead>
            <tr>
              <th>Product Family</th>
              <th>Demand</th>
              <th>Supply</th>
              <th>Gap</th>
              <th>Orders</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {summary.by_product_family.map((pf) => (
              <tr key={pf.product_family} className={pf.gap > GAP_THRESHOLD ? "row-danger" : ""}>
                <td>{pf.product_family}</td>
                <td>{formatCurrency(pf.demand)}</td>
                <td>{formatCurrency(pf.supply)}</td>
                <td className={pf.gap > 0 ? "text-danger" : "text-ok"}>
                  {formatCurrency(pf.gap)}
                </td>
                <td>{pf.order_count.toLocaleString()}</td>
                <td>
                  <span className={`badge ${pf.gap > 0 ? "badge-danger" : "badge-ok"}`}>
                    {pf.gap > 0 ? "At Risk" : "Covered"}
                  </span>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

const KpiCard: React.FC<{ label: string; value: string; variant: "ok" | "warn" | "danger" }> = ({
  label, value, variant,
}) => (
  <div className={`kpi-card kpi-${variant}`}>
    <div className="kpi-label">{label}</div>
    <div className="kpi-value">{value}</div>
  </div>
);
