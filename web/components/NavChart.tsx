"use client";

import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";
import { NavPoint } from "@/lib/queries";
import { money } from "@/lib/format";

interface Props {
  data: NavPoint[];
  reportingCcy?: string;
}

export default function NavChart({ data, reportingCcy = "USD" }: Props) {
  // Y-axis tick formatter shows compact ($1.2M etc) — Intl.NumberFormat
  // with notation: "compact" lets the currency code propagate through
  // so an AUD-reporting client sees "A$1.2M" instead of "$1.2M".
  const compact = new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: reportingCcy,
    notation: "compact",
    maximumFractionDigits: 1,
  });
  return (
    <div className="rounded-lg border border-slate-200 bg-white p-4">
      <div className="mb-2 text-sm font-medium text-slate-700">NAV over time</div>
      <div className="h-72">
        <ResponsiveContainer>
          <AreaChart data={data} margin={{ top: 10, right: 20, bottom: 0, left: 0 }}>
            <defs>
              <linearGradient id="navFill" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#8b5cf6" stopOpacity={0.4} />
                <stop offset="100%" stopColor="#8b5cf6" stopOpacity={0} />
              </linearGradient>
            </defs>
            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
            <XAxis
              dataKey="snapshot_date"
              tick={{ fontSize: 11, fill: "#64748b" }}
              tickFormatter={(d: string) => d.slice(0, 7)}
            />
            <YAxis
              tick={{ fontSize: 11, fill: "#64748b" }}
              tickFormatter={(v: number) => compact.format(v)}
              width={70}
            />
            <Tooltip
              formatter={(v: number) => money(v, reportingCcy)}
              labelFormatter={(d: string) => d.slice(0, 10)}
            />
            <Area
              type="monotone"
              dataKey="nav"
              stroke="#7c3aed"
              strokeWidth={2}
              fill="url(#navFill)"
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
