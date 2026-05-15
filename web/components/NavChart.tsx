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

export default function NavChart({ data }: { data: NavPoint[] }) {
  return (
    <div className="rounded-lg border border-slate-200 bg-white p-4">
      <div className="mb-2 text-sm font-medium text-slate-700">NAV over time</div>
      <div className="h-72">
        <ResponsiveContainer>
          <AreaChart data={data} margin={{ top: 10, right: 20, bottom: 0, left: 0 }}>
            <defs>
              <linearGradient id="navFill" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor="#3b82f6" stopOpacity={0.4} />
                <stop offset="100%" stopColor="#3b82f6" stopOpacity={0} />
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
              tickFormatter={(v: number) => `$${(v / 1e6).toFixed(1)}M`}
              width={60}
            />
            <Tooltip
              formatter={(v: number) => money(v, "USD")}
              labelFormatter={(d: string) => d.slice(0, 10)}
            />
            <Area
              type="monotone"
              dataKey="nav"
              stroke="#2563eb"
              strokeWidth={2}
              fill="url(#navFill)"
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
