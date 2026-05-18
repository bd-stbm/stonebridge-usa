"use client";

import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";
import { money } from "@/lib/format";

export interface MonthlyIncomePoint {
  month: string; // ISO yyyy-mm-dd (first of month)
  Dividends: number;
  Interest: number;
  Other: number;
}

interface Props {
  data: MonthlyIncomePoint[];
  reportingCcy: string;
}

export default function MonthlyIncomeChart({ data, reportingCcy }: Props) {
  return (
    <div className="rounded-lg border border-slate-200 bg-white p-4">
      <div className="mb-2 text-sm font-medium text-slate-700">
        Monthly income
      </div>
      <div className="h-72">
        <ResponsiveContainer>
          <BarChart data={data} margin={{ top: 10, right: 20, bottom: 0, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
            <XAxis
              dataKey="month"
              tick={{ fontSize: 11, fill: "#64748b" }}
              tickFormatter={(d: string) => d.slice(0, 7)}
            />
            <YAxis
              tick={{ fontSize: 11, fill: "#64748b" }}
              tickFormatter={(v: number) =>
                v >= 1e6 ? `$${(v / 1e6).toFixed(1)}M` : `$${(v / 1e3).toFixed(0)}K`
              }
              width={56}
            />
            <Tooltip
              formatter={(v: number) => money(v, reportingCcy)}
              labelFormatter={(d: string) => d.slice(0, 7)}
            />
            <Legend wrapperStyle={{ fontSize: 12 }} />
            <Bar dataKey="Dividends" stackId="income" fill="#059669" />
            <Bar dataKey="Interest" stackId="income" fill="#2563eb" />
            <Bar dataKey="Other" stackId="income" fill="#94a3b8" />
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
