"use client";

import {
  LineChart,
  Line,
  XAxis,
  YAxis,
  Tooltip,
  Legend,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";

export interface RebasedPoint {
  date: string;
  portfolio: number | null;
  benchmark: number | null;
}

interface Props {
  data: RebasedPoint[];
  benchmarkLabel: string;
}

export default function RebasedChart({ data, benchmarkLabel }: Props) {
  return (
    <div className="rounded-lg border border-slate-200 bg-white p-4">
      <div className="mb-2 flex items-center justify-between text-sm">
        <div className="font-medium text-slate-700">
          Portfolio vs benchmark (rebased to 100)
        </div>
      </div>
      <div className="h-72">
        <ResponsiveContainer>
          <LineChart data={data} margin={{ top: 10, right: 20, bottom: 0, left: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
            <XAxis
              dataKey="date"
              tick={{ fontSize: 11, fill: "#64748b" }}
              tickFormatter={(d: string) => d.slice(0, 7)}
            />
            <YAxis
              tick={{ fontSize: 11, fill: "#64748b" }}
              tickFormatter={(v: number) => v.toFixed(0)}
              width={42}
              domain={["auto", "auto"]}
            />
            <Tooltip
              formatter={(v: number) => v.toFixed(2)}
              labelFormatter={(d: string) => d.slice(0, 10)}
            />
            <Legend wrapperStyle={{ fontSize: 12 }} />
            <Line
              name="Portfolio"
              type="monotone"
              dataKey="portfolio"
              stroke="#7c3aed"
              strokeWidth={2}
              dot={false}
              connectNulls
            />
            <Line
              name={benchmarkLabel}
              type="monotone"
              dataKey="benchmark"
              stroke="#94a3b8"
              strokeWidth={2}
              dot={false}
              connectNulls
            />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
