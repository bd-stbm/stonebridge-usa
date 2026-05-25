"use client";

import {
  BarChart,
  Bar,
  Cell,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  CartesianGrid,
} from "recharts";

export interface MonthlyReturnRow {
  month: string;            // ISO yyyy-mm-dd (first of month)
  label: string;            // e.g. "Apr 2026"
  return_pct: number | null;
  gain: number | null;
  start_nav: number | null;
  end_nav: number | null;
  flows: number;
}

interface Props {
  rows: MonthlyReturnRow[];
  selectedMonth: string | null;
  onSelect: (month: string) => void;
}

function pctLabel(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return "—";
  return `${(v * 100).toFixed(2)}%`;
}

function moneyShort(v: number | null | undefined): string {
  if (v == null || !Number.isFinite(v)) return "—";
  const abs = Math.abs(v);
  if (abs >= 1e6) return `$${(v / 1e6).toFixed(2)}M`;
  if (abs >= 1e3) return `$${(v / 1e3).toFixed(1)}k`;
  return `$${v.toFixed(0)}`;
}

export default function MonthlyReturnsBar({
  rows,
  selectedMonth,
  onSelect,
}: Props) {
  return (
    <div className="rounded-lg border border-slate-200 bg-white p-4">
      <div className="mb-3 flex items-center justify-between">
        <div className="text-sm font-medium text-slate-700">
          Monthly returns
          <span className="ml-2 text-xs text-slate-400">click a bar to drill in</span>
        </div>
        {selectedMonth ? (
          <button
            type="button"
            onClick={() => onSelect("")}
            className="rounded border border-slate-300 px-2 py-0.5 text-xs text-slate-600 hover:bg-slate-50"
          >
            Clear selection
          </button>
        ) : null}
      </div>
      <div className="h-64">
        <ResponsiveContainer>
          <BarChart
            data={rows}
            margin={{ top: 8, right: 12, bottom: 0, left: 0 }}
            onClick={e => {
              const payload = (e as { activePayload?: Array<{ payload: MonthlyReturnRow }> })
                ?.activePayload;
              if (payload?.[0]?.payload?.month) onSelect(payload[0].payload.month);
            }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
            <XAxis
              dataKey="label"
              tick={{ fontSize: 11, fill: "#64748b" }}
              interval={Math.max(0, Math.floor(rows.length / 12) - 1)}
              tickLine={false}
            />
            <YAxis
              tick={{ fontSize: 11, fill: "#64748b" }}
              tickFormatter={(v: number) => `${(v * 100).toFixed(0)}%`}
              width={48}
            />
            <Tooltip
              cursor={{ fill: "rgba(124, 58, 237, 0.05)" }}
              content={({ active, payload }) => {
                if (!active || !payload?.length) return null;
                const r = payload[0].payload as MonthlyReturnRow;
                return (
                  <div className="rounded border border-slate-200 bg-white px-3 py-2 text-xs shadow-md">
                    <div className="font-medium text-slate-800">{r.label}</div>
                    <div className="mt-1 text-slate-600">
                      Return: <span className="font-medium text-slate-900">{pctLabel(r.return_pct)}</span>
                    </div>
                    <div className="text-slate-600">
                      Gain: <span className="font-medium text-slate-900">{moneyShort(r.gain)}</span>
                    </div>
                  </div>
                );
              }}
            />
            <Bar dataKey="return_pct" radius={[3, 3, 0, 0]} cursor="pointer" minPointSize={2}>
              {rows.map((r, i) => {
                const isSelected = selectedMonth === r.month;
                const anySelected = selectedMonth != null && selectedMonth !== "";
                const baseColor =
                  r.return_pct == null
                    ? "#cbd5e1"
                    : r.return_pct >= 0
                      ? "#16a34a"
                      : "#dc2626";
                return (
                  <Cell
                    key={i}
                    fill={isSelected ? "#1e293b" : baseColor}
                    opacity={anySelected && !isSelected ? 0.35 : 1}
                  />
                );
              })}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
