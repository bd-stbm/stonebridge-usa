"use client";

import { PieChart, Pie, Cell } from "recharts";
import { pct } from "@/lib/format";

interface AllocationRow {
  asset_class: string;
  share: number;
}

interface Props {
  rows: AllocationRow[];
}

// Monochrome purple ramp, applied by weight rank: darkest for the largest
// slice down to lightest for the smallest. `rows` arrives sorted by weight
// (descending), so index 0 is the largest slice. Extra ramp steps cover
// portfolios with more than the four headline classes; anything beyond the
// ramp falls back to a neutral slate so the donut never runs out of colour.
const DONUT_RAMP = [
  "#534AB7",
  "#7F77DD",
  "#AFA9EC",
  "#CECBF6",
  "#E4E2FA",
  "#F2F1FC",
];
const RAMP_FALLBACK = "#CBD5E1";

function sliceColor(index: number): string {
  return DONUT_RAMP[index] ?? RAMP_FALLBACK;
}

export default function AllocationSummaryTile({ rows }: Props) {
  return (
    <div className="h-full rounded-lg border border-slate-200 bg-white p-5">
      <div className="text-xs uppercase tracking-wide text-slate-500">
        Allocation
      </div>
      <div className="mt-3 flex items-center gap-4">
        <PieChart width={76} height={76}>
          <Pie
            data={rows}
            dataKey="share"
            nameKey="asset_class"
            cx="50%"
            cy="50%"
            innerRadius={24}
            outerRadius={38}
            stroke="none"
            isAnimationActive={false}
          >
            {rows.map((row, i) => (
              <Cell key={row.asset_class} fill={sliceColor(i)} />
            ))}
          </Pie>
        </PieChart>
        <ul className="min-w-0 flex-1 space-y-1">
          {rows.map((row, i) => (
            <li
              key={row.asset_class}
              className="flex items-center gap-2 text-xs"
            >
              <span
                className="h-2.5 w-2.5 shrink-0 rounded-full"
                style={{ backgroundColor: sliceColor(i) }}
              />
              <span className="truncate text-slate-600">{row.asset_class}</span>
              <span className="ml-auto font-medium tabular-nums text-slate-900">
                {pct(row.share, 1)}
              </span>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}
