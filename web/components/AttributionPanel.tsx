"use client";

import { useState } from "react";
import clsx from "clsx";
import type { MonthlyAttributionRow } from "@/lib/queries";
import { money, pct } from "@/lib/format";

type ViewMode = "contribution" | "performance";

interface MonthlyMeta {
  label: string;
  start_nav: number | null;
  end_nav: number | null;
  gain: number | null;
  return_pct: number | null;
}

interface Props {
  monthIso: string;
  monthMeta: MonthlyMeta;
  rows: MonthlyAttributionRow[];
  reportingCcy?: string;
  topN?: number;
}

function performancePct(r: MonthlyAttributionRow): number | null {
  // Per-holding return: include income, normalise by avg cap base
  // (start + 0.5 * flows) — same Modified-Dietz convention used elsewhere.
  // Falls back to start_mv when there were no flows (typical hold).
  const denom = r.start_mv + 0.5 * r.flows;
  if (denom <= 0) return null;
  return r.gain / denom;
}

function contributionPct(r: MonthlyAttributionRow, portfolioStartNav: number): number | null {
  if (!portfolioStartNav || portfolioStartNav <= 0) return null;
  return r.gain / portfolioStartNav;
}

function Tone({
  value,
  format,
  width = 60,
}: {
  value: number | null;
  format: (v: number) => string;
  width?: number;
}) {
  if (value == null) {
    return <span className="text-xs text-slate-400">—</span>;
  }
  return (
    <span
      className={clsx(
        "rounded px-2 py-0.5 text-xs font-medium text-center tabular-nums",
        value >= 0
          ? "bg-emerald-50 text-emerald-700"
          : "bg-rose-50 text-rose-700",
      )}
      style={{ minWidth: width }}
    >
      {format(value)}
    </span>
  );
}

function Row({
  r,
  mode,
  portfolioStartNav,
  reportingCcy,
}: {
  r: MonthlyAttributionRow;
  mode: ViewMode;
  portfolioStartNav: number;
  reportingCcy: string;
}) {
  const perfPct = performancePct(r);
  const contribPct = contributionPct(r, portfolioStartNav);
  const pctValue = mode === "contribution" ? contribPct : perfPct;
  return (
    <div className="flex items-center justify-between border-b border-slate-100 py-2 text-xs last:border-b-0">
      <div className="min-w-0 flex-1 pr-2">
        <div className="truncate font-medium text-slate-800">
          {r.ticker_masttro ?? "—"}
          <span className="ml-2 truncate font-normal text-slate-500">
            {r.asset_name ?? ""}
          </span>
        </div>
      </div>
      <div className="flex items-center gap-3">
        <span className="tabular-nums text-slate-500">
          {money(r.gain, reportingCcy)}
        </span>
        <Tone
          value={pctValue}
          format={v => `${(v * 100).toFixed(2)}%`}
        />
      </div>
    </div>
  );
}

export default function AttributionPanel({
  monthIso,
  monthMeta,
  rows,
  reportingCcy = "USD",
  topN = 10,
}: Props) {
  const [mode, setMode] = useState<ViewMode>("contribution");
  void monthIso;  // accepted but not displayed — month label sits in monthMeta

  const portfolioStartNav = monthMeta.start_nav ?? 0;

  // Sort: by gain for contribution view; by per-holding return for performance view.
  const sortKey: (r: MonthlyAttributionRow) => number =
    mode === "contribution" ? r => r.gain : r => performancePct(r) ?? -Infinity;

  const contributors = rows
    .filter(r => r.gain > 0)
    .sort((a, b) => sortKey(b) - sortKey(a))
    .slice(0, topN);
  const detractors = rows
    .filter(r => r.gain < 0)
    .sort((a, b) => sortKey(a) - sortKey(b))
    .slice(0, topN);

  return (
    <div className="space-y-4">
      <div className="grid grid-cols-2 gap-3 md:grid-cols-4">
        <KpiCell label="Period" value={monthMeta.label} tone="default" />
        <KpiCell
          label="End NAV"
          value={
            monthMeta.end_nav != null
              ? money(monthMeta.end_nav, reportingCcy)
              : "—"
          }
          tone="default"
        />
        <KpiCell
          label="Total gain"
          value={
            monthMeta.gain != null ? money(monthMeta.gain, reportingCcy) : "—"
          }
          tone={
            monthMeta.gain == null
              ? "default"
              : monthMeta.gain >= 0
                ? "positive"
                : "negative"
          }
        />
        <KpiCell
          label="Return"
          value={
            monthMeta.return_pct != null ? pct(monthMeta.return_pct, 2) : "—"
          }
          tone={
            monthMeta.return_pct == null
              ? "default"
              : monthMeta.return_pct >= 0
                ? "positive"
                : "negative"
          }
        />
      </div>

      <div className="flex items-center gap-1 text-xs">
        {(
          [
            { key: "contribution", label: "Top contributors (by $)" },
            { key: "performance", label: "Best performers (by %)" },
          ] as const
        ).map(t => (
          <button
            key={t.key}
            type="button"
            onClick={() => setMode(t.key)}
            className={clsx(
              "rounded-full px-3 py-1 font-medium",
              mode === t.key
                ? "bg-brand text-white"
                : "text-slate-500 hover:bg-slate-100",
            )}
          >
            {t.label}
          </button>
        ))}
      </div>

      <div className="grid grid-cols-1 gap-4 md:grid-cols-2">
        <div className="rounded-lg border border-slate-200 bg-white p-4">
          <div className="mb-2 text-sm font-semibold text-emerald-700">
            {mode === "contribution"
              ? "Top contributors"
              : "Best performers"} — {monthMeta.label}
          </div>
          {contributors.length === 0 ? (
            <div className="py-4 text-xs text-slate-400">
              No positive contributors this month
            </div>
          ) : (
            contributors.map(r => (
              <Row
                key={r.security_id}
                r={r}
                mode={mode}
                portfolioStartNav={portfolioStartNav}
                reportingCcy={reportingCcy}
              />
            ))
          )}
        </div>
        <div className="rounded-lg border border-slate-200 bg-white p-4">
          <div className="mb-2 text-sm font-semibold text-rose-700">
            {mode === "contribution"
              ? "Biggest detractors"
              : "Worst performers"} — {monthMeta.label}
          </div>
          {detractors.length === 0 ? (
            <div className="py-4 text-xs text-slate-400">
              No detractors this month
            </div>
          ) : (
            detractors.map(r => (
              <Row
                key={r.security_id}
                r={r}
                mode={mode}
                portfolioStartNav={portfolioStartNav}
                reportingCcy={reportingCcy}
              />
            ))
          )}
        </div>
      </div>
    </div>
  );
}

function KpiCell({
  label,
  value,
  tone,
}: {
  label: string;
  value: string;
  tone: "default" | "positive" | "negative";
}) {
  return (
    <div className="rounded-lg border border-slate-200 bg-white px-4 py-3">
      <div className="text-xs uppercase tracking-wide text-slate-500">{label}</div>
      <div
        className={clsx(
          "mt-1 text-lg font-semibold tabular-nums",
          tone === "positive" && "text-emerald-600",
          tone === "negative" && "text-rose-600",
          tone === "default" && "text-slate-900",
        )}
      >
        {value}
      </div>
    </div>
  );
}
