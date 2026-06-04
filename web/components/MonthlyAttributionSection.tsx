"use client";

import { useMemo, useState } from "react";
import clsx from "clsx";
import MonthlyReturnsBar, { type MonthlyReturnRow } from "./MonthlyReturnsBar";
import AttributionPanel from "./AttributionPanel";
import type { MonthlyAttributionRow } from "@/lib/queries";
import { PERIODS, type PeriodKey } from "@/lib/returns";

interface Props {
  monthlyReturns: MonthlyReturnRow[];
  attributionByMonth: Record<string, MonthlyAttributionRow[]>;
  reportingCcy?: string;
}

// Attribution is month-end granularity (the RPC returns one row per
// security per month), so 1D — which needs intraday per-holding moves —
// can't be sourced here. Offer the four month-aligned periods only; they
// map cleanly onto a window of the monthly rows we already hold.
const ATTRIBUTION_PERIODS = PERIODS.filter(p => p.key !== "1d");

// Slice the displayed monthly rows down to the window a period covers.
// 1y / 6m are trailing-month counts; ytd is the calendar year of the
// latest month; mtd is the latest month alone (== its bar-chart drilldown).
function windowForPeriod(
  rows: MonthlyReturnRow[],
  period: PeriodKey,
): MonthlyReturnRow[] {
  if (rows.length === 0) return rows;
  const latest = rows[rows.length - 1];
  switch (period) {
    case "mtd":
      return [latest];
    case "ytd": {
      const year = latest.month.slice(0, 4);
      return rows.filter(r => r.month.slice(0, 4) === year);
    }
    case "6m":
      return rows.slice(-6);
    case "1y":
    default:
      return rows.slice(-12);
  }
}

export default function MonthlyAttributionSection({
  monthlyReturns,
  attributionByMonth,
  reportingCcy = "USD",
}: Props) {
  // Period selector windows the aggregated contributors/detractors
  // (MTD / YTD / 6M / 12M). Default 12M preserves the prior behaviour.
  const [period, setPeriod] = useState<PeriodKey>("1y");
  // No month selected → render the aggregated window for the period.
  // Clicking a bar drills into that one month (overriding the period);
  // Clear button on the bar chart returns to the period aggregate.
  const [selectedMonth, setSelectedMonth] = useState<string | null>(null);

  // The bar chart always shows the full 12M; only this windowed slice
  // drives the aggregate panel + KPIs below.
  const windowMonths = useMemo(
    () => windowForPeriod(monthlyReturns, period),
    [monthlyReturns, period],
  );

  // ---- Aggregated rollup over the selected period window ----
  // Build per-security totals: start_mv = mv at start of the earliest
  // window month we have for the security, end_mv = mv at end of the
  // latest, flows / income / gain summed. Months a security never
  // appeared in (e.g. small consistent contributors filtered out by the
  // server-side top-N) are missing — see RPC commit 4431700; in practice
  // the >$100k movers we display are always in-window.
  const aggregatedAttribution = useMemo<MonthlyAttributionRow[]>(() => {
    type Bucket = MonthlyAttributionRow;
    const byId = new Map<number, Bucket>();
    // Walk window months chronologically so the first encounter of a
    // security sets its window start_mv, the last encounter sets end_mv.
    for (const { month } of windowMonths) {
      const rows = attributionByMonth[month] ?? [];
      for (const r of rows) {
        const existing = byId.get(r.security_id);
        if (!existing) {
          byId.set(r.security_id, { ...r });
        } else {
          existing.end_mv = r.end_mv;
          existing.flows += r.flows;
          existing.income += r.income;
          existing.gain += r.gain;
        }
      }
    }
    return Array.from(byId.values());
  }, [windowMonths, attributionByMonth]);

  // KPI / meta block for the aggregated view: single-window Modified
  // Dietz over the selected period. Matches the rest of the dashboard
  // (Overview Returns tile, PerformanceMatrix) so each period's KPI here
  // reconciles with the same period's cell in the entity matrix and the
  // tile on Overview. Previously this compounded monthly returns
  // (chain-linked TWR), which diverged by ~0.5-1pp on entities with
  // meaningful cash flows (e.g. Optsia, with $2.5M of 12M withdrawals).
  const aggregatedMeta = useMemo(() => {
    if (windowMonths.length === 0) return null;
    const first = windowMonths[0];
    const last = windowMonths[windowMonths.length - 1];
    const startNav = first.start_nav ?? 0;
    const endNav = last.end_nav ?? 0;
    const totalFlows = windowMonths.reduce((s, m) => s + (m.flows ?? 0), 0);
    const gain = endNav - startNav - totalFlows;
    const denom = startNav + 0.5 * totalFlows;
    const return_pct = denom > 0 ? gain / denom : null;
    const label =
      ATTRIBUTION_PERIODS.find(p => p.key === period)?.label ??
      `Last ${windowMonths.length}M`;
    return {
      label,
      start_nav: startNav,
      end_nav: endNav,
      gain,
      return_pct,
    };
  }, [windowMonths, period]);

  const selectedRow =
    selectedMonth != null
      ? monthlyReturns.find(r => r.month === selectedMonth) ?? null
      : null;
  const selectedAttribution =
    selectedMonth != null ? attributionByMonth[selectedMonth] ?? [] : [];

  const showingDrilldown = selectedMonth != null && selectedRow != null;
  const panelMeta = showingDrilldown
    ? {
        label: selectedRow.label,
        start_nav: selectedRow.start_nav,
        end_nav: selectedRow.end_nav,
        gain: selectedRow.gain,
        return_pct: selectedRow.return_pct,
      }
    : aggregatedMeta;
  const panelRows = showingDrilldown
    ? selectedAttribution
    : aggregatedAttribution;

  return (
    <div className="space-y-4">
      <MonthlyReturnsBar
        rows={monthlyReturns}
        selectedMonth={selectedMonth}
        onSelect={m => setSelectedMonth(m || null)}
      />
      <div className="flex items-center justify-between">
        <div className="text-sm font-medium text-slate-700">
          Contributors &amp; detractors
        </div>
        <div className="flex rounded-full bg-slate-100 p-0.5">
          {ATTRIBUTION_PERIODS.map(p => (
            <button
              key={p.key}
              type="button"
              // Switching period exits any bar-chart drilldown so the
              // panel reflects the period aggregate, not the held month.
              onClick={() => {
                setPeriod(p.key);
                setSelectedMonth(null);
              }}
              className={clsx(
                "rounded-full px-2.5 py-1 text-xs font-medium transition-colors",
                p.key === period && !showingDrilldown
                  ? "border border-slate-200 bg-white text-slate-900 shadow-sm"
                  : "border border-transparent text-slate-500 hover:text-slate-700",
              )}
            >
              {p.label}
            </button>
          ))}
        </div>
      </div>
      {panelMeta ? (
        <AttributionPanel
          monthIso={selectedMonth ?? "aggregate"}
          monthMeta={panelMeta}
          rows={panelRows}
          reportingCcy={reportingCcy}
        />
      ) : null}
    </div>
  );
}
