"use client";

import { useMemo, useState } from "react";
import MonthlyReturnsBar, { type MonthlyReturnRow } from "./MonthlyReturnsBar";
import AttributionPanel from "./AttributionPanel";
import type { MonthlyAttributionRow } from "@/lib/queries";

interface Props {
  monthlyReturns: MonthlyReturnRow[];
  attributionByMonth: Record<string, MonthlyAttributionRow[]>;
  reportingCcy?: string;
}

export default function MonthlyAttributionSection({
  monthlyReturns,
  attributionByMonth,
  reportingCcy = "USD",
}: Props) {
  // Default: no month selected → render the aggregated "last N months"
  // contributors/detractors. Clicking a bar drills into that one month;
  // Clear button on the bar chart returns to the aggregated view.
  const [selectedMonth, setSelectedMonth] = useState<string | null>(null);

  // ---- Aggregated rollup over the full bar-chart window ----
  // Build per-security totals: start_mv = mv at start of earliest month
  // we have for the security, end_mv = mv at end of latest month, flows
  // / income / gain summed. Months a security never appeared in (e.g.
  // small consistent contributors filtered out by the server-side top-N)
  // are missing — see RPC commit 4431700; in practice the >$100k movers
  // we display are always in-window.
  const aggregatedAttribution = useMemo<MonthlyAttributionRow[]>(() => {
    type Bucket = MonthlyAttributionRow;
    const byId = new Map<number, Bucket>();
    // Walk months chronologically so the first encounter of a security
    // sets its 12M-window start_mv, the last encounter sets end_mv.
    const sortedMonths = monthlyReturns.map(r => r.month);
    for (const month of sortedMonths) {
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
  }, [monthlyReturns, attributionByMonth]);

  // KPI / meta block for the aggregated view: compound the monthly
  // portfolio returns (chain-link), sum the monthly gains, anchor the
  // start NAV to the first month's start and end NAV to the last
  // month's end.
  const aggregatedMeta = useMemo(() => {
    if (monthlyReturns.length === 0) return null;
    const first = monthlyReturns[0];
    const last = monthlyReturns[monthlyReturns.length - 1];
    let cumulative = 1;
    let totalGain = 0;
    let anyReturn = false;
    for (const m of monthlyReturns) {
      if (m.return_pct != null) {
        cumulative *= 1 + m.return_pct;
        anyReturn = true;
      }
      if (m.gain != null) totalGain += m.gain;
    }
    return {
      label: `Last ${monthlyReturns.length}M`,
      start_nav: first.start_nav,
      end_nav: last.end_nav,
      gain: totalGain,
      return_pct: anyReturn ? cumulative - 1 : null,
    };
  }, [monthlyReturns]);

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
