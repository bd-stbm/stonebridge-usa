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
  // Default to the most recent month that has both a return and attribution
  // data. Lets the panel render immediately rather than leaving empty space.
  const defaultMonth = useMemo(() => {
    for (let i = monthlyReturns.length - 1; i >= 0; i--) {
      const m = monthlyReturns[i].month;
      if (attributionByMonth[m]?.length) return m;
    }
    return monthlyReturns[monthlyReturns.length - 1]?.month ?? null;
  }, [monthlyReturns, attributionByMonth]);

  const [selectedMonth, setSelectedMonth] = useState<string | null>(defaultMonth);

  const selectedRow =
    selectedMonth != null
      ? monthlyReturns.find(r => r.month === selectedMonth) ?? null
      : null;
  const selectedAttribution =
    selectedMonth != null ? attributionByMonth[selectedMonth] ?? [] : [];

  return (
    <div className="space-y-4">
      <MonthlyReturnsBar
        rows={monthlyReturns}
        selectedMonth={selectedMonth}
        onSelect={m => setSelectedMonth(m || null)}
      />
      {selectedMonth && selectedRow ? (
        <AttributionPanel
          monthIso={selectedMonth}
          monthMeta={{
            label: selectedRow.label,
            start_nav: selectedRow.start_nav,
            end_nav: selectedRow.end_nav,
            gain: selectedRow.gain,
            return_pct: selectedRow.return_pct,
          }}
          rows={selectedAttribution}
          reportingCcy={reportingCcy}
        />
      ) : null}
    </div>
  );
}
