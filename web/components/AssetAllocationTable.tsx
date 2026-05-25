"use client";

import { useTransition } from "react";
import { useRouter } from "next/navigation";
import clsx from "clsx";
import { setAssetClassFilter } from "@/lib/actions";
import { money, pct } from "@/lib/format";

interface Row {
  asset_class: string;
  nav: number;
  share: number; // 0..1
}

interface Props {
  rows: Row[];
  currentClasses: string[];
  reportingCcy?: string;
}

export default function AssetAllocationTable({
  rows,
  currentClasses,
  reportingCcy = "USD",
}: Props) {
  const router = useRouter();
  const [pending, startTransition] = useTransition();

  const totalNav = rows.reduce((s, r) => s + r.nav, 0);
  const maxShare = rows.reduce((m, r) => Math.max(m, r.share), 0);

  const currentSet = new Set(currentClasses);

  const onRowClick = (cls: string) => {
    // Toggle: if filter is exactly [cls], clear; otherwise set to [cls].
    // Lets the header sub-bar handle multi-class selection while the table
    // gives a one-click drill-in / drill-out.
    const isSoleSelection =
      currentClasses.length === 1 && currentClasses[0] === cls;
    const next = isSoleSelection ? [] : [cls];
    startTransition(async () => {
      await setAssetClassFilter(next);
      router.refresh();
    });
  };

  return (
    <div className="rounded-lg border border-slate-200 bg-white">
      <div className="flex items-baseline justify-between px-5 pt-4">
        <h2 className="text-base font-semibold text-slate-900">
          Asset allocation
        </h2>
        <span className="text-xs text-slate-500">
          {money(totalNav, reportingCcy)} across {rows.length} classes
        </span>
      </div>
      <div className="px-2 pb-3 pt-2">
        <table className="w-full text-sm">
          <thead className="text-xs uppercase tracking-wide text-slate-400">
            <tr>
              <th className="px-3 py-2 text-left font-medium">Asset class</th>
              <th className="px-3 py-2 text-right font-medium">NAV</th>
              <th className="px-3 py-2 text-right font-medium">Weight</th>
              <th className="px-3 py-2 text-left font-medium">{/* bar */}</th>
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td
                  colSpan={4}
                  className="px-3 py-6 text-center text-sm text-slate-400"
                >
                  No positions
                </td>
              </tr>
            ) : (
              rows.map(r => {
                const isSelected = currentSet.has(r.asset_class);
                const isHighlighted = currentSet.size > 0 && isSelected;
                const isDimmed = currentSet.size > 0 && !isSelected;
                const barWidth =
                  maxShare > 0 ? Math.max(2, (r.share / maxShare) * 100) : 0;
                return (
                  <tr
                    key={r.asset_class}
                    onClick={() => onRowClick(r.asset_class)}
                    className={clsx(
                      "cursor-pointer border-t border-slate-100 transition-colors",
                      isHighlighted && "bg-brand-tint/40",
                      isDimmed && "opacity-50 hover:opacity-100",
                      !isHighlighted && "hover:bg-slate-50",
                      pending && "pointer-events-none",
                    )}
                    title={
                      isSelected
                        ? `Click to clear filter`
                        : `Click to filter to ${r.asset_class}`
                    }
                  >
                    <td className="px-3 py-2 font-medium text-slate-800">
                      {r.asset_class}
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums text-slate-700">
                      {money(r.nav, reportingCcy)}
                    </td>
                    <td className="px-3 py-2 text-right tabular-nums text-slate-700">
                      {pct(r.share, 1)}
                    </td>
                    <td className="px-3 py-2">
                      <div
                        className="h-2 rounded bg-brand/80"
                        style={{ width: `${barWidth}%` }}
                      />
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
