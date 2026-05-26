"use client";

import { useMemo, useState } from "react";
import clsx from "clsx";
import type { Position } from "@/lib/queries";
import type { HoldingsGainPieces } from "@/lib/holdings-gains";
import { holdingsGainKey } from "@/lib/holdings-gains";
import { PERIODS, type PeriodKey } from "@/lib/returns";
import { money, pct, price as priceFmt } from "@/lib/format";

type SortKey =
  | "asset_name"
  | "ticker_masttro"
  | "trust_alias"
  | "asset_class"
  | "custodian"
  | "quantity"
  | "price_local"
  | "mv_reporting"
  | "weight"
  | "gain_dollars"
  | "gain_pct";

type SortDir = "asc" | "desc";

const COLUMNS: {
  key: SortKey;
  label: string;
  align: "left" | "right";
}[] = [
  { key: "asset_name", label: "Asset", align: "left" },
  { key: "ticker_masttro", label: "Ticker", align: "left" },
  { key: "asset_class", label: "Asset class", align: "left" },
  { key: "trust_alias", label: "Entity", align: "left" },
  { key: "custodian", label: "Custodian", align: "left" },
  { key: "quantity", label: "Quantity", align: "right" },
  { key: "price_local", label: "Price", align: "right" },
  { key: "mv_reporting", label: "Value", align: "right" },
  { key: "weight", label: "Weight", align: "right" },
  { key: "gain_dollars", label: "$ gain", align: "right" },
  { key: "gain_pct", label: "% gain", align: "right" },
];

// Postgres NUMERIC columns come back from supabase-js as strings (to
// preserve precision). Coerce before formatting so .toLocaleString picks up
// the number-format options instead of falling through to Object's no-op.
function num(v: unknown): number {
  if (v == null) return 0;
  const n = typeof v === "number" ? v : Number(v);
  return Number.isFinite(n) ? n : 0;
}

function uniqueValues(positions: Position[], key: keyof Position): string[] {
  const set = new Set<string>();
  for (const p of positions) {
    const v = p[key];
    if (typeof v === "string" && v.length > 0) set.add(v);
  }
  return Array.from(set).sort((a, b) => a.localeCompare(b));
}

function compareValues(a: unknown, b: unknown, dir: SortDir): number {
  if (a == null && b == null) return 0;
  if (a == null) return 1;
  if (b == null) return -1;
  const sign = dir === "asc" ? 1 : -1;
  if (typeof a === "number" && typeof b === "number") return (a - b) * sign;
  return String(a).localeCompare(String(b)) * sign;
}

function csvCell(v: unknown): string {
  if (v == null) return "";
  const s = typeof v === "number" ? String(v) : String(v);
  // Quote if the field contains comma, quote, or newline; double any
  // embedded quotes per RFC 4180.
  if (/[",\r\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function downloadCsv(filename: string, rows: (string | number | null)[][]) {
  const body = rows.map(r => r.map(csvCell).join(",")).join("\r\n");
  // BOM so Excel auto-detects UTF-8 (asset names sometimes carry
  // non-ASCII characters from Masttro).
  const blob = new Blob(["﻿" + body], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// Per-row gain pieces, normalised across all five periods so the
// downstream sort / format / totals code doesn't fork on period.
//   - 1D : start_mv = mv_reporting_yesterday, flows = 0, income = 0.
//          Modified Dietz reduces to (today - yesterday) / yesterday,
//          i.e. pure price move.
//   - MTD/YTD/6M/1Y : pulled from holdings_period_attribution. A
//          missing key (e.g. a row outside the RPC's universe) yields
//          null gain pieces and the row's $gain / %gain render as "—".
//
// "available" is false when the period's gain can't be computed at all
// — typically 1D where yfinance has no previous close for the security.
interface RowGain {
  available: boolean;
  start_mv: number;
  end_mv: number;
  flows: number;
  income: number;
  gain_dollars: number | null;
  gain_pct: number | null;
}

function rowGain(
  p: Position,
  period: PeriodKey,
  gains: Map<string, HoldingsGainPieces>,
): RowGain {
  const endMv = num(p.mv_reporting);

  if (period === "1d") {
    if (p.mv_reporting_yesterday == null) {
      return {
        available: false,
        start_mv: 0,
        end_mv: endMv,
        flows: 0,
        income: 0,
        gain_dollars: null,
        gain_pct: null,
      };
    }
    const startMv = num(p.mv_reporting_yesterday);
    const gainDollars = endMv - startMv;
    return {
      available: true,
      start_mv: startMv,
      end_mv: endMv,
      flows: 0,
      income: 0,
      gain_dollars: gainDollars,
      gain_pct: startMv !== 0 ? gainDollars / startMv : null,
    };
  }

  const pieces = gains.get(holdingsGainKey(period, p.account_node_id, p.security_id));
  if (!pieces) {
    return {
      available: false,
      start_mv: 0,
      end_mv: endMv,
      flows: 0,
      income: 0,
      gain_dollars: null,
      gain_pct: null,
    };
  }
  const startMv = pieces.start_mv;
  const flows = pieces.flows;
  const income = pieces.income;
  const gainDollars = (endMv - startMv) - flows + income;
  const denom = startMv + 0.5 * flows;
  return {
    available: true,
    start_mv: startMv,
    end_mv: endMv,
    flows,
    income,
    gain_dollars: gainDollars,
    gain_pct: denom !== 0 ? gainDollars / denom : null,
  };
}

function periodLabel(period: PeriodKey): string {
  return PERIODS.find(p => p.key === period)?.label ?? period.toUpperCase();
}

interface Props {
  positions: Position[];
  reportingCcy: string;
  periodGainsEntries: [string, HoldingsGainPieces][];
}

export default function HoldingsFullTable({
  positions,
  reportingCcy,
  periodGainsEntries,
}: Props) {
  const [search, setSearch] = useState("");
  const [custodian, setCustodian] = useState("");
  const [period, setPeriod] = useState<PeriodKey>("ytd");
  const [sortKey, setSortKey] = useState<SortKey>("mv_reporting");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  // Rehydrate the server-serialised entries into a Map once. The page
  // component sends an array of [key, value] tuples across the RSC
  // boundary because Maps aren't serialisable.
  const periodGains = useMemo(
    () => new Map(periodGainsEntries),
    [periodGainsEntries],
  );

  const totalNav = useMemo(
    () => positions.reduce((s, p) => s + num(p.mv_reporting), 0),
    [positions],
  );

  const custodianOptions = useMemo(
    () => uniqueValues(positions, "custodian"),
    [positions],
  );

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    return positions.filter(p => {
      if (custodian && p.custodian !== custodian) return false;
      if (!q) return true;
      const hay = [p.asset_name, p.ticker_masttro, p.isin]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
      return hay.includes(q);
    });
  }, [positions, search, custodian]);

  // Per-row gain pieces for the currently-selected period. Computed
  // once per (filtered, period) combination so the sort / render /
  // totals loops below don't recompute it three times.
  const rowGains = useMemo(() => {
    const map = new Map<Position, RowGain>();
    for (const p of filtered) map.set(p, rowGain(p, period, periodGains));
    return map;
  }, [filtered, period, periodGains]);

  const sorted = useMemo(() => {
    const arr = filtered.slice();
    arr.sort((a, b) => {
      if (sortKey === "weight") {
        const wa = totalNav > 0 ? num(a.mv_reporting) / totalNav : 0;
        const wb = totalNav > 0 ? num(b.mv_reporting) / totalNav : 0;
        return compareValues(wa, wb, sortDir);
      }
      if (sortKey === "gain_dollars" || sortKey === "gain_pct") {
        const ga = rowGains.get(a);
        const gb = rowGains.get(b);
        const va = sortKey === "gain_dollars" ? ga?.gain_dollars : ga?.gain_pct;
        const vb = sortKey === "gain_dollars" ? gb?.gain_dollars : gb?.gain_pct;
        return compareValues(va, vb, sortDir);
      }
      const numericKeys: SortKey[] = [
        "quantity",
        "price_local",
        "mv_reporting",
      ];
      if (numericKeys.includes(sortKey)) {
        return compareValues(num(a[sortKey]), num(b[sortKey]), sortDir);
      }
      return compareValues(a[sortKey], b[sortKey], sortDir);
    });
    return arr;
  }, [filtered, sortKey, sortDir, totalNav, rowGains]);

  const filteredNav = useMemo(
    () => filtered.reduce((s, p) => s + num(p.mv_reporting), 0),
    [filtered],
  );

  // Aggregate Modified Dietz across the filtered set, in the same shape
  // as the per-row math. Rows where the period gain isn't available
  // (e.g. 1D for a security without a yfinance previous close) are
  // excluded from the totals — including them with start_mv = 0 would
  // double-count the end MV as a gain.
  const totals = useMemo(() => {
    let startMv = 0;
    let endMv = 0;
    let flows = 0;
    let income = 0;
    let any = false;
    for (const p of filtered) {
      const g = rowGains.get(p);
      if (!g || !g.available) continue;
      startMv += g.start_mv;
      endMv += g.end_mv;
      flows += g.flows;
      income += g.income;
      any = true;
    }
    if (!any) {
      return { gain_dollars: null, gain_pct: null } as {
        gain_dollars: number | null;
        gain_pct: number | null;
      };
    }
    const gainDollars = (endMv - startMv) - flows + income;
    const denom = startMv + 0.5 * flows;
    return {
      gain_dollars: gainDollars,
      gain_pct: denom !== 0 ? gainDollars / denom : null,
    };
  }, [filtered, rowGains]);

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir(d => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir(key === "asset_name" || key === "ticker_masttro" || key === "trust_alias"
        || key === "asset_class" || key === "custodian" ? "asc" : "desc");
    }
  };

  const resetFilters = () => {
    setSearch("");
    setCustodian("");
  };

  const hasActiveFilter = search || custodian;

  const handleExportCsv = () => {
    const periodTag = periodLabel(period);
    const header = [
      "Asset",
      "Ticker",
      "ISIN",
      "Asset class",
      "Security type",
      "Entity",
      "Custodian",
      "Account",
      "Quantity",
      "Price",
      "Local CCY",
      "Value",
      "Reporting CCY",
      "Weight",
      `${periodTag} $ gain`,
      `${periodTag} % gain`,
    ];
    const rows: (string | number | null)[][] = [header];
    for (const p of sorted) {
      const priceRaw = p.yf_price ?? p.price_local;
      const mvr = num(p.mv_reporting);
      const weight = totalNav > 0 ? mvr / totalNav : 0;
      const g = rowGains.get(p);
      rows.push([
        p.asset_name ?? "",
        p.ticker_masttro ?? "",
        p.isin ?? "",
        p.asset_class ?? "",
        p.security_type ?? "",
        p.trust_alias ?? "",
        p.custodian ?? "",
        p.account_alias ?? "",
        num(p.quantity),
        priceRaw != null ? num(priceRaw) : "",
        p.local_ccy ?? "",
        mvr,
        p.reporting_ccy ?? reportingCcy,
        weight,
        g?.gain_dollars ?? "",
        g?.gain_pct ?? "",
      ]);
    }
    const today = new Date().toISOString().slice(0, 10);
    downloadCsv(`holdings_${today}_${period}.csv`, rows);
  };

  return (
    <div className="space-y-4">
      <div className="rounded-lg border border-slate-200 bg-white p-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-3">
            <span className="text-xs font-medium uppercase tracking-wide text-slate-500">
              Gain period
            </span>
            <div className="flex gap-1">
              {PERIODS.map(p => (
                <button
                  key={p.key}
                  type="button"
                  onClick={() => setPeriod(p.key)}
                  className={clsx(
                    "rounded px-2 py-0.5 text-xs font-medium",
                    p.key === period
                      ? "bg-brand text-white"
                      : "text-slate-500 hover:bg-slate-100",
                  )}
                >
                  {p.label}
                </button>
              ))}
            </div>
          </div>
        </div>
        <div className="mt-3 grid grid-cols-1 gap-3 md:grid-cols-12">
          <div className="md:col-span-9">
            <label className="block text-xs font-medium text-slate-500">
              Search
            </label>
            <input
              type="text"
              value={search}
              onChange={e => setSearch(e.target.value)}
              placeholder="Asset, ticker, ISIN…"
              className="mt-1 block w-full rounded border border-slate-300 px-3 py-2 text-sm focus:border-slate-500 focus:outline-none"
            />
          </div>
          <div className="md:col-span-3">
            <label className="block text-xs font-medium text-slate-500">Custodian</label>
            <select
              value={custodian}
              onChange={e => setCustodian(e.target.value)}
              className="mt-1 block w-full rounded border border-slate-300 bg-white px-3 py-2 text-sm"
            >
              <option value="">All ({custodianOptions.length})</option>
              {custodianOptions.map(c => (
                <option key={c} value={c}>{c}</option>
              ))}
            </select>
          </div>
        </div>
        <div className="mt-3 flex items-center justify-between text-xs text-slate-500">
          <div>
            Showing <span className="font-medium text-slate-700">{filtered.length}</span> of{" "}
            <span className="font-medium text-slate-700">{positions.length}</span> positions
            {" · "}
            <span className="font-medium text-slate-700">{money(filteredNav, reportingCcy)}</span>
            {hasActiveFilter && totalNav > 0 ? (
              <> ({pct(filteredNav / totalNav, 1)} of total)</>
            ) : null}
          </div>
          <div className="flex items-center gap-3">
            {hasActiveFilter ? (
              <button
                type="button"
                onClick={resetFilters}
                className="text-slate-600 underline hover:text-slate-900"
              >
                Clear filters
              </button>
            ) : null}
            <button
              type="button"
              onClick={handleExportCsv}
              disabled={sorted.length === 0}
              className="rounded border border-slate-300 bg-white px-2.5 py-1 text-xs font-medium text-slate-700 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50"
            >
              Export CSV
            </button>
          </div>
        </div>
      </div>

      <div className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
        <table className="min-w-full text-sm">
          <thead className="bg-slate-50 text-xs uppercase tracking-wide text-slate-500">
            <tr>
              {COLUMNS.map(col => {
                const isSorted = sortKey === col.key;
                const label =
                  col.key === "gain_dollars"
                    ? `${periodLabel(period)} $ gain`
                    : col.key === "gain_pct"
                      ? `${periodLabel(period)} % gain`
                      : col.label;
                return (
                  <th
                    key={col.key}
                    className={clsx(
                      "px-4 py-3 select-none cursor-pointer hover:bg-slate-100",
                      col.align === "right" ? "text-right" : "text-left",
                    )}
                    onClick={() => handleSort(col.key)}
                  >
                    <span className={clsx("inline-flex items-center gap-1",
                      col.align === "right" && "justify-end")}>
                      {label}
                      {isSorted ? (
                        <span className="text-slate-400">
                          {sortDir === "asc" ? "▲" : "▼"}
                        </span>
                      ) : null}
                    </span>
                  </th>
                );
              })}
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {sorted.length === 0 ? (
              <tr>
                <td colSpan={COLUMNS.length} className="px-4 py-8 text-center text-sm text-slate-500">
                  No positions match the current filters.
                </td>
              </tr>
            ) : (
              sorted.map((p, i) => {
                const qty = num(p.quantity);
                // Prefer yfinance price when present so the Price column
                // ties out with the Value column (which is mv_reporting,
                // already refreshed). Falls back to Masttro for securities
                // yfinance doesn't cover.
                const priceRaw = p.yf_price ?? p.price_local;
                const priceNum = priceRaw != null ? num(priceRaw) : null;
                const mvr = num(p.mv_reporting);
                const weight = totalNav > 0 ? mvr / totalNav : 0;
                const g = rowGains.get(p);
                return (
                  <tr key={i} className="hover:bg-slate-50">
                    <td className="px-4 py-3 font-medium text-slate-900">
                      {p.asset_name}
                      {p.security_type ? (
                        <div className="text-xs font-normal text-slate-400">
                          {p.security_type}
                        </div>
                      ) : null}
                    </td>
                    <td className="px-4 py-3 text-slate-600">{p.ticker_masttro ?? "—"}</td>
                    <td className="px-4 py-3 text-slate-600">{p.asset_class ?? "—"}</td>
                    <td className="px-4 py-3 text-slate-600">{p.trust_alias ?? "—"}</td>
                    <td className="px-4 py-3 text-slate-600">{p.custodian ?? "—"}</td>
                    <td className="px-4 py-3 text-right text-slate-700">
                      {qty.toLocaleString(undefined, { maximumFractionDigits: 4 })}
                    </td>
                    <td className="px-4 py-3 text-right text-slate-700">
                      {priceNum != null
                        ? priceFmt(priceNum, p.local_ccy ?? reportingCcy)
                        : "—"}
                    </td>
                    <td className="px-4 py-3 text-right font-medium text-slate-900">
                      {money(mvr, reportingCcy)}
                    </td>
                    <td className="px-4 py-3 text-right text-slate-700">{pct(weight, 2)}</td>
                    <td
                      className={clsx(
                        "px-4 py-3 text-right font-medium",
                        g?.gain_dollars == null
                          ? "text-slate-400"
                          : g.gain_dollars >= 0
                            ? "text-emerald-600"
                            : "text-rose-600",
                      )}
                    >
                      {g?.gain_dollars != null
                        ? money(g.gain_dollars, reportingCcy)
                        : "—"}
                    </td>
                    <td
                      className={clsx(
                        "px-4 py-3 text-right font-medium",
                        g?.gain_pct == null
                          ? "text-slate-400"
                          : g.gain_pct >= 0
                            ? "text-emerald-600"
                            : "text-rose-600",
                      )}
                    >
                      {g?.gain_pct != null ? pct(g.gain_pct, 2) : "—"}
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
          {sorted.length > 0 ? (
            <tfoot className="bg-slate-50 text-sm">
              <tr className="border-t border-slate-200">
                <td colSpan={5} className="px-4 py-3 text-xs font-medium uppercase tracking-wide text-slate-500">
                  Totals ({filtered.length} {filtered.length === 1 ? "position" : "positions"})
                </td>
                <td className="px-4 py-3" />
                <td className="px-4 py-3" />
                <td className="px-4 py-3 text-right font-semibold text-slate-900">
                  {money(filteredNav, reportingCcy)}
                </td>
                <td className="px-4 py-3 text-right text-slate-700">
                  {totalNav > 0 ? pct(filteredNav / totalNav, 1) : "—"}
                </td>
                <td
                  className={clsx(
                    "px-4 py-3 text-right font-semibold",
                    totals.gain_dollars == null
                      ? "text-slate-400"
                      : totals.gain_dollars >= 0
                        ? "text-emerald-600"
                        : "text-rose-600",
                  )}
                >
                  {totals.gain_dollars != null
                    ? money(totals.gain_dollars, reportingCcy)
                    : "—"}
                </td>
                <td
                  className={clsx(
                    "px-4 py-3 text-right font-semibold",
                    totals.gain_pct == null
                      ? "text-slate-400"
                      : totals.gain_pct >= 0
                        ? "text-emerald-600"
                        : "text-rose-600",
                  )}
                >
                  {totals.gain_pct != null ? pct(totals.gain_pct, 2) : "—"}
                </td>
              </tr>
            </tfoot>
          ) : null}
        </table>
      </div>
    </div>
  );
}
