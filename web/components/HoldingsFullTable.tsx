"use client";

import { useMemo, useState } from "react";
import clsx from "clsx";
import type { Position } from "@/lib/queries";
import type { HoldingsGainPieces } from "@/lib/holdings-gains";
import { holdingsGainKey } from "@/lib/holdings-gains";
import KpiTile from "@/components/KpiTile";
import { PERIODS, type PeriodKey } from "@/lib/returns";
import { money, pct, price as priceFmt } from "@/lib/format";

type SortKey =
  | "asset_name"
  | "ticker_masttro"
  | "trust_alias"
  | "asset_class"
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

// Row grain after aggregation. Positions are pre-grouped by
// (security_id × trust_alias) so multiple accounts — across any
// custodian — under the same entity collapse into one row.
//
// constituents[] records the underlying (account_node_id, security_id)
// pairs so per-period gain pieces (from holdings_period_attribution,
// which is at account×security grain) can be summed up.
interface Holding {
  key: string;
  security_id: number;
  asset_name: string;
  asset_class: string | null;
  security_type: string | null;
  ticker_masttro: string | null;
  isin: string | null;
  local_ccy: string | null;
  reporting_ccy: string;
  yf_price: number | null;
  price_local: number | null;
  trust_alias: string | null;
  quantity: number;
  mv_reporting: number;
  // null when any constituent lacks a yfinance previous-close. pricing
  // refresh is keyed on security_id alone, so in practice constituents
  // for a single security share the same null-state — but we guard
  // against the mixed case anyway by collapsing to null on the first
  // missing constituent.
  mv_reporting_yesterday: number | null;
  account_count: number;
  constituents: { account_node_id: string; security_id: number }[];
}

function aggregateAcrossAccounts(positions: Position[]): Holding[] {
  const map = new Map<string, Holding>();
  for (const p of positions) {
    const key = `${p.security_id}|${p.trust_alias ?? ""}`;
    const existing = map.get(key);
    const py = p.mv_reporting_yesterday;
    if (existing) {
      existing.quantity += num(p.quantity);
      existing.mv_reporting += num(p.mv_reporting);
      if (existing.mv_reporting_yesterday == null || py == null) {
        existing.mv_reporting_yesterday = null;
      } else {
        existing.mv_reporting_yesterday += num(py);
      }
      existing.account_count += 1;
      existing.constituents.push({
        account_node_id: p.account_node_id,
        security_id: p.security_id,
      });
    } else {
      map.set(key, {
        key,
        security_id: p.security_id,
        asset_name: p.asset_name,
        asset_class: p.asset_class,
        security_type: p.security_type,
        ticker_masttro: p.ticker_masttro,
        isin: p.isin,
        local_ccy: p.local_ccy,
        reporting_ccy: p.reporting_ccy,
        yf_price: p.yf_price,
        price_local: p.price_local,
        trust_alias: p.trust_alias,
        quantity: num(p.quantity),
        mv_reporting: num(p.mv_reporting),
        mv_reporting_yesterday: py != null ? num(py) : null,
        account_count: 1,
        constituents: [
          { account_node_id: p.account_node_id, security_id: p.security_id },
        ],
      });
    }
  }
  return Array.from(map.values());
}

// Per-row gain pieces, normalised across all five periods so the
// downstream sort / format / totals code doesn't fork on period.
//   - 1D : start_mv = mv_reporting_yesterday (already aggregated to
//          the row's grain), flows = 0, income = 0. Modified Dietz
//          reduces to (today - yesterday) / yesterday, i.e. pure
//          price move.
//   - MTD/YTD/6M/1Y : start_mv / flows / income summed across the
//          row's constituents from the holdings_period_attribution
//          map (keyed at account × security).
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
  h: Holding,
  period: PeriodKey,
  gains: Map<string, HoldingsGainPieces>,
): RowGain {
  const endMv = h.mv_reporting;

  if (period === "1d") {
    if (h.mv_reporting_yesterday == null) {
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
    const startMv = h.mv_reporting_yesterday;
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

  let startMv = 0;
  let flows = 0;
  let income = 0;
  let anyFound = false;
  for (const c of h.constituents) {
    const pieces = gains.get(
      holdingsGainKey(period, c.account_node_id, c.security_id),
    );
    if (!pieces) continue;
    startMv += pieces.start_mv;
    flows += pieces.flows;
    income += pieces.income;
    anyFound = true;
  }
  if (!anyFound) {
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

// Aggregate Modified Dietz across a set of (already gain-computed) rows.
// Rows whose period gain isn't available (e.g. 1D without a yfinance
// previous close) are skipped — including them with start_mv = 0 would
// count their end MV as pure gain. Pulled out of the component so the
// headline KPI (whole in-scope portfolio) and the footer (whatever's
// currently shown) can aggregate over different row sets with one path.
function aggregateTotals(
  rows: Holding[],
  rowGains: Map<Holding, RowGain>,
): { gain_dollars: number | null; gain_pct: number | null } {
  let startMv = 0;
  let endMv = 0;
  let flows = 0;
  let income = 0;
  let any = false;
  for (const h of rows) {
    const g = rowGains.get(h);
    if (!g || !g.available) continue;
    startMv += g.start_mv;
    endMv += g.end_mv;
    flows += g.flows;
    income += g.income;
    any = true;
  }
  if (!any) return { gain_dollars: null, gain_pct: null };
  const gainDollars = (endMv - startMv) - flows + income;
  const denom = startMv + 0.5 * flows;
  return {
    gain_dollars: gainDollars,
    gain_pct: denom !== 0 ? gainDollars / denom : null,
  };
}

interface Props {
  positions: Position[];
  reportingCcy: string;
  periodGainsEntries: [string, HoldingsGainPieces][];
  // Top-of-page KPI inputs. Computed server-side from the same
  // `positions` array so we don't recompute them here, but kept as
  // separate props so the KPI strip can render before the heavy
  // per-row aggregation runs.
  nav: number;
  positionsCount: number;
  entitiesCount: number;
  assetClassesCount: number;
}

export default function HoldingsFullTable({
  positions,
  reportingCcy,
  periodGainsEntries,
  nav,
  positionsCount,
  entitiesCount,
  assetClassesCount,
}: Props) {
  const [period, setPeriod] = useState<PeriodKey>("ytd");
  const [sortKey, setSortKey] = useState<SortKey>("mv_reporting");
  const [sortDir, setSortDir] = useState<SortDir>("desc");
  // Hide closed/exited positions (e.g. a sold-out holding lingering at
  // qty 0 in the latest snapshot). Both quantity and value zero = closed;
  // cash has null quantity (→ 0) but non-zero value, so it stays.
  const [openOnly, setOpenOnly] = useState(true);
  // Free-text search over security identity (name / ticker / ISIN).
  const [query, setQuery] = useState("");

  // Rehydrate the server-serialised entries into a Map once. The page
  // component sends an array of [key, value] tuples across the RSC
  // boundary because Maps aren't serialisable.
  const periodGains = useMemo(
    () => new Map(periodGainsEntries),
    [periodGainsEntries],
  );

  const holdings = useMemo(
    () => aggregateAcrossAccounts(positions),
    [positions],
  );

  // Closed positions = both quantity and value zero. Cash keeps its
  // value so it survives the filter.
  const openHoldings = useMemo(
    () =>
      openOnly
        ? holdings.filter(h => h.quantity !== 0 || h.mv_reporting !== 0)
        : holdings,
    [holdings, openOnly],
  );

  // Apply the search on top of the open/closed filter. Matches a
  // case-insensitive substring against the security's name, Masttro
  // ticker, and ISIN. Folding it in here (rather than only hiding rows
  // at render time) means the row count, footer totals and CSV export
  // all reflect the current search.
  const visibleHoldings = useMemo(() => {
    const needle = query.trim().toLowerCase();
    if (!needle) return openHoldings;
    return openHoldings.filter(
      h =>
        (h.asset_name?.toLowerCase().includes(needle) ?? false) ||
        (h.ticker_masttro?.toLowerCase().includes(needle) ?? false) ||
        (h.isin?.toLowerCase().includes(needle) ?? false),
    );
  }, [openHoldings, query]);

  // Total value of what's actually shown — reflects both the open/closed
  // filter and the search. Drives the summary line, the footer Totals row,
  // and the per-row weight (so weights renormalise to the visible set and
  // the footer's 100% stays consistent). Closed rows carry ~0 value, so
  // with no search this equals the full open-portfolio NAV as before.
  const totalNav = useMemo(
    () => visibleHoldings.reduce((s, h) => s + h.mv_reporting, 0),
    [visibleHoldings],
  );

  // Per-row gain pieces for the currently-selected period. Computed over
  // the full open set (the search-independent superset of visibleHoldings)
  // so the headline KPI can aggregate the whole in-scope portfolio while
  // the table/footer aggregate the searched subset — both reading the
  // same map.
  const rowGains = useMemo(() => {
    const map = new Map<Holding, RowGain>();
    for (const h of openHoldings) map.set(h, rowGain(h, period, periodGains));
    return map;
  }, [openHoldings, period, periodGains]);

  const sorted = useMemo(() => {
    const arr = visibleHoldings.slice();
    arr.sort((a, b) => {
      if (sortKey === "weight") {
        const wa = totalNav > 0 ? a.mv_reporting / totalNav : 0;
        const wb = totalNav > 0 ? b.mv_reporting / totalNav : 0;
        return compareValues(wa, wb, sortDir);
      }
      if (sortKey === "gain_dollars" || sortKey === "gain_pct") {
        const ga = rowGains.get(a);
        const gb = rowGains.get(b);
        const va = sortKey === "gain_dollars" ? ga?.gain_dollars : ga?.gain_pct;
        const vb = sortKey === "gain_dollars" ? gb?.gain_dollars : gb?.gain_pct;
        return compareValues(va, vb, sortDir);
      }
      if (
        sortKey === "quantity" ||
        sortKey === "price_local" ||
        sortKey === "mv_reporting"
      ) {
        const va =
          sortKey === "price_local"
            ? a.yf_price ?? a.price_local
            : a[sortKey];
        const vb =
          sortKey === "price_local"
            ? b.yf_price ?? b.price_local
            : b[sortKey];
        return compareValues(num(va), num(vb), sortDir);
      }
      return compareValues(a[sortKey], b[sortKey], sortDir);
    });
    return arr;
  }, [visibleHoldings, sortKey, sortDir, totalNav, rowGains]);

  // Footer "Totals" row — aggregated over the rows actually shown, so it
  // tracks both the open/closed filter and the search box.
  const totals = useMemo(
    () => aggregateTotals(visibleHoldings, rowGains),
    [visibleHoldings, rowGains],
  );

  // Headline KPI gain — aggregated over the whole in-scope (open)
  // portfolio, deliberately independent of the search box so the top
  // tiles move only with the global filters (trust / account / asset
  // class) and the period, not with what's typed into search.
  const headlineTotals = useMemo(
    () => aggregateTotals(openHoldings, rowGains),
    [openHoldings, rowGains],
  );

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir(d => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir(key === "asset_name" || key === "ticker_masttro" || key === "trust_alias"
        || key === "asset_class" ? "asc" : "desc");
    }
  };

  const handleExportCsv = () => {
    const periodTag = periodLabel(period);
    const header = [
      "Asset",
      "Ticker",
      "ISIN",
      "Asset class",
      "Security type",
      "Entity",
      "Accounts",
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
    for (const h of sorted) {
      const priceRaw = h.yf_price ?? h.price_local;
      const weight = totalNav > 0 ? h.mv_reporting / totalNav : 0;
      const g = rowGains.get(h);
      rows.push([
        h.asset_name ?? "",
        h.ticker_masttro ?? "",
        h.isin ?? "",
        h.asset_class ?? "",
        h.security_type ?? "",
        h.trust_alias ?? "",
        h.account_count,
        h.quantity,
        priceRaw != null ? num(priceRaw) : "",
        h.local_ccy ?? "",
        h.mv_reporting,
        h.reporting_ccy ?? reportingCcy,
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
      {/* KPI strip — represents the whole in-scope portfolio (global
          filters + period), independent of the in-table search. The
          second tile re-renders with the selected period. Replaces the
          previous "Unrealized G/L" KPI which relied on Masttro's
          totalCost (unreliable for accounts that have been rebalanced —
          see Cornerstone Super where it showed a spurious ~AUD 14M loss). */}
      <div className="mb-6 grid grid-cols-2 gap-4 md:grid-cols-4">
        <KpiTile label="NAV" value={money(nav, reportingCcy)} />
        <KpiTile
          label={`${periodLabel(period)} gain`}
          value={
            headlineTotals.gain_dollars != null
              ? money(headlineTotals.gain_dollars, reportingCcy)
              : "—"
          }
          tone={
            headlineTotals.gain_dollars == null
              ? "default"
              : headlineTotals.gain_dollars >= 0
                ? "positive"
                : "negative"
          }
          hint={
            headlineTotals.gain_pct != null
              ? pct(headlineTotals.gain_pct, 2)
              : undefined
          }
        />
        <KpiTile label="Positions" value={positionsCount.toString()} />
        <KpiTile
          label="Asset classes"
          value={assetClassesCount.toString()}
          hint={`${entitiesCount} entities`}
        />
      </div>

      <div className="rounded-lg border border-slate-200 bg-white p-4">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div className="flex items-center gap-3">
            <span className="text-xs font-medium uppercase tracking-wide text-slate-500">
              Gain period
            </span>
            <div className="inline-flex rounded-md border border-slate-200 bg-slate-50 p-0.5">
              {PERIODS.map(p => (
                <button
                  key={p.key}
                  type="button"
                  onClick={() => setPeriod(p.key)}
                  className={clsx(
                    "rounded px-2.5 py-1 text-xs font-medium transition",
                    p.key === period
                      ? "bg-white text-brand shadow-sm"
                      : "text-slate-500 hover:text-slate-700",
                  )}
                >
                  {p.label}
                </button>
              ))}
            </div>
          </div>
          <div className="flex items-center gap-3">
            <div className="relative w-full sm:w-80">
              <svg
                className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400"
                viewBox="0 0 20 20"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
                aria-hidden="true"
              >
                <circle cx="9" cy="9" r="6" />
                <path d="m17 17-3.5-3.5" strokeLinecap="round" />
              </svg>
              <input
                type="search"
                value={query}
                onChange={e => setQuery(e.target.value)}
                placeholder="Search security, ticker, ISIN…"
                aria-label="Search holdings"
                className="w-full rounded-md border border-slate-300 bg-white py-1.5 pl-9 pr-3 text-sm text-slate-700 placeholder:text-slate-400 focus:border-brand focus:outline-none focus:ring-1 focus:ring-brand"
              />
            </div>
            <button
              type="button"
              role="switch"
              aria-checked={openOnly}
              onClick={() => setOpenOnly(v => !v)}
              className="group inline-flex shrink-0 items-center gap-2 rounded-md border border-slate-300 bg-white px-3 py-1.5 text-xs font-medium text-slate-700 transition hover:bg-slate-50"
            >
              <span>Open positions only</span>
              <span
                className={clsx(
                  "relative h-4 w-7 rounded-full transition-colors",
                  openOnly ? "bg-brand" : "bg-slate-300",
                )}
              >
                <span
                  className={clsx(
                    "absolute top-0.5 h-3 w-3 rounded-full bg-white shadow-sm transition-all",
                    openOnly ? "left-3.5" : "left-0.5",
                  )}
                />
              </span>
              <span
                className={clsx(
                  "w-6 text-left text-[10px] font-semibold uppercase tracking-wide",
                  openOnly ? "text-brand" : "text-slate-400",
                )}
              >
                {openOnly ? "On" : "Off"}
              </span>
            </button>
          </div>
        </div>
      </div>

      <div className="rounded-lg border border-slate-200 bg-white">
        <div className="flex items-center justify-end border-b border-slate-100 px-4 py-2.5">
          <button
            type="button"
            onClick={handleExportCsv}
            disabled={sorted.length === 0}
            className="inline-flex items-center gap-1.5 rounded-md border border-slate-300 bg-white px-3 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50"
          >
            <svg
              className="h-3.5 w-3.5"
              viewBox="0 0 20 20"
              fill="none"
              stroke="currentColor"
              strokeWidth="1.8"
              aria-hidden="true"
            >
              <path
                d="M10 3v9m0 0 3.25-3.25M10 12 6.75 8.75"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
              <path
                d="M4 14v2a1.5 1.5 0 0 0 1.5 1.5h9A1.5 1.5 0 0 0 16 16v-2"
                strokeLinecap="round"
              />
            </svg>
            Export CSV
          </button>
        </div>
        <div className="overflow-x-auto">
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
                      // Keep the Asset column pinned while the rest scrolls
                      // horizontally on narrow screens.
                      col.key === COLUMNS[0].key &&
                        "sticky left-0 z-20 bg-slate-50",
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
                  {query.trim()
                    ? `No holdings match “${query.trim()}”.`
                    : "No positions in scope."}
                </td>
              </tr>
            ) : (
              sorted.map(h => {
                // Prefer yfinance price when present so the Price column
                // ties out with the Value column (which is mv_reporting,
                // already refreshed). Falls back to Masttro for securities
                // yfinance doesn't cover.
                const priceRaw = h.yf_price ?? h.price_local;
                const priceNum = priceRaw != null ? num(priceRaw) : null;
                const weight = totalNav > 0 ? h.mv_reporting / totalNav : 0;
                const g = rowGains.get(h);
                return (
                  <tr key={h.key} className="group hover:bg-slate-50">
                    <td className="sticky left-0 z-10 bg-white px-4 py-3 font-medium text-slate-900 group-hover:bg-slate-50">
                      {h.asset_name}
                      {h.security_type || h.account_count > 1 ? (
                        <div className="text-xs font-normal text-slate-400">
                          {h.security_type}
                          {h.security_type && h.account_count > 1 ? " · " : ""}
                          {h.account_count > 1 ? `${h.account_count} accounts` : ""}
                        </div>
                      ) : null}
                    </td>
                    <td className="px-4 py-3 text-slate-600">{h.ticker_masttro ?? "—"}</td>
                    <td className="px-4 py-3 text-slate-600">{h.asset_class ?? "—"}</td>
                    <td className="px-4 py-3 text-slate-600">{h.trust_alias ?? "—"}</td>
                    <td className="px-4 py-3 text-right text-slate-700">
                      {h.quantity.toLocaleString(undefined, { maximumFractionDigits: 4 })}
                    </td>
                    <td className="px-4 py-3 text-right text-slate-700">
                      {priceNum != null
                        ? priceFmt(priceNum, h.local_ccy ?? reportingCcy)
                        : "—"}
                    </td>
                    <td className="px-4 py-3 text-right font-medium text-slate-900">
                      {money(h.mv_reporting, reportingCcy)}
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
                <td colSpan={4} className="sticky left-0 z-10 bg-slate-50 px-4 py-3 text-xs font-medium uppercase tracking-wide text-slate-500">
                  Totals ({visibleHoldings.length} {visibleHoldings.length === 1 ? "holding" : "holdings"})
                </td>
                <td className="px-4 py-3" />
                <td className="px-4 py-3" />
                <td className="px-4 py-3 text-right font-semibold text-slate-900">
                  {money(totalNav, reportingCcy)}
                </td>
                <td className="px-4 py-3 text-right text-slate-700">
                  {totalNav > 0 ? pct(1, 1) : "—"}
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
    </div>
  );
}
