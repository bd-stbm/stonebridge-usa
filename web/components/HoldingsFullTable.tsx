"use client";

import { useMemo, useState } from "react";
import clsx from "clsx";
import type { Position } from "@/lib/queries";
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
  | "unrealized_gl_local";

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
  { key: "unrealized_gl_local", label: "Unrealized G/L", align: "right" },
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

interface Props {
  positions: Position[];
  reportingCcy: string;
}

export default function HoldingsFullTable({ positions, reportingCcy }: Props) {
  const [search, setSearch] = useState("");
  const [assetClass, setAssetClass] = useState("");
  const [custodian, setCustodian] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("mv_reporting");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  const totalNav = useMemo(
    () => positions.reduce((s, p) => s + num(p.mv_reporting), 0),
    [positions],
  );

  const assetClassOptions = useMemo(
    () => uniqueValues(positions, "asset_class"),
    [positions],
  );
  const custodianOptions = useMemo(
    () => uniqueValues(positions, "custodian"),
    [positions],
  );

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    return positions.filter(p => {
      if (assetClass && p.asset_class !== assetClass) return false;
      if (custodian && p.custodian !== custodian) return false;
      if (!q) return true;
      const hay = [p.asset_name, p.ticker_masttro, p.isin]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
      return hay.includes(q);
    });
  }, [positions, search, assetClass, custodian]);

  const sorted = useMemo(() => {
    const arr = filtered.slice();
    arr.sort((a, b) => {
      if (sortKey === "weight") {
        const wa = totalNav > 0 ? num(a.mv_reporting) / totalNav : 0;
        const wb = totalNav > 0 ? num(b.mv_reporting) / totalNav : 0;
        return compareValues(wa, wb, sortDir);
      }
      // For numeric columns we coerce so sort respects numeric order even
      // when supabase-js returned them as strings.
      const numericKeys: SortKey[] = [
        "quantity",
        "price_local",
        "mv_reporting",
        "unrealized_gl_local",
      ];
      if (numericKeys.includes(sortKey)) {
        return compareValues(num(a[sortKey]), num(b[sortKey]), sortDir);
      }
      return compareValues(a[sortKey], b[sortKey], sortDir);
    });
    return arr;
  }, [filtered, sortKey, sortDir, totalNav]);

  const filteredNav = useMemo(
    () => filtered.reduce((s, p) => s + num(p.mv_reporting), 0),
    [filtered],
  );
  const filteredGl = useMemo(
    () => filtered.reduce((s, p) => s + num(p.unrealized_gl_local), 0),
    [filtered],
  );

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
    setAssetClass("");
    setCustodian("");
  };

  const hasActiveFilter = search || assetClass || custodian;

  const handleExportCsv = () => {
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
      "Unrealized G/L",
    ];
    const rows: (string | number | null)[][] = [header];
    for (const p of sorted) {
      const priceRaw = p.yf_price ?? p.price_local;
      const mvr = num(p.mv_reporting);
      const weight = totalNav > 0 ? mvr / totalNav : 0;
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
        num(p.unrealized_gl_local),
      ]);
    }
    const today = new Date().toISOString().slice(0, 10);
    downloadCsv(`holdings_${today}.csv`, rows);
  };

  return (
    <div className="space-y-4">
      <div className="rounded-lg border border-slate-200 bg-white p-4">
        <div className="grid grid-cols-1 gap-3 md:grid-cols-12">
          <div className="md:col-span-6">
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
            <label className="block text-xs font-medium text-slate-500">Asset class</label>
            <select
              value={assetClass}
              onChange={e => setAssetClass(e.target.value)}
              className="mt-1 block w-full rounded border border-slate-300 bg-white px-3 py-2 text-sm"
            >
              <option value="">All ({assetClassOptions.length})</option>
              {assetClassOptions.map(a => (
                <option key={a} value={a}>{a}</option>
              ))}
            </select>
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
                      {col.label}
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
                const price = priceRaw != null ? num(priceRaw) : null;
                const mvr = num(p.mv_reporting);
                const gl = num(p.unrealized_gl_local);
                const weight = totalNav > 0 ? mvr / totalNav : 0;
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
                      {price != null
                        ? priceFmt(price, p.local_ccy ?? reportingCcy)
                        : "—"}
                    </td>
                    <td className="px-4 py-3 text-right font-medium text-slate-900">
                      {money(mvr, reportingCcy)}
                    </td>
                    <td className="px-4 py-3 text-right text-slate-700">{pct(weight, 2)}</td>
                    <td
                      className={clsx(
                        "px-4 py-3 text-right font-medium",
                        gl >= 0 ? "text-emerald-600" : "text-rose-600",
                      )}
                    >
                      {money(gl, p.local_ccy ?? reportingCcy)}
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
                    filteredGl >= 0 ? "text-emerald-600" : "text-rose-600",
                  )}
                >
                  {money(filteredGl, reportingCcy)}
                </td>
              </tr>
            </tfoot>
          ) : null}
        </table>
      </div>
    </div>
  );
}
