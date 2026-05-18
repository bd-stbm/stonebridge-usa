"use client";

import { useMemo, useState } from "react";
import clsx from "clsx";
import type { Position } from "@/lib/queries";
import { money, pct } from "@/lib/format";

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
  { key: "trust_alias", label: "Trust", align: "left" },
  { key: "custodian", label: "Custodian", align: "left" },
  { key: "quantity", label: "Quantity", align: "right" },
  { key: "price_local", label: "Price", align: "right" },
  { key: "mv_reporting", label: "Value", align: "right" },
  { key: "weight", label: "Weight", align: "right" },
  { key: "unrealized_gl_local", label: "Unrealized G/L", align: "right" },
];

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

interface Props {
  positions: Position[];
  reportingCcy: string;
}

export default function HoldingsFullTable({ positions, reportingCcy }: Props) {
  const [search, setSearch] = useState("");
  const [trust, setTrust] = useState("");
  const [assetClass, setAssetClass] = useState("");
  const [custodian, setCustodian] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("mv_reporting");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  const totalNav = useMemo(
    () => positions.reduce((s, p) => s + (p.mv_reporting ?? 0), 0),
    [positions],
  );

  const trustOptions = useMemo(() => uniqueValues(positions, "trust_alias"), [positions]);
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
      if (trust && p.trust_alias !== trust) return false;
      if (assetClass && p.asset_class !== assetClass) return false;
      if (custodian && p.custodian !== custodian) return false;
      if (!q) return true;
      const hay = [p.asset_name, p.ticker_masttro, p.isin]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
      return hay.includes(q);
    });
  }, [positions, search, trust, assetClass, custodian]);

  const sorted = useMemo(() => {
    const arr = filtered.slice();
    arr.sort((a, b) => {
      if (sortKey === "weight") {
        const wa = totalNav > 0 ? (a.mv_reporting ?? 0) / totalNav : 0;
        const wb = totalNav > 0 ? (b.mv_reporting ?? 0) / totalNav : 0;
        return compareValues(wa, wb, sortDir);
      }
      return compareValues(a[sortKey], b[sortKey], sortDir);
    });
    return arr;
  }, [filtered, sortKey, sortDir, totalNav]);

  const filteredNav = useMemo(
    () => filtered.reduce((s, p) => s + (p.mv_reporting ?? 0), 0),
    [filtered],
  );
  const filteredGl = useMemo(
    () => filtered.reduce((s, p) => s + (p.unrealized_gl_local ?? 0), 0),
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
    setTrust("");
    setAssetClass("");
    setCustodian("");
  };

  const hasActiveFilter = search || trust || assetClass || custodian;

  return (
    <div className="space-y-4">
      <div className="rounded-lg border border-slate-200 bg-white p-4">
        <div className="grid grid-cols-1 gap-3 md:grid-cols-12">
          <div className="md:col-span-4">
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
            <label className="block text-xs font-medium text-slate-500">Trust</label>
            <select
              value={trust}
              onChange={e => setTrust(e.target.value)}
              className="mt-1 block w-full rounded border border-slate-300 bg-white px-3 py-2 text-sm"
            >
              <option value="">All ({trustOptions.length})</option>
              {trustOptions.map(t => (
                <option key={t} value={t}>{t}</option>
              ))}
            </select>
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
          <div className="md:col-span-2">
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
          {hasActiveFilter ? (
            <button
              type="button"
              onClick={resetFilters}
              className="text-slate-600 underline hover:text-slate-900"
            >
              Clear filters
            </button>
          ) : null}
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
                const weight = totalNav > 0 ? (p.mv_reporting ?? 0) / totalNav : 0;
                const gl = p.unrealized_gl_local ?? 0;
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
                      {(p.quantity ?? 0).toLocaleString(undefined, {
                        maximumFractionDigits: 4,
                      })}
                    </td>
                    <td className="px-4 py-3 text-right text-slate-700">
                      {p.price_local != null
                        ? money(p.price_local, p.local_ccy ?? reportingCcy)
                        : "—"}
                    </td>
                    <td className="px-4 py-3 text-right font-medium text-slate-900">
                      {money(p.mv_reporting, reportingCcy)}
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
