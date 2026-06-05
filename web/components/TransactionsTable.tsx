"use client";

import { useMemo, useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import clsx from "clsx";
import type { Transaction } from "@/lib/queries";
import { money, price as priceFmt } from "@/lib/format";

type SortKey =
  | "transaction_date"
  | "transaction_type_clean"
  | "asset_name"
  | "quantity"
  | "net_amount_reporting"
  | "account_alias";

type SortDir = "asc" | "desc";

const COLUMNS: {
  key: SortKey;
  label: string;
  align: "left" | "right";
}[] = [
  { key: "transaction_date", label: "Date", align: "left" },
  { key: "transaction_type_clean", label: "Type", align: "left" },
  { key: "asset_name", label: "Asset", align: "left" },
  { key: "account_alias", label: "Account", align: "left" },
  { key: "quantity", label: "Quantity", align: "right" },
  { key: "net_amount_reporting", label: "Amount", align: "right" },
];

const PRESETS: { value: string; label: string }[] = [
  { value: "1m", label: "1M" },
  { value: "3m", label: "3M" },
  { value: "6m", label: "6M" },
  { value: "12m", label: "12M" },
  { value: "ytd", label: "YTD" },
  { value: "2y", label: "2Y" },
  { value: "5y", label: "5Y" },
  { value: "all", label: "All" },
];

function isoDate(d: Date): string {
  return d.toISOString().slice(0, 10);
}

function computePresetFrom(preset: string): string {
  const today = new Date();
  const y = today.getUTCFullYear();
  const m = today.getUTCMonth();
  const d = today.getUTCDate();
  switch (preset) {
    case "1m":
      return isoDate(new Date(Date.UTC(y, m - 1, d)));
    case "3m":
      return isoDate(new Date(Date.UTC(y, m - 3, d)));
    case "6m":
      return isoDate(new Date(Date.UTC(y, m - 6, d)));
    case "12m":
      return isoDate(new Date(Date.UTC(y - 1, m, d)));
    case "ytd":
      return `${y}-01-01`;
    case "2y":
      return isoDate(new Date(Date.UTC(y - 2, m, d)));
    case "5y":
      return isoDate(new Date(Date.UTC(y - 5, m, d)));
    case "all":
      return "2000-01-01";
    default:
      return isoDate(new Date(Date.UTC(y - 1, m, d)));
  }
}

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

interface Props {
  transactions: Transaction[];
  from: string;
  to: string;
}

export default function TransactionsTable({ transactions, from, to }: Props) {
  const router = useRouter();
  const [pending, startTransition] = useTransition();

  const [search, setSearch] = useState("");
  const [type, setType] = useState("");
  const [sortKey, setSortKey] = useState<SortKey>("transaction_date");
  const [sortDir, setSortDir] = useState<SortDir>("desc");

  const typeOptions = useMemo(() => {
    const set = new Set<string>();
    for (const t of transactions) {
      if (t.transaction_type_clean) set.add(t.transaction_type_clean);
    }
    return Array.from(set).sort();
  }, [transactions]);

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    return transactions.filter(t => {
      if (type && t.transaction_type_clean !== type) return false;
      if (!q) return true;
      const hay = [t.asset_name, t.ticker_masttro, t.comments]
        .filter(Boolean)
        .join(" ")
        .toLowerCase();
      return hay.includes(q);
    });
  }, [transactions, search, type]);

  const sorted = useMemo(() => {
    const arr = filtered.slice();
    arr.sort((a, b) => {
      const numericKeys: SortKey[] = ["quantity", "net_amount_reporting"];
      if (numericKeys.includes(sortKey)) {
        return compareValues(num(a[sortKey]), num(b[sortKey]), sortDir);
      }
      return compareValues(a[sortKey], b[sortKey], sortDir);
    });
    return arr;
  }, [filtered, sortKey, sortDir]);

  const handleSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir(d => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortKey(key);
      setSortDir(
        key === "quantity" || key === "net_amount_reporting" || key === "transaction_date"
          ? "desc"
          : "asc",
      );
    }
  };

  const pushRange = (newFrom: string, newTo: string) => {
    const params = new URLSearchParams();
    params.set("from", newFrom);
    params.set("to", newTo);
    startTransition(() => {
      router.push(`/transactions?${params.toString()}`);
    });
  };

  const applyPreset = (preset: string) => {
    const today = new Date();
    pushRange(computePresetFrom(preset), isoDate(today));
  };

  const resetFilters = () => {
    setSearch("");
    setType("");
  };

  const hasActiveFilter = search || type;

  return (
    <div className="space-y-4">
      <div className="rounded-lg border border-slate-200 bg-white p-4">
        <div className="grid grid-cols-1 gap-3 md:grid-cols-12">
          <div className="md:col-span-3">
            <label className="block text-xs font-medium text-slate-500">
              Search
            </label>
            <input
              type="text"
              value={search}
              onChange={e => setSearch(e.target.value)}
              placeholder="Asset, ticker, comments…"
              className="mt-1 block w-full rounded border border-slate-300 px-3 py-2 text-sm focus:border-slate-500 focus:outline-none"
            />
          </div>
          <div className="md:col-span-3">
            <label className="block text-xs font-medium text-slate-500">Type</label>
            <select
              value={type}
              onChange={e => setType(e.target.value)}
              className="mt-1 block w-full rounded border border-slate-300 bg-white px-3 py-2 text-sm"
            >
              <option value="">All ({typeOptions.length})</option>
              {typeOptions.map(t => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
          </div>
          <div className="md:col-span-3">
            <label className="block text-xs font-medium text-slate-500">From</label>
            <input
              type="date"
              value={from}
              disabled={pending}
              onChange={e => pushRange(e.target.value, to)}
              className="mt-1 block w-full rounded border border-slate-300 bg-white px-3 py-2 text-sm disabled:opacity-60"
            />
          </div>
          <div className="md:col-span-3">
            <label className="block text-xs font-medium text-slate-500">To</label>
            <input
              type="date"
              value={to}
              disabled={pending}
              onChange={e => pushRange(from, e.target.value)}
              className="mt-1 block w-full rounded border border-slate-300 bg-white px-3 py-2 text-sm disabled:opacity-60"
            />
          </div>
          <div className="md:col-span-12">
            <div className="mt-1 flex flex-wrap gap-1">
              {PRESETS.map(p => (
                <button
                  key={p.value}
                  type="button"
                  disabled={pending}
                  onClick={() => applyPreset(p.value)}
                  className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 hover:bg-slate-50 disabled:opacity-60"
                >
                  {p.label}
                </button>
              ))}
            </div>
          </div>
        </div>
        <div className="mt-3 flex items-center justify-between text-xs text-slate-500">
          <div>
            Showing <span className="font-medium text-slate-700">{filtered.length}</span> of{" "}
            <span className="font-medium text-slate-700">{transactions.length}</span> transactions in range
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
                      col.key === COLUMNS[0].key &&
                        "sticky left-0 z-20 bg-slate-50",
                    )}
                    onClick={() => handleSort(col.key)}
                  >
                    <span
                      className={clsx(
                        "inline-flex items-center gap-1",
                        col.align === "right" && "justify-end",
                      )}
                    >
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
                  No transactions match the current filters.
                </td>
              </tr>
            ) : (
              sorted.map(t => {
                const qty = num(t.quantity);
                const amount = num(t.net_amount_reporting);
                const isFlow = t.is_external_flow;
                return (
                  <tr key={t.transaction_id} className="group hover:bg-slate-50">
                    <td className="sticky left-0 z-10 bg-white px-4 py-3 text-slate-600 whitespace-nowrap align-top group-hover:bg-slate-50">
                      {t.transaction_date ?? "—"}
                    </td>
                    <td className="px-4 py-3 align-top">
                      <span
                        className={clsx(
                          "rounded-full px-2 py-0.5 text-xs font-medium",
                          isFlow
                            ? "bg-brand-tint text-brand-dark"
                            : "bg-slate-100 text-slate-700",
                        )}
                      >
                        {t.transaction_type_clean ?? "—"}
                      </span>
                    </td>
                    <td className="px-4 py-3 align-top">
                      <div className="font-medium text-slate-900">
                        {t.asset_name ?? (t.comments ? (
                          <span className="italic text-slate-500">{t.comments}</span>
                        ) : "—")}
                        {t.ticker_masttro ? (
                          <span className="ml-1 text-xs font-normal text-slate-400">
                            {t.ticker_masttro}
                          </span>
                        ) : null}
                      </div>
                      {t.asset_name && t.comments ? (
                        <div className="mt-0.5 max-w-md text-xs italic text-slate-500">
                          {t.comments}
                        </div>
                      ) : null}
                    </td>
                    <td className="px-4 py-3 text-slate-600 align-top">
                      {t.account_alias ?? "—"}
                    </td>
                    <td className="px-4 py-3 text-right text-slate-700 align-top">
                      {qty !== 0 ? qty.toLocaleString(undefined, { maximumFractionDigits: 4 }) : "—"}
                    </td>
                    <td
                      className={clsx(
                        "px-4 py-3 text-right font-medium align-top",
                        amount >= 0 ? "text-slate-900" : "text-rose-600",
                      )}
                    >
                      {amount !== 0 ? priceFmt(amount, t.reporting_ccy ?? "USD") : "—"}
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
