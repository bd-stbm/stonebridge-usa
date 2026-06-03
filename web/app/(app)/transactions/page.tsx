import KpiTile from "@/components/KpiTile";
import TransactionsTable from "@/components/TransactionsTable";
import { getTransactions } from "@/lib/queries";
import {
  getSelectedAccounts,
  getSelectedAssetClasses,
  getSelectedTrusts,
} from "@/lib/trust-filter";
import { getActiveSubClient } from "@/lib/session";
import { money } from "@/lib/format";

export const dynamic = "force-dynamic";

function isoDate(d: Date): string {
  return d.toISOString().slice(0, 10);
}

function defaultFrom(): string {
  const today = new Date();
  return isoDate(
    new Date(Date.UTC(today.getUTCFullYear() - 1, today.getUTCMonth(), today.getUTCDate())),
  );
}

function defaultTo(): string {
  return isoDate(new Date());
}

function isValidIso(s: string | undefined): s is string {
  return typeof s === "string" && /^\d{4}-\d{2}-\d{2}$/.test(s);
}

export default async function TransactionsPage({
  searchParams,
}: {
  searchParams: { from?: string; to?: string };
}) {
  const subClient = await getActiveSubClient();
  const trusts = getSelectedTrusts();
  const accounts = getSelectedAccounts();
  const assetClasses = getSelectedAssetClasses();

  const from = isValidIso(searchParams.from) ? searchParams.from : defaultFrom();
  const to = isValidIso(searchParams.to) ? searchParams.to : defaultTo();

  const transactions = await getTransactions(
    subClient,
    trusts,
    accounts,
    from,
    to,
    assetClasses,
  );

  const reportingCcy = transactions[0]?.reporting_ccy ?? "USD";

  let inflow = 0;
  let outflow = 0;
  const typeSet = new Set<string>();
  for (const t of transactions) {
    if (t.transaction_type_clean) typeSet.add(t.transaction_type_clean);
    if (!t.is_external_flow) continue;
    const amt = Number(t.net_amount_reporting ?? 0);
    if (amt > 0) inflow += amt;
    else outflow += Math.abs(amt);
  }

  const scopeNote =
    [
      trusts.length === 1
        ? `Entity: ${trusts[0]}`
        : trusts.length > 1
          ? `${trusts.length} entities`
          : null,
      accounts.length > 0
        ? `${accounts.length} account${accounts.length > 1 ? "s" : ""} scoped`
        : null,
    ]
      .filter(Boolean)
      .join(" · ") || "All entities under " + subClient;

  return (
    <main className="mx-auto max-w-7xl space-y-6 px-6 py-8">
      <div className="flex items-baseline justify-between">
        <h1 className="text-2xl font-semibold text-slate-900">Transactions</h1>
        <span className="text-xs text-slate-500">{scopeNote}</span>
      </div>

      <div className="grid grid-cols-2 gap-4 md:grid-cols-4">
        <KpiTile
          label="Transactions in range"
          value={transactions.length.toLocaleString()}
        />
        <KpiTile label="Types" value={typeSet.size.toString()} />
        <KpiTile
          label="External inflows"
          value={money(inflow, reportingCcy)}
          tone={inflow > 0 ? "positive" : "default"}
          hint="Deposits"
        />
        <KpiTile
          label="External outflows"
          value={money(outflow, reportingCcy)}
          tone={outflow > 0 ? "negative" : "default"}
          hint="Withdrawals"
        />
      </div>

      <TransactionsTable
        transactions={transactions}
        from={from}
        to={to}
      />
    </main>
  );
}
