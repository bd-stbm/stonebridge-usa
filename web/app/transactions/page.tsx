import Header from "@/components/Header";
import KpiTile from "@/components/KpiTile";
import TransactionsTable from "@/components/TransactionsTable";
import {
  DEFAULT_SUB_CLIENT,
  getTransactions,
  type Transaction,
} from "@/lib/queries";
import { getSelectedAccount, getSelectedTrust } from "@/lib/trust-filter";
import { money } from "@/lib/format";

export const dynamic = "force-dynamic";

const VALID_RANGES = ["12m", "ytd", "5y", "all"] as const;
type Range = (typeof VALID_RANGES)[number];

function isoDate(d: Date): string {
  return d.toISOString().slice(0, 10);
}

function computeFromDate(range: Range): string {
  const today = new Date();
  switch (range) {
    case "12m":
      return isoDate(
        new Date(Date.UTC(today.getUTCFullYear() - 1, today.getUTCMonth(), today.getUTCDate())),
      );
    case "ytd":
      return `${today.getUTCFullYear()}-01-01`;
    case "5y":
      return isoDate(
        new Date(Date.UTC(today.getUTCFullYear() - 5, today.getUTCMonth(), today.getUTCDate())),
      );
    case "all":
      return "2000-01-01";
  }
}

export default async function TransactionsPage({
  searchParams,
}: {
  searchParams: { range?: string };
}) {
  const trust = getSelectedTrust();
  const account = getSelectedAccount();
  const range: Range = (VALID_RANGES as readonly string[]).includes(searchParams.range ?? "")
    ? (searchParams.range as Range)
    : "12m";
  const fromDate = computeFromDate(range);

  const transactions = await getTransactions(
    DEFAULT_SUB_CLIENT,
    trust,
    account,
    fromDate,
  );

  const reportingCcy = transactions[0]?.reporting_ccy ?? "USD";

  // KPI counts: # transactions, distinct types, inflow $ (sum of positive
  // external flows), outflow $ (abs sum of negative external flows).
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
    [trust ? `Trust: ${trust}` : null, account ? "Account scoped" : null]
      .filter(Boolean)
      .join(" · ") || "All trusts under " + DEFAULT_SUB_CLIENT;

  return (
    <>
      <Header subClient={DEFAULT_SUB_CLIENT} />
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
          <KpiTile
            label="Types"
            value={typeSet.size.toString()}
          />
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

        <TransactionsTable transactions={transactions} range={range} />
      </main>
    </>
  );
}
