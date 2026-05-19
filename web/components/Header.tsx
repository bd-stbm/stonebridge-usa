import Image from "next/image";
import Link from "next/link";
import { getSupabaseServer } from "@/lib/supabase-server";
import {
  DEFAULT_SUB_CLIENT,
  listAccounts,
  listTrusts,
} from "@/lib/queries";
import { getSelectedAccounts, getSelectedTrusts } from "@/lib/trust-filter";
import TrustFilter from "@/components/TrustFilter";
import AccountFilter from "@/components/AccountFilter";
import UserMenu from "@/components/UserMenu";

const TABS = [
  { href: "/", label: "Overview" },
  { href: "/holdings", label: "Holdings" },
  { href: "/performance", label: "Performance" },
  { href: "/income", label: "Income" },
  { href: "/transactions", label: "Transactions" },
];

function summarise(label: string, items: string[]): string | null {
  if (items.length === 0) return null;
  if (items.length === 1) return items[0];
  return `${items.length} ${label}`;
}

export default async function Header({ subClient }: { subClient: string }) {
  const currentTrusts = getSelectedTrusts();
  const currentAccounts = getSelectedAccounts();
  const scope = subClient ?? DEFAULT_SUB_CLIENT;

  const [{ data: { user } }, trusts, accounts] = await Promise.all([
    getSupabaseServer().auth.getUser(),
    listTrusts(scope),
    listAccounts(scope, currentTrusts),
  ]);

  const trustCrumb = summarise("trusts", currentTrusts);
  const accountSelected =
    currentAccounts.length === 1
      ? accounts.find(a => a.node_id === currentAccounts[0])?.alias ?? currentAccounts[0]
      : null;
  const accountCrumb =
    accountSelected ??
    (currentAccounts.length > 1 ? `${currentAccounts.length} accounts` : null);

  return (
    <header className="border-b border-slate-200 bg-white">
      <div className="mx-auto flex max-w-7xl flex-wrap items-center justify-between gap-4 px-6 py-4">
        <Link href="/" className="flex items-center gap-3">
          <Image
            src="/stonebridge-logo.png"
            alt="Stonebridge"
            width={140}
            height={48}
            className="h-10 w-auto"
            priority
          />
          <span className="hidden text-xs text-slate-500 sm:inline">
            {scope}
            {trustCrumb ? (
              <> · <span className="text-slate-700">{trustCrumb}</span></>
            ) : null}
            {accountCrumb ? (
              <> · <span className="text-slate-700">{accountCrumb}</span></>
            ) : null}
          </span>
        </Link>
        <nav className="flex flex-wrap items-center gap-6 text-sm">
          {TABS.map(t => (
            <Link
              key={t.href}
              href={t.href}
              className="text-slate-600 hover:text-brand"
            >
              {t.label}
            </Link>
          ))}
          {user?.email ? (
            <div className="border-l border-slate-200 pl-6">
              <UserMenu email={user.email} />
            </div>
          ) : null}
        </nav>
      </div>
      <div className="border-t border-slate-100 bg-slate-50">
        <div className="mx-auto flex max-w-7xl flex-wrap items-center justify-end gap-4 px-6 py-2">
          <TrustFilter trusts={trusts} currentTrusts={currentTrusts} />
          <AccountFilter accounts={accounts} currentAccounts={currentAccounts} />
        </div>
      </div>
    </header>
  );
}
