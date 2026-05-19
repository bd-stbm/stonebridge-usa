import Image from "next/image";
import Link from "next/link";
import { getSupabaseServer } from "@/lib/supabase-server";
import {
  DEFAULT_SUB_CLIENT,
  listAccounts,
  listTrusts,
} from "@/lib/queries";
import { getSelectedAccount, getSelectedTrust } from "@/lib/trust-filter";
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

export default async function Header({ subClient }: { subClient: string }) {
  const currentTrust = getSelectedTrust();
  const currentAccount = getSelectedAccount();
  const scope = subClient ?? DEFAULT_SUB_CLIENT;

  const [{ data: { user } }, trusts, accounts] = await Promise.all([
    getSupabaseServer().auth.getUser(),
    listTrusts(scope),
    listAccounts(scope, currentTrust),
  ]);

  const currentAccountLabel = currentAccount
    ? accounts.find(a => a.node_id === currentAccount)?.alias ?? currentAccount
    : null;

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
            {currentTrust ? (
              <> · <span className="text-slate-700">{currentTrust}</span></>
            ) : null}
            {currentAccountLabel ? (
              <> · <span className="text-slate-700">{currentAccountLabel}</span></>
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
          <TrustFilter trusts={trusts} currentTrust={currentTrust} />
          <AccountFilter accounts={accounts} currentAccount={currentAccount} />
        </div>
      </div>
    </header>
  );
}
