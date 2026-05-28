import Image from "next/image";
import Link from "next/link";
import { getSupabaseServer } from "@/lib/supabase-server";
import {
  listAccounts,
  listAssetClasses,
  listSubClients,
  listTrusts,
} from "@/lib/queries";
import {
  getSelectedAccounts,
  getSelectedAssetClasses,
  getSelectedSubClient,
  getSelectedTrusts,
} from "@/lib/trust-filter";
import { isAdminEmail } from "@/lib/admin";
import TrustFilter from "@/components/TrustFilter";
import AccountFilter from "@/components/AccountFilter";
import AssetClassFilter from "@/components/AssetClassFilter";
import SubClientSelector from "@/components/SubClientSelector";
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

export default async function Header() {
  const currentTrusts = getSelectedTrusts();
  const currentAccounts = getSelectedAccounts();
  const currentAssetClasses = getSelectedAssetClasses();
  const scope = getSelectedSubClient();

  // Auth first — we need the email to decide whether to fetch the sub-client
  // list (admin-only). Trusts + accounts are scoped to the current sub-client
  // and always needed.
  const {
    data: { user },
  } = await getSupabaseServer().auth.getUser();
  const showSubClientSelector = isAdminEmail(user?.email);

  const [trusts, accounts, assetClasses, subClients] = await Promise.all([
    listTrusts(scope),
    listAccounts(scope, currentTrusts),
    listAssetClasses(scope),
    showSubClientSelector ? listSubClients() : Promise.resolve<string[]>([]),
  ]);

  const trustCrumb = summarise("entities", currentTrusts);
  // accounts in the dropdown are aggregated by physical custody account
  // (1..N reflection node_ids per row). The cookie still holds the
  // underlying node_ids — count breadcrumbs by fully-covered logical
  // accounts so the user sees "1 account" rather than "15 accounts".
  const cookieSet = new Set(currentAccounts);
  const fullyCoveredAccounts = accounts.filter(
    a => a.node_ids.length > 0 && a.node_ids.every(id => cookieSet.has(id)),
  );
  const accountCrumb =
    currentAccounts.length === 0
      ? null
      : fullyCoveredAccounts.length === 1
        ? fullyCoveredAccounts[0].alias
        : `${fullyCoveredAccounts.length || currentAccounts.length} accounts`;

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
          {showSubClientSelector ? (
            <SubClientSelector
              subClients={subClients}
              currentSubClient={scope}
            />
          ) : null}
          <TrustFilter trusts={trusts} currentTrusts={currentTrusts} />
          <AccountFilter accounts={accounts} currentAccounts={currentAccounts} />
          <AssetClassFilter
            classes={assetClasses}
            currentClasses={currentAssetClasses}
          />
        </div>
      </div>
    </header>
  );
}
