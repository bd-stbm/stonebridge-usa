import Image from "next/image";
import Link from "next/link";
import {
  listAccounts,
  listAssetClasses,
  listTrusts,
} from "@/lib/queries";
import {
  getSelectedAccounts,
  getSelectedAssetClasses,
  getSelectedTrusts,
} from "@/lib/trust-filter";
import {
  getAccessibleSubClients,
  getActiveSubClient,
  getSessionUser,
} from "@/lib/session";
import TrustFilter from "@/components/TrustFilter";
import AccountFilter from "@/components/AccountFilter";
import AssetClassFilter from "@/components/AssetClassFilter";
import SubClientSelector from "@/components/SubClientSelector";
import UserMenu from "@/components/UserMenu";
import MobileNav from "@/components/MobileNav";

const TABS = [
  { href: "/", label: "Overview" },
  { href: "/networth", label: "Net Worth" },
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

  // Identity + the families this user can actually see (RLS-scoped). The
  // effective scope is clamped to that set so a client never lands on a
  // family they can't read. The selector is shown to admins and to any
  // client who holds more than one family; a single-family client has
  // nothing to switch between, so it's hidden.
  const sessionUser = await getSessionUser();
  const accessibleSubClients = await getAccessibleSubClients();
  const scope = await getActiveSubClient(accessibleSubClients);
  const showSubClientSelector =
    (sessionUser?.isAdmin ?? false) || accessibleSubClients.length > 1;

  const [trusts, accounts, assetClasses] = await Promise.all([
    listTrusts(scope),
    listAccounts(scope, currentTrusts),
    listAssetClasses(scope),
  ]);
  const subClients = showSubClientSelector ? accessibleSubClients : [];

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
      <div className="mx-auto flex max-w-7xl flex-wrap items-center justify-between gap-4 px-4 py-4 sm:px-6">
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
        <div className="flex items-center gap-4">
          <nav className="hidden items-center gap-6 text-sm md:flex">
            {TABS.map(t => (
              <Link
                key={t.href}
                href={t.href}
                className="text-slate-600 hover:text-brand"
              >
                {t.label}
              </Link>
            ))}
            {sessionUser?.isAdmin ? (
              <Link
                href="/admin/users"
                className="text-slate-600 hover:text-brand"
              >
                Users
              </Link>
            ) : null}
          </nav>
          {sessionUser?.email ? (
            <div className="md:border-l md:border-slate-200 md:pl-4">
              <UserMenu email={sessionUser.email} />
            </div>
          ) : null}
          <MobileNav tabs={TABS} isAdmin={sessionUser?.isAdmin ?? false} />
        </div>
      </div>
      <div className="border-t border-slate-100 bg-slate-50">
        <div className="mx-auto flex max-w-7xl flex-wrap items-center justify-end gap-4 px-4 py-2 sm:px-6">
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
