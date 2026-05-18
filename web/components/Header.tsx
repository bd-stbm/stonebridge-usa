import Link from "next/link";
import { getSupabaseServer } from "@/lib/supabase-server";
import { DEFAULT_SUB_CLIENT, listTrusts } from "@/lib/queries";
import { getSelectedTrust } from "@/lib/trust-filter";
import TrustFilter from "@/components/TrustFilter";

const TABS = [
  { href: "/", label: "Overview" },
  { href: "/holdings", label: "Holdings" },
  { href: "/performance", label: "Performance" },
  { href: "/income", label: "Income" },
];

export default async function Header({ subClient }: { subClient: string }) {
  const [{ data: { user } }, trusts] = await Promise.all([
    getSupabaseServer().auth.getUser(),
    listTrusts(subClient ?? DEFAULT_SUB_CLIENT),
  ]);
  const currentTrust = getSelectedTrust();

  return (
    <header className="border-b border-slate-200 bg-white">
      <div className="mx-auto flex max-w-7xl flex-wrap items-center justify-between gap-4 px-6 py-4">
        <div>
          <h1 className="text-lg font-semibold text-slate-900">Stonebridge</h1>
          <p className="text-xs text-slate-500">
            {subClient}
            {currentTrust ? <> · <span className="text-slate-700">{currentTrust}</span></> : null}
          </p>
        </div>
        <nav className="flex flex-wrap items-center gap-6 text-sm">
          {TABS.map(t => (
            <Link
              key={t.href}
              href={t.href}
              className="text-slate-600 hover:text-slate-900"
            >
              {t.label}
            </Link>
          ))}
          {user ? (
            <form
              action="/auth/signout"
              method="post"
              className="flex items-center gap-3 border-l border-slate-200 pl-6"
            >
              <span className="text-xs text-slate-500">{user.email}</span>
              <button
                type="submit"
                className="rounded border border-slate-300 px-2 py-1 text-xs text-slate-700 hover:bg-slate-50"
              >
                Sign out
              </button>
            </form>
          ) : null}
        </nav>
      </div>
      <div className="border-t border-slate-100 bg-slate-50">
        <div className="mx-auto flex max-w-7xl items-center justify-end px-6 py-2">
          <TrustFilter trusts={trusts} currentTrust={currentTrust} />
        </div>
      </div>
    </header>
  );
}
