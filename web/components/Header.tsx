import Link from "next/link";
import { getSupabaseServer } from "@/lib/supabase-server";

const TABS = [
  { href: "/", label: "Overview" },
  { href: "/holdings", label: "Holdings" },
  { href: "/performance", label: "Performance" },
  { href: "/income", label: "Income" },
];

export default async function Header({ subClient }: { subClient: string }) {
  const {
    data: { user },
  } = await getSupabaseServer().auth.getUser();

  return (
    <header className="border-b border-slate-200 bg-white">
      <div className="mx-auto flex max-w-7xl items-center justify-between px-6 py-4">
        <div>
          <h1 className="text-lg font-semibold text-slate-900">Stonebridge</h1>
          <p className="text-xs text-slate-500">{subClient}</p>
        </div>
        <nav className="flex items-center gap-6 text-sm">
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
    </header>
  );
}
