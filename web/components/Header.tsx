import Link from "next/link";

const TABS = [
  { href: "/", label: "Overview" },
  { href: "/holdings", label: "Holdings" },
  { href: "/performance", label: "Performance" },
  { href: "/income", label: "Income" },
];

export default function Header({ subClient }: { subClient: string }) {
  return (
    <header className="border-b border-slate-200 bg-white">
      <div className="mx-auto flex max-w-7xl items-center justify-between px-6 py-4">
        <div>
          <h1 className="text-lg font-semibold text-slate-900">Stonebridge</h1>
          <p className="text-xs text-slate-500">{subClient}</p>
        </div>
        <nav className="flex gap-6 text-sm">
          {TABS.map(t => (
            <Link
              key={t.href}
              href={t.href}
              className="text-slate-600 hover:text-slate-900"
            >
              {t.label}
            </Link>
          ))}
        </nav>
      </div>
    </header>
  );
}
