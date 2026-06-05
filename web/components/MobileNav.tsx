"use client";

import { useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import clsx from "clsx";

interface Tab {
  href: string;
  label: string;
}

// Mobile-only (below md) hamburger that collapses the header's nav tabs
// into a dropdown. The desktop tab row in Header stays inline; this is its
// small-screen replacement. UserMenu lives outside this, visible at all
// widths.
export default function MobileNav({
  tabs,
  isAdmin,
}: {
  tabs: Tab[];
  isAdmin: boolean;
}) {
  const [open, setOpen] = useState(false);
  const pathname = usePathname();

  const items = isAdmin ? [...tabs, { href: "/admin/users", label: "Users" }] : tabs;

  return (
    <div className="relative md:hidden">
      <button
        type="button"
        onClick={() => setOpen(o => !o)}
        aria-label={open ? "Close menu" : "Open menu"}
        aria-expanded={open}
        className="flex h-9 w-9 items-center justify-center rounded-md border border-slate-300 text-slate-600 hover:bg-slate-50"
      >
        <svg
          className="h-5 w-5"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          aria-hidden="true"
        >
          {open ? (
            <path d="M6 6l12 12M18 6 6 18" />
          ) : (
            <path d="M4 7h16M4 12h16M4 17h16" />
          )}
        </svg>
      </button>
      {open ? (
        <>
          {/* Click-away backdrop */}
          <div
            className="fixed inset-0 z-30"
            onClick={() => setOpen(false)}
            aria-hidden="true"
          />
          <nav className="absolute right-0 top-full z-40 mt-2 w-48 overflow-hidden rounded-md border border-slate-200 bg-white py-1 shadow-lg">
            {items.map(t => {
              const active =
                t.href === "/" ? pathname === "/" : pathname.startsWith(t.href);
              return (
                <Link
                  key={t.href}
                  href={t.href}
                  onClick={() => setOpen(false)}
                  className={clsx(
                    "block px-4 py-2.5 text-sm",
                    active
                      ? "bg-brand-tint font-medium text-brand"
                      : "text-slate-700 hover:bg-slate-50",
                  )}
                >
                  {t.label}
                </Link>
              );
            })}
          </nav>
        </>
      ) : null}
    </div>
  );
}
