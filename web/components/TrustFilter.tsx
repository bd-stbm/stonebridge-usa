"use client";

import { useTransition } from "react";
import { useRouter } from "next/navigation";
import { setTrustFilter } from "@/lib/actions";

interface Props {
  trusts: string[];
  currentTrust: string | null;
}

export default function TrustFilter({ trusts, currentTrust }: Props) {
  const router = useRouter();
  const [pending, startTransition] = useTransition();

  return (
    <label className="flex items-center gap-2 text-xs text-slate-500">
      <span className="hidden sm:inline">Trust</span>
      <select
        value={currentTrust ?? ""}
        disabled={pending}
        onChange={e => {
          const value = e.target.value;
          startTransition(async () => {
            await setTrustFilter(value);
            router.refresh();
          });
        }}
        className="rounded border border-slate-300 bg-white px-2 py-1 text-xs text-slate-700 disabled:opacity-60"
      >
        <option value="">All trusts</option>
        {trusts.map(t => (
          <option key={t} value={t}>
            {t}
          </option>
        ))}
      </select>
    </label>
  );
}
