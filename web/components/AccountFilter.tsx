"use client";

import { useTransition } from "react";
import { useRouter } from "next/navigation";
import { setAccountFilter } from "@/lib/actions";
import type { AccountOption } from "@/lib/queries";

interface Props {
  accounts: AccountOption[];
  currentAccount: string | null;
}

export default function AccountFilter({ accounts, currentAccount }: Props) {
  const router = useRouter();
  const [pending, startTransition] = useTransition();

  return (
    <label className="flex items-center gap-2 text-xs text-slate-500">
      <span className="hidden sm:inline">Account</span>
      <select
        value={currentAccount ?? ""}
        disabled={pending || accounts.length === 0}
        onChange={e => {
          const value = e.target.value;
          startTransition(async () => {
            await setAccountFilter(value);
            router.refresh();
          });
        }}
        className="rounded border border-slate-300 bg-white px-2 py-1 text-xs text-slate-700 disabled:opacity-60"
      >
        <option value="">All accounts</option>
        {accounts.map(a => (
          <option key={a.node_id} value={a.node_id}>
            {a.alias}
            {a.custodian ? ` · ${a.custodian}` : ""}
          </option>
        ))}
      </select>
    </label>
  );
}
