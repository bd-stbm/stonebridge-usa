import Header from "@/components/Header";
import { DEFAULT_SUB_CLIENT } from "@/lib/queries";

export default function HoldingsPage() {
  return (
    <>
      <Header subClient={DEFAULT_SUB_CLIENT} />
      <main className="mx-auto max-w-7xl px-6 py-8">
        <h1 className="text-2xl font-semibold text-slate-900">Holdings</h1>
        <p className="mt-2 text-slate-500">
          Full holdings detail, sortable columns, period filter — coming soon.
        </p>
      </main>
    </>
  );
}
