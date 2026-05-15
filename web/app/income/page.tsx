import Header from "@/components/Header";
import { DEFAULT_SUB_CLIENT } from "@/lib/queries";

export default function IncomePage() {
  return (
    <>
      <Header subClient={DEFAULT_SUB_CLIENT} />
      <main className="mx-auto max-w-7xl px-6 py-8">
        <h1 className="text-2xl font-semibold text-slate-900">Income</h1>
        <p className="mt-2 text-slate-500">
          Monthly dividends / interest, yield, income by holding — coming soon.
        </p>
      </main>
    </>
  );
}
