import Header from "@/components/Header";
import { getAccessibleSubClients, getSessionUser } from "@/lib/session";

// Header lives in this shared layout so it persists across tab navigation
// instead of being torn down and rebuilt on every page. Combined with the
// sibling loading.tsx, this makes tab clicks feel instant — the header
// stays put and only the <main> region shows the skeleton while the new
// page's server data loads.
export default async function AppLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  // A signed-in user who isn't an admin and has no family mapping yet (RLS
  // returns zero accessible families) would otherwise see empty tiles on
  // every page. Show a clear holding message instead. This is UX only —
  // the data is already protected by row-level security.
  const [user, accessible] = await Promise.all([
    getSessionUser(),
    getAccessibleSubClients(),
  ]);
  const noAccess = !!user && !user.isAdmin && accessible.length === 0;

  return (
    <>
      <Header />
      {noAccess ? (
        <main className="mx-auto max-w-7xl px-4 py-16 sm:px-6">
          <div className="rounded-lg border border-slate-200 bg-white p-8 text-center">
            <h1 className="text-lg font-semibold text-slate-900">
              Your access is being set up
            </h1>
            <p className="mt-2 text-sm text-slate-500">
              Your account isn&apos;t linked to a portfolio yet. Please contact
              your Stonebridge advisor and we&apos;ll get you connected.
            </p>
          </div>
        </main>
      ) : (
        children
      )}
    </>
  );
}
