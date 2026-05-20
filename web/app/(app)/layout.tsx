import Header from "@/components/Header";
import { DEFAULT_SUB_CLIENT } from "@/lib/queries";

// Header lives in this shared layout so it persists across tab navigation
// instead of being torn down and rebuilt on every page. Combined with the
// sibling loading.tsx, this makes tab clicks feel instant — the header
// stays put and only the <main> region shows the skeleton while the new
// page's server data loads.
export default function AppLayout({ children }: { children: React.ReactNode }) {
  return (
    <>
      <Header subClient={DEFAULT_SUB_CLIENT} />
      {children}
    </>
  );
}
