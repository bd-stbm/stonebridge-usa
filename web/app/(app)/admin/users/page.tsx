import { redirect } from "next/navigation";
import { getSessionUser } from "@/lib/session";
import { listAllFamilies, listManagedUsers } from "@/lib/admin-data";
import UsersAdminPanel from "@/components/UsersAdminPanel";

export const dynamic = "force-dynamic";

export default async function UsersAdminPage() {
  // Server-side gate. The (app)/layout Header also hides the link for
  // non-admins, and every server action re-checks requireAdmin(), but this
  // stops a non-admin from loading the page (and the service-role data) at all.
  const user = await getSessionUser();
  if (!user?.isAdmin) redirect("/");

  const [users, families] = await Promise.all([
    listManagedUsers(),
    listAllFamilies(),
  ]);

  return (
    <main className="mx-auto max-w-7xl px-6 py-8">
      <div className="mb-6">
        <h1 className="text-2xl font-semibold text-slate-900">User management</h1>
        <p className="mt-1 text-sm text-slate-500">
          Invite clients and choose which families each can see. Access is
          enforced by the database — a client only ever receives rows for the
          families assigned here.
        </p>
      </div>
      <UsersAdminPanel users={users} families={families} currentUserId={user.id} />
    </main>
  );
}
