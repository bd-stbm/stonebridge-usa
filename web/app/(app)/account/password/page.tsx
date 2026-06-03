import ChangePasswordForm from "@/components/ChangePasswordForm";

export const dynamic = "force-dynamic";

export default function AccountPasswordPage() {
  return (
    <main className="mx-auto max-w-md px-6 py-8">
      <div className="mb-6">
        <h1 className="text-2xl font-semibold text-slate-900">Change password</h1>
        <p className="mt-1 text-sm text-slate-500">
          Update the password for your Stonebridge login.
        </p>
      </div>
      <ChangePasswordForm />
    </main>
  );
}
