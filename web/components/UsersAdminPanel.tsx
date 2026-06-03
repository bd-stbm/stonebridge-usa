"use client";

import { useState, useTransition } from "react";
import { useRouter } from "next/navigation";
import clsx from "clsx";
import type { FamilyOption, ManagedUser, UserRole } from "@/lib/admin-data";
import {
  createClientUser,
  revokeUser,
  saveUser,
  setUserPassword,
} from "@/lib/admin-actions";

interface Props {
  users: ManagedUser[];
  families: FamilyOption[];
  currentUserId: string;
}

function FamilyChecklist({
  families,
  selected,
  onToggle,
  disabled,
}: {
  families: FamilyOption[];
  selected: Set<string>;
  onToggle: (nodeId: string) => void;
  disabled?: boolean;
}) {
  return (
    <div className="flex flex-wrap gap-x-4 gap-y-1">
      {families.map(f => (
        <label
          key={f.nodeId}
          className={clsx(
            "inline-flex items-center gap-1.5 text-xs",
            disabled ? "text-slate-400" : "text-slate-700",
          )}
        >
          <input
            type="checkbox"
            checked={selected.has(f.nodeId)}
            onChange={() => onToggle(f.nodeId)}
            disabled={disabled}
            className="h-3.5 w-3.5 rounded border-slate-300 text-brand focus:ring-brand"
          />
          {f.alias}
        </label>
      ))}
    </div>
  );
}

function UserRow({
  user,
  families,
  isSelf,
}: {
  user: ManagedUser;
  families: FamilyOption[];
  isSelf: boolean;
}) {
  const router = useRouter();
  const [role, setRole] = useState<UserRole>(user.role ?? "client");
  const [selected, setSelected] = useState<Set<string>>(
    new Set(user.familyNodeIds),
  );
  const [pending, startTransition] = useTransition();
  const [msg, setMsg] = useState<string | null>(null);

  const dirty =
    role !== (user.role ?? "client") ||
    selected.size !== user.familyNodeIds.length ||
    user.familyNodeIds.some(n => !selected.has(n));

  const toggle = (nodeId: string) =>
    setSelected(prev => {
      const next = new Set(prev);
      next.has(nodeId) ? next.delete(nodeId) : next.add(nodeId);
      return next;
    });

  const onSave = () =>
    startTransition(async () => {
      setMsg(null);
      const res = await saveUser(user.id, role, Array.from(selected));
      if (!res.ok) setMsg(res.error ?? "Save failed.");
      else router.refresh();
    });

  const onRevoke = () => {
    if (
      !confirm(
        `Revoke ${user.email ?? "this user"}'s access? Their login stays but they'll see nothing until reassigned.`,
      )
    )
      return;
    startTransition(async () => {
      setMsg(null);
      const res = await revokeUser(user.id);
      if (!res.ok) setMsg(res.error ?? "Revoke failed.");
      else router.refresh();
    });
  };

  const onSetPassword = () => {
    const pw = window.prompt(
      `Set a new password for ${user.email ?? "this user"} (min 8 characters):`,
    );
    if (!pw) return;
    startTransition(async () => {
      setMsg(null);
      const res = await setUserPassword(user.id, pw);
      if (res.ok) window.alert("Password updated.");
      else setMsg(res.error ?? "Failed to set password.");
    });
  };

  return (
    <tr className="align-top">
      <td className="px-4 py-3">
        <div className="font-medium text-slate-900">{user.email ?? "—"}</div>
        <div className="text-xs text-slate-400">
          {user.role == null
            ? "no access yet"
            : user.lastSignInAt
              ? `last sign-in ${user.lastSignInAt.slice(0, 10)}`
              : "never signed in"}
          {isSelf ? " · you" : ""}
        </div>
      </td>
      <td className="px-4 py-3">
        <select
          value={role}
          onChange={e => setRole(e.target.value as UserRole)}
          disabled={pending || isSelf}
          className="rounded border border-slate-300 bg-white px-2 py-1 text-xs text-slate-700 focus:border-brand focus:outline-none disabled:opacity-60"
        >
          <option value="client">Client</option>
          <option value="admin">Admin</option>
        </select>
      </td>
      <td className="px-4 py-3">
        {role === "admin" ? (
          <span className="text-xs italic text-slate-400">
            Admins see all families.
          </span>
        ) : (
          <FamilyChecklist
            families={families}
            selected={selected}
            onToggle={toggle}
            disabled={pending}
          />
        )}
        {msg ? <div className="mt-1 text-xs text-rose-600">{msg}</div> : null}
      </td>
      <td className="whitespace-nowrap px-4 py-3 text-right">
        <button
          type="button"
          onClick={onSave}
          disabled={!dirty || pending}
          className="rounded bg-brand px-2.5 py-1 text-xs font-medium text-white hover:bg-brand-dark disabled:cursor-not-allowed disabled:opacity-40"
        >
          {pending ? "Saving…" : "Save"}
        </button>
        <button
          type="button"
          onClick={onSetPassword}
          disabled={pending}
          className="ml-2 rounded border border-slate-300 px-2.5 py-1 text-xs font-medium text-slate-600 hover:bg-slate-50 disabled:opacity-40"
        >
          Set password
        </button>
        {!isSelf && user.role != null ? (
          <button
            type="button"
            onClick={onRevoke}
            disabled={pending}
            className="ml-2 rounded border border-slate-300 px-2.5 py-1 text-xs font-medium text-slate-600 hover:bg-slate-50 disabled:opacity-40"
          >
            Revoke
          </button>
        ) : null}
      </td>
    </tr>
  );
}

export default function UsersAdminPanel({ users, families, currentUserId }: Props) {
  const router = useRouter();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [pending, startTransition] = useTransition();
  const [msg, setMsg] = useState<{ tone: "ok" | "err"; text: string } | null>(null);

  const toggle = (nodeId: string) =>
    setSelected(prev => {
      const next = new Set(prev);
      next.has(nodeId) ? next.delete(nodeId) : next.add(nodeId);
      return next;
    });

  const onCreate = () =>
    startTransition(async () => {
      setMsg(null);
      const res = await createClientUser(email, password, Array.from(selected));
      if (!res.ok) {
        setMsg({ tone: "err", text: res.error ?? "Could not create user." });
      } else {
        setMsg({
          tone: "ok",
          text: `Created ${email.trim()}. Share the email + password with them; they can change it from the user menu.`,
        });
        setEmail("");
        setPassword("");
        setSelected(new Set());
        router.refresh();
      }
    });

  return (
    <div className="space-y-8">
      {/* Add client */}
      <section className="rounded-lg border border-slate-200 bg-white p-4">
        <h2 className="text-sm font-semibold text-slate-900">Add a client</h2>
        <p className="mt-0.5 text-xs text-slate-500">
          Creates the login with a password you set (no email is sent). Pick
          the families they should see — you can change these any time below.
        </p>
        <div className="mt-3 flex flex-col gap-3 md:flex-row md:items-end">
          <div className="md:w-60">
            <label className="block text-xs font-medium text-slate-600">
              Email
            </label>
            <input
              type="email"
              value={email}
              onChange={e => setEmail(e.target.value)}
              placeholder="client@example.com"
              className="mt-1 w-full rounded border border-slate-300 px-2.5 py-1.5 text-sm text-slate-700 focus:border-brand focus:outline-none focus:ring-1 focus:ring-brand"
            />
          </div>
          <div className="md:w-48">
            <label className="block text-xs font-medium text-slate-600">
              Temporary password
            </label>
            <input
              type="text"
              value={password}
              onChange={e => setPassword(e.target.value)}
              placeholder="min 8 characters"
              autoComplete="off"
              className="mt-1 w-full rounded border border-slate-300 px-2.5 py-1.5 text-sm text-slate-700 focus:border-brand focus:outline-none focus:ring-1 focus:ring-brand"
            />
          </div>
          <div className="flex-1">
            <span className="block text-xs font-medium text-slate-600">
              Families
            </span>
            <div className="mt-1.5">
              <FamilyChecklist
                families={families}
                selected={selected}
                onToggle={toggle}
                disabled={pending}
              />
            </div>
          </div>
          <button
            type="button"
            onClick={onCreate}
            disabled={pending || !email.trim() || password.length < 8}
            className="rounded bg-brand px-3 py-1.5 text-sm font-medium text-white hover:bg-brand-dark disabled:cursor-not-allowed disabled:opacity-40"
          >
            {pending ? "Creating…" : "Create client"}
          </button>
        </div>
        {msg ? (
          <div
            className={clsx(
              "mt-2 text-xs",
              msg.tone === "ok" ? "text-emerald-600" : "text-rose-600",
            )}
          >
            {msg.text}
          </div>
        ) : null}
      </section>

      {/* Users */}
      <section className="overflow-x-auto rounded-lg border border-slate-200 bg-white">
        <table className="min-w-full text-sm">
          <thead className="bg-slate-50 text-xs uppercase tracking-wide text-slate-500">
            <tr>
              <th className="px-4 py-3 text-left">User</th>
              <th className="px-4 py-3 text-left">Role</th>
              <th className="px-4 py-3 text-left">Families</th>
              <th className="px-4 py-3 text-right">Actions</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-slate-100">
            {users.length === 0 ? (
              <tr>
                <td colSpan={4} className="px-4 py-8 text-center text-sm text-slate-500">
                  No users yet.
                </td>
              </tr>
            ) : (
              users.map(u => (
                <UserRow
                  key={u.id}
                  user={u}
                  families={families}
                  isSelf={u.id === currentUserId}
                />
              ))
            )}
          </tbody>
        </table>
      </section>
    </div>
  );
}
