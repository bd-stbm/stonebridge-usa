"""RLS isolation test harness for Phase 2a (migration 028).

Simulates admin / single-family client / multi-family client / unmapped
sessions by setting `request.jwt.claims` + `SET ROLE authenticated` inside a
transaction that is always ROLLED BACK, so no persistent change is made. A
real auth.users uid is used (so the FK on app_user/user_family_access holds)
and temporarily demoted to 'client' within the rolled-back txn.

Run AFTER migration 028 is applied. Exits non-zero if any assertion fails.

    python scripts/test_rls_isolation.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from tracker.db import connect

MILLER = "102_93360"
DYNE_AU = "102_93362"

failures: list[str] = []


def check(label: str, ok: bool, detail: str = "") -> None:
    mark = "PASS" if ok else "FAIL"
    print(f"  [{mark}] {label}" + (f"  — {detail}" if detail else ""))
    if not ok:
        failures.append(label)


def visible_sub_clients(cur) -> set[str]:
    cur.execute(
        "SELECT sub_client_node_id, count(*) n FROM position_snapshot "
        "GROUP BY 1 ORDER BY 1"
    )
    return {r["sub_client_node_id"] for r in cur.fetchall() if r["sub_client_node_id"]}


def enter_session(cur, uid: str) -> None:
    """Switch the current txn to act as authenticated user `uid`."""
    cur.execute(
        "SELECT set_config('request.jwt.claims', %s, true)",
        (json.dumps({"sub": uid, "role": "authenticated"}),),
    )
    cur.execute("SET LOCAL ROLE authenticated")


def main() -> int:
    conn = connect()
    cur = conn.cursor()

    # Ground truth (as superuser, RLS bypassed): which sub_clients have data.
    cur.execute(
        "SELECT DISTINCT sub_client_node_id FROM position_snapshot "
        "WHERE sub_client_node_id IS NOT NULL"
    )
    all_subs = {r["sub_client_node_id"] for r in cur.fetchall()}
    print(f"Ground-truth sub_clients with positions: {sorted(all_subs)}")

    cur.execute("SELECT user_id, role FROM app_user ORDER BY role")
    admins = [r for r in cur.fetchall() if r["role"] == "admin"]
    if not admins:
        print("No admin in app_user — did Part A seed run? Aborting.")
        return 1
    uid = str(admins[0]["user_id"])
    print(f"Using auth uid {uid} (currently admin) for simulations.\n")

    # --- 1. ADMIN sees everything ---------------------------------------
    print("1. Admin session:")
    try:
        enter_session(cur, uid)
        cur.execute("SELECT is_admin() a, current_user_sub_clients() s")
        r = cur.fetchone()
        check("is_admin() true for admin", r["a"] is True, f"is_admin={r['a']}")
        seen = visible_sub_clients(cur)
        check("admin sees ALL sub_clients", seen == all_subs,
              f"{len(seen)}/{len(all_subs)}")
    finally:
        conn.rollback()

    # --- 2. Single-family client (Miller only) --------------------------
    print("2. Client mapped to Miller only:")
    try:
        cur.execute("UPDATE app_user SET role='client' WHERE user_id=%s", (uid,))
        cur.execute("DELETE FROM user_family_access WHERE user_id=%s", (uid,))
        cur.execute(
            "INSERT INTO user_family_access(user_id, sub_client_node_id) VALUES (%s,%s)",
            (uid, MILLER),
        )
        enter_session(cur, uid)
        cur.execute("SELECT is_admin() a, current_user_sub_clients() s")
        r = cur.fetchone()
        check("is_admin() false for client", r["a"] is False)
        check("current_user_sub_clients = [Miller]", set(r["s"]) == {MILLER},
              str(r["s"]))
        seen = visible_sub_clients(cur)
        check("position_snapshot = Miller only", seen == {MILLER}, str(seen))
        # transaction_log
        cur.execute("SELECT DISTINCT sub_client_node_id FROM transaction_log "
                    "WHERE sub_client_node_id IS NOT NULL")
        tl = {x["sub_client_node_id"] for x in cur.fetchall()}
        check("transaction_log = Miller only", tl <= {MILLER}, str(tl))
        # entity_attribution
        cur.execute("SELECT DISTINCT sub_client_node_id FROM entity_attribution "
                    "WHERE sub_client_node_id IS NOT NULL")
        ea = {x["sub_client_node_id"] for x in cur.fetchall()}
        check("entity_attribution = Miller only", ea == {MILLER}, str(ea))
        # security_invoker views must inherit the scoping
        cur.execute("SELECT DISTINCT sub_client_alias FROM v_positions_refreshed")
        va = {x["sub_client_alias"] for x in cur.fetchall()}
        check("v_positions_refreshed view scoped", len(va) <= 1, str(va))
        # entity rows: only Miller's accounts visible (RLS already filtered)
        cur.execute("SELECT count(DISTINCT sub_client_node_id) n FROM entity "
                    "WHERE sub_client_node_id IS NOT NULL")
        check("entity scoped to one family", cur.fetchone()["n"] == 1)
    finally:
        conn.rollback()

    # --- 3. Multi-family client (Miller + Dyne AU) ----------------------
    print("3. Client mapped to Miller + Dyne (AU):")
    try:
        cur.execute("UPDATE app_user SET role='client' WHERE user_id=%s", (uid,))
        cur.execute("DELETE FROM user_family_access WHERE user_id=%s", (uid,))
        cur.executemany(
            "INSERT INTO user_family_access(user_id, sub_client_node_id) VALUES (%s,%s)",
            [(uid, MILLER), (uid, DYNE_AU)],
        )
        enter_session(cur, uid)
        seen = visible_sub_clients(cur)
        check("position_snapshot = {Miller, Dyne AU}", seen == {MILLER, DYNE_AU},
              str(seen))
    finally:
        conn.rollback()

    # --- 4. Unmapped client sees nothing --------------------------------
    print("4. Client with no family mapping:")
    try:
        cur.execute("UPDATE app_user SET role='client' WHERE user_id=%s", (uid,))
        cur.execute("DELETE FROM user_family_access WHERE user_id=%s", (uid,))
        enter_session(cur, uid)
        seen = visible_sub_clients(cur)
        check("position_snapshot empty", seen == set(), str(seen))
        cur.execute("SELECT count(*) n FROM v_positions_refreshed")
        check("v_positions_refreshed empty", cur.fetchone()["n"] == 0)
    finally:
        conn.rollback()

    # --- 5. Shared reference data still readable by a client ------------
    print("5. Client can still read shared reference data:")
    try:
        cur.execute("UPDATE app_user SET role='client' WHERE user_id=%s", (uid,))
        cur.execute("DELETE FROM user_family_access WHERE user_id=%s", (uid,))
        cur.execute("INSERT INTO user_family_access(user_id, sub_client_node_id) "
                    "VALUES (%s,%s)", (uid, MILLER))
        enter_session(cur, uid)
        cur.execute("SELECT count(*) n FROM security")
        check("security readable", cur.fetchone()["n"] > 0)
        cur.execute("SELECT count(*) n FROM index_price_history")
        check("index_price_history readable", cur.fetchone()["n"] > 0)
    finally:
        conn.rollback()

    conn.close()
    print()
    if failures:
        print(f"RESULT: {len(failures)} FAILURE(S): {failures}")
        return 1
    print("RESULT: all isolation checks PASSED.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
