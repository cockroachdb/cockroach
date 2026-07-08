// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ttljob_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestRowLevelTTLCheckConstraintNoEscalation is a regression test for a
// privilege-escalation vector via row-level TTL.
//
// A user who holds REFERENCES (and SELECT) on a TTL-enabled parent table can
// create a child table with an ON DELETE SET NULL foreign key and a CHECK
// constraint that calls a SECURITY INVOKER UDF. CockroachDB permits VOLATILE
// UDFs in CHECK constraints, and an ON DELETE SET NULL cascade re-evaluates the
// child's CHECK constraints (the FK column is written). When the TTL job later
// deletes an expired parent row, the cascade evaluates that CHECK — and if the
// CHECK's UDF runs with an elevated identity, its body can run arbitrary SQL via
// crdb_internal.execute_internally (here, GRANT admin), escalating the attacker.
//
// The attacker's UDF swallows errors so it can be planted while unprivileged
// (execute_internally requires REPAIRCLUSTER, which the attacker lacks): the
// grant silently fails during the attacker's own INSERT and would only succeed
// if the cascade ran it as a privileged identity.
//
// Two escalation paths are closed and both are exercised here, keyed on who owns
// the parent TTL table:
//
//   - node: the TTL job runs its statements as the table's owner, not node, so
//     the cascade never executes with node's privilege-check bypass.
//   - the owner, when the owner is a superuser (root/admin): even though the job
//     then runs as a superuser, the cascade — and the CHECK it re-evaluates —
//     runs as the referencing (child) table's owner (the attacker), a
//     non-superuser, so execute_internally's REPAIRCLUSTER check still fails.
//
// In both cases no admin is granted, and the TTL delete still succeeds even
// though the child is owned by a different, unprivileged user.
func TestRowLevelTTLCheckConstraintNoEscalation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testutils.RunTrueAndFalse(t, "parent-owned-by-superuser", func(t *testing.T, superuserOwner bool) {
		th, cleanup := newRowLevelTTLTestJobTestHelper(
			t,
			&sql.TTLTestingKnobs{
				AOSTDuration:     &zeroDuration,
				ReturnStatsError: true,
			},
			false, /* testMultiTenant */
			1,     /* numNodes */
		)
		defer cleanup()

		root := th.sqlDB
		root.Exec(t, `CREATE USER `+username.TestUser)

		// Parent TTL table with one expired row. When superuserOwner is false the
		// table is owned by a non-admin; otherwise it is left owned by root (an
		// admin), the more dangerous case — the TTL job then runs as a superuser.
		root.Exec(t, `CREATE TABLE parent (id INT PRIMARY KEY, expire_at TIMESTAMPTZ) WITH (ttl_expiration_expression = 'expire_at')`)
		if !superuserOwner {
			root.Exec(t, `CREATE USER victim`)
			root.Exec(t, `ALTER TABLE parent OWNER TO victim`)
		}
		// Creating a foreign key requires CREATE on the referenced table on this
		// release (REFERENCES on releases with the newer foreign-key grant
		// semantics).
		root.Exec(t, `GRANT CREATE, SELECT ON parent TO `+username.TestUser)
		root.Exec(t, `GRANT CREATE, USAGE ON SCHEMA defaultdb.public TO `+username.TestUser)
		root.Exec(t, `INSERT INTO parent VALUES (1, '2020-01-01')`)

		// The attacker plants a child table whose CHECK constraint runs a UDF that
		// grants itself admin via execute_internally.
		attacker := sqlutils.MakeSQLRunner(th.testCluster.Server(0).ApplicationLayer().SQLConn(t, serverutils.User(username.TestUser)))
		attacker.Exec(t, `
CREATE FUNCTION pwn(x INT) RETURNS BOOL VOLATILE LANGUAGE PLpgSQL AS $$
DECLARE
  r STRING;
BEGIN
  SELECT g INTO r FROM crdb_internal.execute_internally('GRANT admin TO testuser') AS g LIMIT 1;
  RETURN true;
EXCEPTION WHEN OTHERS THEN
  RETURN true;
END
$$`)
		attacker.Exec(t, `CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id) ON DELETE SET NULL, CHECK (pwn(pid)))`)
		attacker.Exec(t, `INSERT INTO child VALUES (10, 1)`)

		isAttackerAdmin := func() bool {
			var n int
			root.QueryRow(t,
				`SELECT count(*) FROM [SHOW GRANTS ON ROLE admin] WHERE member = $1`,
				username.TestUser,
			).Scan(&n)
			return n > 0
		}
		require.False(t, isAttackerAdmin(), "attacker must not be admin before the TTL job runs")

		// Run the TTL job. It deletes the expired parent row, cascading SET NULL
		// into the child (owned by the attacker) and evaluating the malicious
		// CHECK. The job must succeed and the attacker must not gain admin.
		th.waitForScheduledJob(t, jobs.StateSucceeded, "")

		require.False(t, isAttackerAdmin(),
			"SECURITY: the TTL job evaluated the attacker's CHECK-constraint UDF with "+
				"elevated privileges, allowing the attacker to grant themselves admin")

		// The cascade actually ran: the expired parent row is gone and the child's
		// FK column was set to NULL.
		require.Empty(t, root.QueryStr(t, `SELECT id FROM parent`))
		root.CheckQueryResults(t, `SELECT id, pid FROM child`, [][]string{{"10", "NULL"}})
	})
}
