// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestLegacyFKAndTTLAuthorizationSetting covers the
// sql.auth.legacy_fk_and_ttl_authorization_unsafe.enabled escape hatch as it
// applies to foreign-key cascades. The default (off) behavior -- the cascade,
// and the user-defined code it fires, run as the referencing table's owner --
// is covered by the fk_cascade_owner logic test. This test covers what turning
// the escape hatch on does, which the logic test cannot: an unsafe setting can
// only be set by pasting back a per-session interlock key, so it is set here by
// overriding it directly.
//
// Three things are checked:
//
//   - the setting is registered as unsafe (a plain SET CLUSTER SETTING is
//     refused and directs the operator to the interlock);
//   - under the legacy model, a cascade runs as the user issuing the
//     mutation, so that user needs privileges on the referencing table and the
//     trigger the cascade fires observes that user;
//   - flipping the setting invalidates a cached plan, so a statement that was
//     already planned under one model is rebuilt under the other. This
//     matters because the setting selects whether the mutating user's SELECT
//     privilege on the referencing table is checked at build time, and a stale
//     plan would keep skipping that check.
func TestLegacyFKAndTTLAuthorizationSetting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(sqlDB)

	// deleter owns parent and issues the DELETE. childowner owns child, the
	// trigger function, and the audit table the trigger writes to. deleter is
	// deliberately given no privileges on child.
	db.Exec(t, `CREATE USER deleter WITH PASSWORD 'test'`)
	db.Exec(t, `CREATE USER childowner`)
	db.Exec(t, `CREATE TABLE parent (id INT PRIMARY KEY)`)
	db.Exec(t, `ALTER TABLE parent OWNER TO deleter`)
	db.Exec(t, `GRANT CREATE ON parent TO childowner`)
	db.Exec(t, `CREATE TABLE audit (who STRING)`)
	db.Exec(t, `ALTER TABLE audit OWNER TO childowner`)
	db.Exec(t, `GRANT INSERT ON audit TO public`)
	db.Exec(t, `CREATE FUNCTION log_del() RETURNS TRIGGER LANGUAGE PLpgSQL AS $$
		BEGIN
			INSERT INTO audit VALUES (current_user);
			RETURN OLD;
		END
	$$`)
	db.Exec(t, `ALTER FUNCTION log_del() OWNER TO childowner`)
	db.Exec(t, `GRANT EXECUTE ON FUNCTION log_del() TO public`)
	db.Exec(t,
		`CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id) ON DELETE CASCADE)`)
	db.Exec(t, `ALTER TABLE child OWNER TO childowner`)
	db.Exec(t,
		`CREATE TRIGGER t_before BEFORE DELETE ON child FOR EACH ROW EXECUTE FUNCTION log_del()`)
	db.Exec(t, `INSERT INTO parent VALUES (1), (2), (3)`)
	db.Exec(t, `INSERT INTO child VALUES (10, 1), (20, 2), (30, 3)`)

	// deleter connects once and reuses the connection for every DELETE below.
	deleterDB := srv.SQLConn(t,
		serverutils.UserPassword("deleter", "test"), serverutils.ClientCerts(false))

	// The DELETE is issued through a prepared statement so that its memo is
	// cached and reused across the setting flips below. That is what makes the
	// staleness check load-bearing: an unprepared statement with a different
	// literal each time would be re-planned regardless.
	deleteStmt, err := deleterDB.Prepare(`DELETE FROM parent WHERE id = $1`)
	require.NoError(t, err)
	defer func() { _ = deleteStmt.Close() }()

	// whoRanTrigger returns and clears the identity the trigger observed.
	whoRanTrigger := func(t *testing.T) string {
		t.Helper()
		var who string
		db.QueryRow(t, `SELECT who FROM audit`).Scan(&who)
		db.Exec(t, `DELETE FROM audit`)
		return who
	}

	t.Run("setting is unsafe", func(t *testing.T) {
		_, err := sqlDB.Exec(
			`SET CLUSTER SETTING sql.auth.legacy_fk_and_ttl_authorization_unsafe.enabled = true`)
		require.ErrorContains(t, err, "may cause cluster instability or data corruption")
		require.ErrorContains(t, err, "unsafe_setting_interlock_key")
	})

	t.Run("default runs cascade as child owner", func(t *testing.T) {
		// deleter holds nothing on child, yet the cascade succeeds. This also
		// populates the plan cache for deleteStmt.
		_, err := deleteStmt.Exec(1)
		require.NoError(t, err)
		require.Equal(t, "childowner", whoRanTrigger(t))
	})

	// Flip to the legacy model. Setting it directly rather than through SET
	// CLUSTER SETTING bypasses the interlock, which is covered above.
	sqlclustersettings.LegacyFKAndTTLAuthorization.Override(ctx, &srv.ClusterSettings().SV, true)

	t.Run("legacy model requires privileges on the referencing table", func(t *testing.T) {
		// deleteStmt was already planned above under the child-owner identity,
		// where the SELECT-on-child check is skipped. The cached memo must be
		// discarded now that the setting has changed, and the rebuilt plan must
		// perform the check and fail.
		_, err := deleteStmt.Exec(2)
		require.ErrorContains(t, err, "user deleter does not have SELECT privilege on relation child")
	})

	t.Run("legacy model runs cascade as the mutating user", func(t *testing.T) {
		db.Exec(t, `GRANT SELECT, DELETE ON child TO deleter`)
		_, err := deleteStmt.Exec(2)
		require.NoError(t, err)
		require.Equal(t, "deleter", whoRanTrigger(t))
	})

	// Flipping back must again invalidate the cached plan, so the cascade
	// returns to the child owner's identity.
	sqlclustersettings.LegacyFKAndTTLAuthorization.Override(ctx, &srv.ClusterSettings().SV, false)

	t.Run("clearing the setting restores the child owner", func(t *testing.T) {
		_, err := deleteStmt.Exec(3)
		require.NoError(t, err)
		require.Equal(t, "childowner", whoRanTrigger(t))
	})
}

// TestCascadeIdentityPrivilegeShift pins down which privileges a cross-owner
// cascade needs under the legacy and default authorization models, using the
// escape hatch to run the identical scenario both ways.
//
// Under the legacy model the cascade runs as the user issuing the mutation,
// so a cross-owner cascade works only if that user was granted SELECT plus
// the DML privilege the referential action performs on the referencing table.
// Under the default model the referential action is authorized by the foreign
// key itself and runs as the referencing table's owner, and the foreign-key
// existence check for a referenced value the action writes back (ON UPDATE
// CASCADE here) is likewise authorized by the foreign key -- so the identical
// cascade needs no grants at all, on either table, for either user.
func TestCascadeIdentityPrivilegeShift(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(sqlDB)

	db.Exec(t, `CREATE USER mutator WITH PASSWORD 'test'`)
	db.Exec(t, `CREATE USER childowner`)
	db.Exec(t, `CREATE TABLE parent (id INT PRIMARY KEY, uid INT UNIQUE)`)
	db.Exec(t, `ALTER TABLE parent OWNER TO mutator`)
	// childowner gets only what it needs to declare the foreign key, and
	// specifically not SELECT on parent.
	db.Exec(t, `GRANT CREATE ON parent TO childowner`)
	db.Exec(t,
		`CREATE TABLE child (id INT PRIMARY KEY, puid INT REFERENCES parent(uid) ON UPDATE CASCADE)`)
	db.Exec(t, `ALTER TABLE child OWNER TO childowner`)
	// The grants a working cross-owner cascade requires under the legacy
	// model: the cascade runs as the mutating user, so that user needs access
	// to the child.
	db.Exec(t, `GRANT SELECT, UPDATE ON child TO mutator`)
	db.Exec(t, `INSERT INTO parent VALUES (1, 10)`)
	db.Exec(t, `INSERT INTO child VALUES (100, 10)`)

	mutatorDB := srv.SQLConn(t,
		serverutils.UserPassword("mutator", "test"), serverutils.ClientCerts(false))

	childPUID := func(t *testing.T) string {
		t.Helper()
		var puid string
		db.QueryRow(t, `SELECT puid::STRING FROM child WHERE id = 100`).Scan(&puid)
		return puid
	}

	// Legacy model first. The cascade runs as mutator, whose grants on child
	// authorize the write-back, and whose ownership of parent satisfies the
	// cascade's own foreign-key existence check.
	sqlclustersettings.LegacyFKAndTTLAuthorization.Override(ctx, &srv.ClusterSettings().SV, true)

	t.Run("legacy model: cross-owner ON UPDATE CASCADE succeeds", func(t *testing.T) {
		_, err := mutatorDB.Exec(`UPDATE parent SET uid = 11 WHERE id = 1`)
		require.NoError(t, err)
		require.Equal(t, "11", childPUID(t))
	})

	// Default model: the same statement with every grant stripped. mutator
	// holds nothing on child and childowner holds nothing on parent; the
	// foreign key alone authorizes the cascade and its existence check.
	sqlclustersettings.LegacyFKAndTTLAuthorization.Override(ctx, &srv.ClusterSettings().SV, false)

	t.Run("default model: the cascade needs no grants on either side", func(t *testing.T) {
		db.Exec(t, `REVOKE SELECT, UPDATE ON child FROM mutator`)
		_, err := mutatorDB.Exec(`UPDATE parent SET uid = 12 WHERE id = 1`)
		require.NoError(t, err)
		require.Equal(t, "12", childPUID(t))
	})

	// ...and the reverse: under the legacy model those grants were required.
	sqlclustersettings.LegacyFKAndTTLAuthorization.Override(ctx, &srv.ClusterSettings().SV, true)

	t.Run("legacy model: mutating user did need privileges on child", func(t *testing.T) {
		_, err := mutatorDB.Exec(`UPDATE parent SET uid = 13 WHERE id = 1`)
		require.ErrorContains(t, err,
			"user mutator does not have SELECT privilege on relation child")
		// The row is unchanged: the statement failed rather than partially
		// applied.
		require.Equal(t, "12", childPUID(t))
	})
}

// TestCascadeTriggerIdentityShift covers the identity that user-defined code
// fired by a cascade runs as. Unlike the privilege shift on the referential
// action itself, this applies to every referential action, including
// ON DELETE CASCADE.
//
// Under the legacy model a trigger on the referencing table runs as whoever
// issued the mutation; under the default model it runs as the referencing
// table's owner. Where the mutating user is the more privileged of the two --
// which is the common shape, since it is usually the one driving the
// workload -- code that succeeded under the legacy model can start failing.
func TestCascadeTriggerIdentityShift(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(sqlDB)

	db.Exec(t, `CREATE USER mutator WITH PASSWORD 'test'`)
	db.Exec(t, `CREATE USER childowner`)
	db.Exec(t, `CREATE TABLE parent (id INT PRIMARY KEY)`)
	db.Exec(t, `ALTER TABLE parent OWNER TO mutator`)
	db.Exec(t, `GRANT CREATE ON parent TO childowner`)
	// audit is writable by mutator but not by childowner.
	db.Exec(t, `CREATE TABLE audit (who STRING)`)
	db.Exec(t, `GRANT INSERT ON audit TO mutator`)
	db.Exec(t, `CREATE FUNCTION log_del() RETURNS TRIGGER LANGUAGE PLpgSQL AS $$
		BEGIN
			INSERT INTO audit VALUES (current_user);
			RETURN OLD;
		END
	$$`)
	db.Exec(t, `GRANT EXECUTE ON FUNCTION log_del() TO public`)
	db.Exec(t,
		`CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id) ON DELETE CASCADE)`)
	db.Exec(t, `ALTER TABLE child OWNER TO childowner`)
	db.Exec(t,
		`CREATE TRIGGER t BEFORE DELETE ON child FOR EACH ROW EXECUTE FUNCTION log_del()`)
	db.Exec(t, `GRANT SELECT, DELETE ON child TO mutator`)
	db.Exec(t, `INSERT INTO parent VALUES (1), (2)`)
	db.Exec(t, `INSERT INTO child VALUES (10, 1), (20, 2)`)

	mutatorDB := srv.SQLConn(t,
		serverutils.UserPassword("mutator", "test"), serverutils.ClientCerts(false))

	// Legacy model: the trigger runs as the deleting user, who can write to
	// audit, so an ON DELETE CASCADE succeeds.
	sqlclustersettings.LegacyFKAndTTLAuthorization.Override(ctx, &srv.ClusterSettings().SV, true)

	t.Run("legacy model: cascade-fired trigger runs as the deleter", func(t *testing.T) {
		_, err := mutatorDB.Exec(`DELETE FROM parent WHERE id = 1`)
		require.NoError(t, err)
		db.CheckQueryResults(t, `SELECT who FROM audit`, [][]string{{"mutator"}})
	})

	// Default model: the same trigger now runs as childowner, who cannot write
	// to audit, so the same statement shape fails.
	sqlclustersettings.LegacyFKAndTTLAuthorization.Override(ctx, &srv.ClusterSettings().SV, false)

	t.Run("default model: the same cascade now fails", func(t *testing.T) {
		_, err := mutatorDB.Exec(`DELETE FROM parent WHERE id = 2`)
		require.ErrorContains(t, err,
			"user childowner does not have INSERT privilege on relation audit")
		// Nothing was deleted: the failed trigger rolled the statement back.
		db.CheckQueryResults(t, `SELECT count(*) FROM parent`, [][]string{{"1"}})
	})
}

// TestLegacyFKAndTTLAuthorizationIsAFaithfulRollback asserts that the escape
// hatch really does restore the legacy authorization model, rather than merely
// approximating it. This matters because the hatch is the only way back for an
// application the model change breaks: if it were not faithful, such an
// application would have no route to a working configuration.
//
// Two scenarios pin the two halves of the legacy model:
//
//   - The referential-action half is lifted verbatim from the "Delete cascade
//     without privileges" case in the fk logic test as it read when cascades
//     ran as the mutating user. It is a three-stage ladder: the user issuing
//     the delete needed SELECT on the referencing table, then DELETE on it,
//     before the cascade would run. Each stage must reproduce exactly,
//     including the error strings, which name the relation the user lacked
//     access to.
//
//   - The existence-check half replays the reproduction from
//     cockroachdb/cockroach#172490 verbatim: a child-table owner holding no
//     privileges on the referenced table could not write to its own table,
//     because the outbound foreign-key check demanded SELECT on the referenced
//     table from the writing user -- at plan time, so even a NULL foreign-key
//     value failed.
//
// The logic tests cannot cover this: the setting is unsafe, so it can only be
// set by pasting back a per-session interlock key.
func TestLegacyFKAndTTLAuthorizationIsAFaithfulRollback(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	db := sqlutils.MakeSQLRunner(sqlDB)

	sqlclustersettings.LegacyFKAndTTLAuthorization.Override(ctx, &srv.ClusterSettings().SV, true)

	t.Run("cascades run as the mutating user and require its privileges", func(t *testing.T) {
		db.Exec(t, `CREATE USER testuser WITH PASSWORD 'test'`)
		db.Exec(t, `CREATE DATABASE d`)
		db.Exec(t, `CREATE TABLE d.a (id STRING PRIMARY KEY)`)
		db.Exec(t, `CREATE TABLE d.b (id STRING PRIMARY KEY, a_id STRING REFERENCES d.a ON DELETE CASCADE)`)
		db.Exec(t, `INSERT INTO d.a VALUES ('a1')`)
		db.Exec(t, `INSERT INTO d.b VALUES ('b1', 'a1')`)
		db.Exec(t, `GRANT ALL ON DATABASE d TO testuser`)
		db.Exec(t, `GRANT ALL ON d.a TO testuser`)

		testuserDB := srv.SQLConn(t,
			serverutils.UserPassword("testuser", "test"), serverutils.ClientCerts(false),
			serverutils.DBName("d"))

		// Stage 1: no privileges on the child. The cascade runs as testuser, so
		// planning requires testuser to hold SELECT on d.b.
		_, err := testuserDB.Exec(`DELETE FROM d.a WHERE id = 'a1'`)
		require.ErrorContains(t, err, "user testuser does not have SELECT privilege on relation b")

		// Stage 2: SELECT is not enough; the cascade deletes from d.b as testuser.
		db.Exec(t, `GRANT SELECT ON d.b TO testuser`)
		_, err = testuserDB.Exec(`DELETE FROM d.a WHERE id = 'a1'`)
		require.ErrorContains(t, err, "user testuser does not have DELETE privilege on relation b")

		// Stage 3: with both, the cascade runs as testuser and succeeds.
		db.Exec(t, `GRANT DELETE ON d.b TO testuser`)
		res, err := testuserDB.Exec(`DELETE FROM d.a WHERE id = 'a1'`)
		require.NoError(t, err)
		rows, err := res.RowsAffected()
		require.NoError(t, err)
		require.EqualValues(t, 1, rows)
		db.CheckQueryResults(t, `SELECT count(*) FROM d.b`, [][]string{{"0"}})
	})

	t.Run("foreign-key checks require SELECT on the referenced table", func(t *testing.T) {
		db.Exec(t, `CREATE USER owner_p`)
		db.Exec(t, `CREATE USER owner_c WITH PASSWORD 'test'`)
		db.Exec(t, `CREATE TABLE parent (id INT PRIMARY KEY)`)
		db.Exec(t, `CREATE TABLE child (id INT PRIMARY KEY, pid INT REFERENCES parent(id))`)
		db.Exec(t, `ALTER TABLE parent OWNER TO owner_p`)
		db.Exec(t, `ALTER TABLE child OWNER TO owner_c`)
		db.Exec(t, `INSERT INTO parent VALUES (1)`)

		// owner_c owns child and may write to it, but holds no privileges on
		// parent.
		ownerCDB := srv.SQLConn(t,
			serverutils.UserPassword("owner_c", "test"), serverutils.ClientCerts(false))

		_, err := ownerCDB.Exec(`INSERT INTO child VALUES (10, 1)`)
		require.ErrorContains(t, err,
			"user owner_c does not have SELECT privilege on relation parent")

		// The check and its privilege requirement are built at plan time
		// regardless of the values written, so even a NULL foreign-key value
		// fails.
		_, err = ownerCDB.Exec(`INSERT INTO child VALUES (11, NULL)`)
		require.ErrorContains(t, err,
			"user owner_c does not have SELECT privilege on relation parent")

		// SELECT on the referenced table is exactly what the legacy model
		// demands.
		db.Exec(t, `GRANT SELECT ON parent TO owner_c`)
		_, err = ownerCDB.Exec(`INSERT INTO child VALUES (10, 1)`)
		require.NoError(t, err)
	})
}
