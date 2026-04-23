// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	gosql "database/sql"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestPGLocksAdvisorySingleKeyExclusive validates pg_locks for a single-int8
// transaction advisory key (classid=0, objid=key, objsubid=1) and that the
// row clears after commit.
func TestPGLocksAdvisorySingleKeyExclusive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	_, err := db.ExecContext(ctx, `SET database = defaultdb`)
	require.NoError(t, err)

	const k = 10001
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, k)
	require.NoError(t, err)

	var n int
	err = tx.QueryRowContext(ctx, `
		SELECT count(*) FROM pg_catalog.pg_locks
		WHERE locktype = 'advisory' AND granted AND classid = 0 AND objid = $1::oid AND objsubid = 1
		AND relation IS NULL AND page IS NULL AND NOT fastpath
	`, k).Scan(&n)
	require.NoError(t, err)
	require.Equal(t, 1, n, "expected one granted advisory row for the held lock")

	var mode string
	var fast bool
	var rel, vxid gosql.NullString
	var vtxn string
	err = tx.QueryRowContext(ctx, `
		SELECT mode, fastpath, relation, virtualxid, virtualtransaction
		FROM pg_catalog.pg_locks
		WHERE locktype = 'advisory' AND granted AND objid = $1::oid
	`, k).Scan(&mode, &fast, &rel, &vxid, &vtxn)
	require.NoError(t, err)
	require.Equal(t, "ExclusiveLock", mode)
	require.False(t, fast)
	require.False(t, rel.Valid) // NULL relation for advisory
	require.False(t, vxid.Valid) // no virtual xid
	require.Greater(t, len(vtxn), 2)
	require.Equal(t, "0/", vtxn[0:2], "virtualtransaction is like PostgreSQL '0/pid'")

	require.NoError(t, tx.Commit())

	err = db.QueryRowContext(ctx, `SELECT count(*)::int FROM pg_catalog.pg_locks WHERE objid = $1`, k).Scan(&n)
	require.NoError(t, err)
	require.Equal(t, 0, n)
}

// TestPGLocksAdvisorySingleKeyShared matches ShareLock and objsubid 1.
func TestPGLocksAdvisorySingleKeyShared(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	_, err := db.ExecContext(ctx, `SET database = defaultdb`)
	require.NoError(t, err)

	const k = 10002
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()

	_, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock_shared($1)`, k)
	require.NoError(t, err)

	var mode string
	err = tx.QueryRowContext(ctx, `SELECT mode FROM pg_catalog.pg_locks WHERE locktype = 'advisory' AND granted AND objid = $1`, k).Scan(&mode)
	require.NoError(t, err)
	require.Equal(t, "ShareLock", mode)
	require.NoError(t, tx.Commit())
}

// TestPGLocksAdvisoryTwoInt4 maps to objsubid=2 with classid/objid as the two int4 parts.
func TestPGLocksAdvisoryTwoInt4(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	_, err := db.ExecContext(ctx, `SET database = defaultdb`)
	require.NoError(t, err)

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(7::int4, 8::int4)`)
	require.NoError(t, err)

	var classid, objid int
	var subid int16
	err = tx.QueryRowContext(ctx, `
		SELECT classid::int, objid::int, objsubid::int
		FROM pg_catalog.pg_locks
		WHERE locktype = 'advisory' AND granted AND objsubid = 2
	`).Scan(&classid, &objid, &subid)
	require.NoError(t, err)
	require.Equal(t, 7, classid)
	require.Equal(t, 8, objid)
	require.Equal(t, int16(2), subid)
	require.NoError(t, tx.Commit())
}
