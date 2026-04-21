// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestPointSelectFastPath verifies that the experimental hardcoded
// point-select fast path returns byte-for-byte identical results to
// the normal SQL execution path. We toggle the cluster setting on and
// off and assert that a prepared `SELECT v FROM kv WHERE k = $1`
// against many keys returns the same values either way.
//
// This test guards against the prototype's main risk: that the manual
// key-encoding / value-decoding paths drift from what the optimizer
// and row.Fetcher would produce.
func TestPointSelectFastPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	// Disable the optimizer's placeholder fast path so the slow path
	// is the unspecialized one — same configuration the benchmark uses.
	xform.PlaceholderFastPathEnabled.Override(ctx, &st.SV, false)

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		Settings: st,
		Insecure: true,
	})
	defer srv.Stopper().Stop(ctx)
	srv.SQLServer().(*sql.Server).GetExecutorConfig().LicenseEnforcer.Disable(ctx)

	pgURL, cleanup := srv.ApplicationLayer().PGUrl(t, serverutils.DBName("defaultdb"))
	defer cleanup()

	// Setup: create and populate the table.
	setupConn, err := pgx.Connect(ctx, pgURL.String())
	require.NoError(t, err)
	defer func() { _ = setupConn.Close(ctx) }()

	_, err = setupConn.Exec(ctx, `CREATE TABLE kv (k INT8 PRIMARY KEY, v BYTES)`)
	require.NoError(t, err)

	// Use a small but non-trivial dataset: enough keys to exercise
	// boundary cases (k=0, large k, missing k) without being slow.
	const numRows = 100
	for i := 0; i < numRows; i++ {
		_, err = setupConn.Exec(ctx, `INSERT INTO kv (k, v) VALUES ($1, $2)`,
			int64(i), []byte(fmt.Sprintf("value-%d", i)))
		require.NoError(t, err)
	}

	// queryAll executes the prepared SELECT against `keys` on a fresh
	// connection (so the fast-path detection cache is per-connection).
	// Returns one entry per requested key: the value bytes if the row
	// existed, or nil if not.
	queryAll := func(t *testing.T, fastPathEnabled bool, keys []int64) [][]byte {
		t.Helper()
		_, err := setupConn.Exec(ctx, fmt.Sprintf(
			`SET CLUSTER SETTING sql.fast_path.point_select.enabled = %v`, fastPathEnabled))
		require.NoError(t, err)
		conn, err := pgx.Connect(ctx, pgURL.String())
		require.NoError(t, err)
		defer func() { _ = conn.Close(ctx) }()
		_, err = conn.Prepare(ctx, "ps", `SELECT v FROM kv WHERE k = $1`)
		require.NoError(t, err)
		out := make([][]byte, len(keys))
		for i, k := range keys {
			var v []byte
			scanErr := conn.QueryRow(ctx, "ps", k).Scan(&v)
			if errors.Is(scanErr, pgx.ErrNoRows) {
				out[i] = nil
				continue
			}
			require.NoErrorf(t, scanErr, "k=%d", k)
			out[i] = v
		}
		return out
	}

	// Mix of present and missing keys.
	testKeys := []int64{0, 1, 42, 99, numRows, numRows + 1, -1}

	slow := queryAll(t, false /* fast path off */, testKeys)
	fast := queryAll(t, true /* fast path on */, testKeys)

	require.Equal(t, len(slow), len(fast))
	for i, k := range testKeys {
		require.Equalf(t, slow[i], fast[i],
			"divergence at k=%d: slow=%v fast=%v", k, slow[i], fast[i])
	}

	// Assert the expected shape: present keys return non-nil bytes,
	// missing keys return nil. (Catches the case where both paths are
	// equivalently broken.)
	for i, k := range testKeys {
		switch {
		case k >= 0 && k < numRows:
			require.NotNilf(t, fast[i], "expected row for k=%d", k)
			require.Equalf(t, []byte(fmt.Sprintf("value-%d", k)), fast[i], "k=%d", k)
		default:
			require.Nilf(t, fast[i], "expected no row for k=%d", k)
		}
	}
}
