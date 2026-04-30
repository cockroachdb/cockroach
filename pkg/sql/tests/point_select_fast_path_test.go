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
// the normal SQL execution path, for both the primary-key and
// covering-secondary-index lookup shapes. We toggle the cluster
// setting on and off and assert the prepared SELECT returns the
// same values either way.
//
// This test guards against the prototype's main risk: that the
// manual key-encoding / value-decoding paths drift from what the
// optimizer and row.Fetcher would produce.
func TestPointSelectFastPath(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type insertSpec struct {
		sql  string
		args func(i int64, valueBytes []byte) []any
	}
	for _, tc := range []struct {
		name     string
		tableDDL string
		inserts  []insertSpec
		stmt     string
	}{
		{
			name:     "pk",
			tableDDL: `CREATE TABLE kv (k INT8 PRIMARY KEY, v BYTES)`,
			inserts: []insertSpec{{
				sql:  `INSERT INTO kv (k, v) VALUES ($1, $2)`,
				args: func(i int64, v []byte) []any { return []any{i, v} },
			}},
			stmt: `SELECT v FROM kv WHERE k = $1`,
		},
		{
			name: "secondary_index_noncovering",
			tableDDL: `CREATE TABLE kv_idx (
				pk_id INT8 PRIMARY KEY,
				k     INT8 NOT NULL,
				v     BYTES,
				UNIQUE INDEX kv_idx_k (k)
			)`,
			inserts: []insertSpec{{
				sql:  `INSERT INTO kv_idx (pk_id, k, v) VALUES ($1, $1, $2)`,
				args: func(i int64, v []byte) []any { return []any{i, v} },
			}},
			stmt: `SELECT v FROM kv_idx WHERE k = $1`,
		},
		{
			name: "join",
			tableDDL: `CREATE TABLE first (
				id    INT8 PRIMARY KEY,
				k     INT8 NOT NULL,
				fk_id INT8 NOT NULL,
				UNIQUE INDEX first_k (k)
			); CREATE TABLE second (
				id INT8 PRIMARY KEY,
				v  BYTES
			)`,
			// Two per-row inserts: one into each table, run in
			// order so the FK relationship lands as 1:1 between
			// first.id and second.id.
			inserts: []insertSpec{
				{
					sql:  `INSERT INTO first (id, k, fk_id) VALUES ($1, $1, $1)`,
					args: func(i int64, _ []byte) []any { return []any{i} },
				},
				{
					sql:  `INSERT INTO second (id, v) VALUES ($1, $2)`,
					args: func(i int64, v []byte) []any { return []any{i, v} },
				},
			},
			stmt: `SELECT s.v FROM first f JOIN second s ON f.fk_id = s.id WHERE f.k = $1`,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := cluster.MakeTestingClusterSettings()
			xform.PlaceholderFastPathEnabled.Override(ctx, &st.SV, false)

			srv := serverutils.StartServerOnly(t, base.TestServerArgs{
				Settings: st,
				Insecure: true,
			})
			defer srv.Stopper().Stop(ctx)
			srv.SQLServer().(*sql.Server).GetExecutorConfig().LicenseEnforcer.Disable(ctx)

			pgURL, cleanup := srv.ApplicationLayer().PGUrl(t, serverutils.DBName("defaultdb"))
			defer cleanup()

			setupConn, err := pgx.Connect(ctx, pgURL.String())
			require.NoError(t, err)
			defer func() { _ = setupConn.Close(ctx) }()

			_, err = setupConn.Exec(ctx, tc.tableDDL)
			require.NoError(t, err)

			const numRows = 100
			for i := 0; i < numRows; i++ {
				v := []byte(fmt.Sprintf("value-%d", i))
				for _, ins := range tc.inserts {
					_, err = setupConn.Exec(ctx, ins.sql, ins.args(int64(i), v)...)
					require.NoErrorf(t, err, "insert: %s", ins.sql)
				}
			}

			queryAll := func(t *testing.T, fastPathEnabled bool, keys []int64) [][]byte {
				t.Helper()
				_, err := setupConn.Exec(ctx, fmt.Sprintf(
					`SET CLUSTER SETTING sql.fast_path.point_select.enabled = %v`, fastPathEnabled))
				require.NoError(t, err)
				conn, err := pgx.Connect(ctx, pgURL.String())
				require.NoError(t, err)
				defer func() { _ = conn.Close(ctx) }()
				_, err = conn.Prepare(ctx, "ps", tc.stmt)
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

			testKeys := []int64{0, 1, 42, 99, numRows, numRows + 1, -1}
			slow := queryAll(t, false, testKeys)
			fast := queryAll(t, true, testKeys)

			require.Equal(t, len(slow), len(fast))
			for i, k := range testKeys {
				require.Equalf(t, slow[i], fast[i],
					"divergence at k=%d: slow=%v fast=%v", k, slow[i], fast[i])
			}

			for i, k := range testKeys {
				switch {
				case k >= 0 && k < numRows:
					require.NotNilf(t, fast[i], "expected row for k=%d", k)
					require.Equalf(t, []byte(fmt.Sprintf("value-%d", k)), fast[i], "k=%d", k)
				default:
					require.Nilf(t, fast[i], "expected no row for k=%d", k)
				}
			}
		})
	}
}
