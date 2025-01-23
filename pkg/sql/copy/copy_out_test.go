// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package copy

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// TestCopyOutTransaction tests COPY TO behaves as appropriate in a transaction.
func TestCopyOutTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	_, err := db.Exec(`
		CREATE TABLE t (
			i INT PRIMARY KEY
		);
	`)
	require.NoError(t, err)

	pgURL, cleanupGoDB, err := s.PGUrlE(
		serverutils.CertsDirPrefix("StartServer"),
		serverutils.User(username.RootUser),
	)
	require.NoError(t, err)
	s.AppStopper().AddCloser(stop.CloserFn(func() { cleanupGoDB() }))
	config, err := pgx.ParseConfig(pgURL.String())
	require.NoError(t, err)

	conn, err := pgx.ConnectConfig(ctx, config)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close(ctx)) }()

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)

	_, err = tx.Exec(ctx, "INSERT INTO t VALUES (1)")
	require.NoError(t, err)

	var buf bytes.Buffer
	_, err = tx.Conn().PgConn().CopyTo(ctx, &buf, "COPY t TO STDOUT")
	require.NoError(t, err)
	require.Equal(t, "1\n", buf.String())

	require.NoError(t, tx.Rollback(ctx))
}

// TestCopyOutRandom tests COPY TO on a set of random rows matches pgx.
func TestCopyOutRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numRows = 100

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	colNames := []string{"id"}
	colTypes := []*types.T{types.Int}
	colStr := "id INT PRIMARY KEY"
	for _, typ := range types.Scalar {
		colName := tree.NameString(strings.ToLower(typ.SQLString()))
		colNames = append(colNames, colName)
		colTypes = append(colTypes, typ)
		colStr += fmt.Sprintf(", %s %s", colName, typ.SQLString())
	}

	rng := randutil.NewTestRandWithSeed(0)
	sqlutils.CreateTable(
		t, db, "t",
		colStr,
		numRows,
		func(row int) []tree.Datum {
			datums := []tree.Datum{
				tree.NewDInt(tree.DInt(row)),
			}
			for _, typ := range colTypes[1:] {
				datums = append(datums, randgen.RandDatum(rng, typ, false))
			}
			return datums
		},
	)

	// Use pgx for this next bit as it allows selecting rows by raw values.
	// Furthermore, it handles CopyTo!
	pgURL, cleanupGoDB, err := s.PGUrlE(
		serverutils.CertsDirPrefix("StartServer"),
		serverutils.User(username.RootUser),
	)
	require.NoError(t, err)
	s.AppStopper().AddCloser(stop.CloserFn(func() { cleanupGoDB() }))
	config, err := pgx.ParseConfig(pgURL.String())
	require.NoError(t, err)
	config.Database = sqlutils.TestDB
	conn, err := pgx.ConnectConfig(ctx, config)
	require.NoError(t, err)
	defer func() { require.NoError(t, conn.Close(ctx)) }()

	// Select all rows using the simple query protocol.
	var rowOutput [][][]byte
	allQueries := conn.PgConn().Exec(ctx, "SELECT * FROM t")
	for allQueries.NextResult() {
		query := allQueries.ResultReader()
		for query.NextRow() {
			// Do a deep copy of the row.
			vals := query.Values()
			row := make([][]byte, len(vals))
			for i := range vals {
				col := make([]byte, len(vals[i]))
				copy(col, vals[i])
				row[i] = col
			}
			rowOutput = append(rowOutput, row)
		}
		_, err := query.Close()
		require.NoError(t, err)
	}
	require.NoError(t, allQueries.Close())

	// Run COPY t TO STDOUT and compare against the copy.
	t.Run("text", func(t *testing.T) {
		var buf bytes.Buffer
		_, err = conn.PgConn().CopyTo(ctx, &buf, "COPY t TO STDOUT")
		require.NoError(t, err)

		// Compare all rows. Note we omit the last row as it is empty.
		lines := strings.Split(buf.String(), "\n")
		lines = lines[:len(lines)-1]
		for lineNum, line := range lines {
			for fieldNum, field := range strings.Split(line, "\t") {
				require.Equalf(
					t,
					string(rowOutput[lineNum][fieldNum]),
					sql.DecodeCopy(field),
					"error line %d, field %d (%s)",
					lineNum,
					fieldNum,
					colTypes[fieldNum].SQLString(),
				)
			}
		}
	})

	t.Run("csv", func(t *testing.T) {
		var buf bytes.Buffer
		_, err = conn.PgConn().CopyTo(ctx, &buf, "COPY t TO STDOUT CSV")
		require.NoError(t, err)

		allRecords, err := csv.NewReader(bytes.NewReader(buf.Bytes())).ReadAll()
		require.NoError(t, err)
		for lineNum, line := range allRecords {
			for fieldNum, field := range line {
				require.Equalf(
					t,
					string(rowOutput[lineNum][fieldNum]),
					field.Val,
					"error line %d, field %d (%s)",
					lineNum,
					fieldNum,
					colTypes[fieldNum].SQLString(),
				)
			}
		}
	})
}
