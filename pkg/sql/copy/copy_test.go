// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package copy_test

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"net/url"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

const lineitemSchema string = `CREATE TABLE lineitem (
	l_orderkey      INT8 NOT NULL,
	l_partkey       INT8 NOT NULL,
	l_suppkey       INT8 NOT NULL,
	l_linenumber    INT8 NOT NULL,
	l_quantity      DECIMAL(15,2) NOT NULL,
	l_extendedprice DECIMAL(15,2) NOT NULL,
	l_discount      DECIMAL(15,2) NOT NULL,
	l_tax           DECIMAL(15,2) NOT NULL,
	l_returnflag    CHAR(1) NOT NULL,
	l_linestatus    CHAR(1) NOT NULL,
	l_shipdate      DATE NOT NULL,
	l_commitdate    DATE NOT NULL,
	l_receiptdate   DATE NOT NULL,
	l_shipinstruct  CHAR(25) NOT NULL,
	l_shipmode      CHAR(10) NOT NULL,
	l_comment       VARCHAR(44) NOT NULL,
	PRIMARY KEY     (l_orderkey, l_linenumber),
	INDEX l_ok      (l_orderkey ASC),
	INDEX l_pk      (l_partkey ASC),
	INDEX l_sk      (l_suppkey ASC),
	INDEX l_sd      (l_shipdate ASC),
	INDEX l_cd      (l_commitdate ASC),
	INDEX l_rd      (l_receiptdate ASC),
	INDEX l_pk_sk   (l_partkey ASC, l_suppkey ASC),
	INDEX l_sk_pk   (l_suppkey ASC, l_partkey ASC)
)`

const csvData = `%d|155190|7706|1|17|21168.23|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVER IN PERSON|TRUCK|egular courts above the`

func TestCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: cluster.MakeTestingClusterSettings(),
	})
	defer s.Stopper().Stop(ctx)

	url, cleanup := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(username.RootUser))
	defer cleanup()
	var sqlConnCtx clisqlclient.Context
	conn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, url.String())

	testCopy := func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "exec-ddl":
			err := conn.Exec(ctx, d.Input)
			if err != nil {
				require.NoError(t, err, "%s: %s", d.Pos, d.Cmd)
			}
			return ""
		case "copy", "copy-error":
			lines := strings.Split(d.Input, "\n")
			stmt := lines[0]
			data := strings.Join(lines[1:], "\n")
			rows, err := conn.GetDriverConn().CopyFrom(ctx, strings.NewReader(data), stmt)
			if d.Cmd == "copy" {
				require.NoError(t, err, "%s\n%s\n", d.Cmd, d.Input)
				require.Equal(t, int(rows), len(lines)-1, "Not all rows were inserted")
			} else {
				return err.Error()
			}
			return fmt.Sprintf("%d", rows)
		case "query":
			rows, err := conn.Query(ctx, d.Input)
			require.NoError(t, err)

			vals := make([]driver.Value, len(rows.Columns()))
			var results string
			for {
				if err := rows.Next(vals); err == io.EOF {
					break
				} else if err != nil {
					require.NoError(t, err)
				}
				for i, v := range vals {
					if i > 0 {
						results += "|"
					}
					results += fmt.Sprintf("%v", v)
				}
				results += "\n"
			}
			err = rows.Close()
			require.NoError(t, err)
			return results
		default:
			return fmt.Sprintf("unknown command: %s\n", d.Cmd)
		}

	}
	datadriven.RunTest(t, testutils.TestDataPath(t, "copyfrom"), testCopy)
}

// TestCopyFromTransaction tests that copy from rows are written with
// same transaction timestamp when done under an explicit transaction,
// copy rows are same transaction when done with default settings and
// batches are in separate transactions when non atomic mode is enabled.
func TestCopyFromTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Settings: cluster.MakeTestingClusterSettings(),
	})
	defer s.Stopper().Stop(ctx)

	url, cleanup := sqlutils.PGUrl(t, s.ServingSQLAddr(), "copytest", url.User(username.RootUser))
	defer cleanup()
	var sqlConnCtx clisqlclient.Context

	testCases := []struct {
		name         string
		query        string
		data         []string
		testf        func(clisqlclient.Conn, func(clisqlclient.Conn))
		checkResults func(t *testing.T, f1, f2 driver.Value)
	}{
		{
			"explicit_copy",
			"COPY lineitem FROM STDIN WITH CSV DELIMITER '|';",
			[]string{fmt.Sprintf(csvData, 1), fmt.Sprintf(csvData, 2)},
			func(tconn clisqlclient.Conn, f func(tconn clisqlclient.Conn)) {
				err := tconn.Exec(ctx, "BEGIN")
				require.NoError(t, err)
				f(tconn)
				err = tconn.Exec(ctx, "COMMIT")
				require.NoError(t, err)
			},
			func(t *testing.T, f1, f2 driver.Value) { require.Equal(t, f1, f2) },
		},
		{
			"implicit_atomic",
			"COPY lineitem FROM STDIN WITH CSV DELIMITER '|';",
			[]string{fmt.Sprintf(csvData, 1), fmt.Sprintf(csvData, 2)},
			func(tconn clisqlclient.Conn, f func(tconn clisqlclient.Conn)) {
				err := tconn.Exec(ctx, "SET copy_from_atomic_enabled = true")
				require.NoError(t, err)
				defer sql.SetCopyFromBatchSize(1)()
				f(tconn)
			},
			func(t *testing.T, f1, f2 driver.Value) { require.Equal(t, f1, f2) },
		},
		{
			"implicit_non_atomic",
			"COPY lineitem FROM STDIN WITH CSV DELIMITER '|';",
			[]string{fmt.Sprintf(csvData, 1), fmt.Sprintf(csvData, 2)},
			func(tconn clisqlclient.Conn, f func(tconn clisqlclient.Conn)) {
				err := tconn.Exec(ctx, "SET copy_from_atomic_enabled = false")
				require.NoError(t, err)
				defer sql.SetCopyFromBatchSize(1)()
				f(tconn)
			},
			func(t *testing.T, f1, f2 driver.Value) { require.NotEqual(t, f1, f2) },
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tconn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, url.String())
			tc.testf(tconn, func(tconn clisqlclient.Conn) {
				// Put each test in its own db so they can be parallelized.
				err := tconn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s; USE %s", tc.name, tc.name))
				require.NoError(t, err)
				err = tconn.Exec(ctx, lineitemSchema)
				require.NoError(t, err)
				numrows, err := tconn.GetDriverConn().CopyFrom(ctx, strings.NewReader(strings.Join(tc.data, "\n")), tc.query)
				require.NoError(t, err)
				require.Equal(t, len(tc.data), int(numrows))

				result, err := tconn.QueryRow(ctx, "SELECT l_partkey FROM lineitem WHERE l_orderkey = 1")
				require.NoError(t, err)
				partKey, ok := result[0].(string)
				require.True(t, ok)
				require.Equal(t, "155190", partKey)

				results, err := tconn.Query(ctx, "SELECT crdb_internal_mvcc_timestamp FROM lineitem")
				defer func() { _ = results.Close() }()
				require.NoError(t, err)
				var lastts driver.Value
				firstTime := true
				vals := make([]driver.Value, 1)
				for {
					err = results.Next(vals)
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
					if !firstTime {
						tc.checkResults(t, lastts, vals[0])
					} else {
						firstTime = false
					}
					lastts = vals[0]
				}
			})
			err := tconn.Exec(ctx, "TRUNCATE TABLE lineitem")
			require.NoError(t, err)
		})
	}
}

// BenchmarkCopyFrom measures copy performance against a TestServer.
func BenchmarkCopyFrom(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	s, _, _ := serverutils.StartServer(b, base.TestServerArgs{
		Settings: cluster.MakeTestingClusterSettings(),
	})
	defer s.Stopper().Stop(ctx)

	url, cleanup := sqlutils.PGUrl(b, s.ServingSQLAddr(), "copytest", url.User(username.RootUser))
	defer cleanup()
	var sqlConnCtx clisqlclient.Context
	conn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, url.String())

	err := conn.Exec(ctx, lineitemSchema)
	require.NoError(b, err)

	// send data in 5 batches of 10k rows
	const numRows = sql.CopyBatchRowSizeDefault * 4
	datalen := 0
	var rows []string
	for i := 0; i < numRows; i++ {
		row := fmt.Sprintf(csvData, i)
		rows = append(rows, row)
		datalen += len(row)
	}
	rowsize := datalen / numRows
	for _, batchSizeFactor := range []float64{.5, 1, 2, 4} {
		batchSize := int(batchSizeFactor * sql.CopyBatchRowSizeDefault)
		b.Run(fmt.Sprintf("%d", batchSize), func(b *testing.B) {
			actualRows := rows[:batchSize]
			for i := 0; i < b.N; i++ {
				numrows, err := conn.GetDriverConn().CopyFrom(ctx, strings.NewReader(strings.Join(actualRows, "\n")), "COPY lineitem FROM STDIN WITH CSV DELIMITER '|';")
				require.NoError(b, err)
				require.Equal(b, int(numrows), len(actualRows))
				b.StopTimer()
				err = conn.Exec(ctx, "TRUNCATE TABLE lineitem")
				require.NoError(b, err)
				b.StartTimer()
			}
			b.SetBytes(int64(len(actualRows) * rowsize))
		})
	}
}
