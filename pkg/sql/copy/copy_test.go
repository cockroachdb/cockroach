// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package copy

import (
	"bytes"
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
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

func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer ccl.TestingEnableEnterprise()() // allow usage of READ COMMITTED
	ctx := context.Background()

	doTest := func(t *testing.T, d *datadriven.TestData, conn clisqlclient.Conn) string {
		switch d.Cmd {
		case "exec-ddl":
			err := conn.Exec(ctx, d.Input)
			if err != nil {
				require.NoError(t, err, "%s: %s", d.Pos, d.Cmd)
			}
			return ""
		case "copy-from", "copy-from-error", "copy-from-kvtrace":
			kvtrace := d.Cmd == "copy-from-kvtrace"
			lines := strings.Split(d.Input, "\n")
			expectedRows := len(lines) - 1
			stmt := lines[0]
			data := strings.Join(lines[1:], "\n")
			st, err := parser.ParseOne(stmt)
			require.NoError(t, err)
			if copy, ok := st.AST.(*tree.CopyFrom); ok {
				if copy.Options.HasHeader {
					expectedRows--
				}
			}

			if kvtrace {
				err := conn.Exec(ctx, "SET TRACING=on,kv")
				require.NoError(t, err)
			}
			rows, err := conn.GetDriverConn().CopyFrom(ctx, strings.NewReader(data), stmt)
			if kvtrace {
				err := conn.Exec(ctx, "SET TRACING=off")
				require.NoError(t, err)
			}
			switch d.Cmd {
			case "copy-from":
				require.NoError(t, err, "%s\n%s\n", d.Cmd, d.Input)
				require.Equal(t, int(rows), expectedRows, "Not all rows were inserted")
				return fmt.Sprintf("%d", rows)
			case "copy-from-error":
				require.Error(t, err, "copy-from-error didn't return and error!")
				return err.Error()
			case "copy-from-kvtrace":
				require.NoError(t, err, "%s\n%s\n", d.Cmd, d.Input)
				rows, err := conn.Query(ctx,
					`SELECT
	regexp_replace(message, '(/Tenant/[0-9]*)?/Table/[0-9]*/', '/Table/<>/')
	FROM [SHOW KV TRACE FOR SESSION]
	WHERE message LIKE '%Put % -> %'`)
				defer func() {
					_ = rows.Close()
				}()
				require.NoError(t, err)
				vals := make([]driver.Value, 1)
				var results []string
				for err = nil; err == nil; {
					err = rows.Next(vals)
					if err == io.EOF {
						break
					}
					require.NoError(t, err)
					results = append(results, fmt.Sprintf("%v", vals[0]))
				}
				sort.Strings(results)
				return strings.Join(results, "\n")
			}
		case "copy-to", "copy-to-error":
			var buf bytes.Buffer
			err := conn.GetDriverConn().CopyTo(ctx, &buf, d.Input)
			if d.Cmd == "copy-to" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				return expandErrorString(err)
			}
			return buf.String()
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
		return ""
	}

	for _, vectorize := range []string{"on", "off"} {
		t.Run(fmt.Sprintf("vectorize=%s", vectorize), func(t *testing.T) {
			for _, atomic := range []string{"on", "off"} {
				t.Run(fmt.Sprintf("atomic=%s", atomic), func(t *testing.T) {
					for _, fastPath := range []string{"on", "off"} {
						t.Run(fmt.Sprintf("fastPath=%s", fastPath), func(t *testing.T) {
							datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
								defer log.Scope(t).Close(t)

								srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
								defer srv.Stopper().Stop(ctx)

								s := srv.ApplicationLayer()

								url, cleanup := pgurlutils.PGUrl(t, s.AdvSQLAddr(), t.Name(), url.User(username.RootUser))
								defer cleanup()
								var sqlConnCtx clisqlclient.Context
								conn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, url.String())

								err := conn.Exec(ctx, fmt.Sprintf(`SET VECTORIZE='%s'`, vectorize))
								require.NoError(t, err)

								err = conn.Exec(ctx, fmt.Sprintf(`SET COPY_FAST_PATH_ENABLED='%s'`, fastPath))
								require.NoError(t, err)

								err = conn.Exec(ctx, fmt.Sprintf(`SET COPY_FROM_ATOMIC_ENABLED='%s'`, atomic))
								require.NoError(t, err)

								datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
									return doTest(t, d, conn)
								})
							})
						})
					}
				})
			}
		})
	}
}

var issueLinkRE = regexp.MustCompile("https://go.crdb.dev/issue-v/([0-9]+)/.*")

func expandErrorString(err error) string {
	var sb strings.Builder
	sb.WriteString(err.Error())

	if pgErr := (*pgconn.PgError)(nil); errors.As(err, &pgErr) {
		if pgErr.Hint != "" {
			sb.WriteString(fmt.Sprintf("\nHINT: %s", pgErr.Hint))
		}
		if pgErr.Detail != "" {
			sb.WriteString(fmt.Sprintf("\nDETAIL: %s", pgErr.Detail))
		}
	}
	return issueLinkRE.ReplaceAllString(sb.String(), `https://go.crdb.dev/issue-v/$1/`)
}

// TestCopyFromTransaction tests that copy from rows are written with
// same transaction timestamp when done under an explicit transaction,
// copy rows are same transaction when done with default settings and
// batches are in separate transactions when non atomic mode is enabled.
func TestCopyFromTransaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	testutils.RunTrueAndFalse(t, "disableAutoCommitDuringExec", func(t *testing.T, b bool) {
		defer log.Scope(t).Close(t)
		srv := serverutils.StartServerOnly(t, base.TestServerArgs{
			Knobs: base.TestingKnobs{
				SQLExecutor: &sql.ExecutorTestingKnobs{
					DisableAutoCommitDuringExec: b,
				},
			},
		})
		defer srv.Stopper().Stop(ctx)

		s := srv.ApplicationLayer()
		// Disable pipelining. Without this, pipelined writes performed as part
		// of the COPY can be lost, which can then cause the COPY to fail.
		kvcoord.PipelinedWritesEnabled.Override(ctx, &s.ClusterSettings().SV, false)

		url, cleanup := pgurlutils.PGUrl(t, s.AdvSQLAddr(), "copytest", url.User(username.RootUser))
		defer cleanup()
		var sqlConnCtx clisqlclient.Context

		decEq := func(v1, v2 driver.Value) bool {
			valToDecimal := func(v driver.Value) *apd.Decimal {
				mt, ok := v.(pgtype.Numeric)
				require.True(t, ok)
				buf, err := mt.MarshalJSON()
				require.NoError(t, err)
				decimal, _, err := apd.NewFromString(string(buf))
				require.NoError(t, err)
				return decimal
			}
			return valToDecimal(v1).Cmp(valToDecimal(v2)) == 0
		}

		testCases := []struct {
			name   string
			query  string
			data   []string
			testf  func(clisqlclient.Conn, func(clisqlclient.Conn))
			result func(f1, f2 driver.Value) bool
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
				decEq,
			},
			{
				"implicit_atomic",
				"COPY lineitem FROM STDIN WITH CSV DELIMITER '|';",
				[]string{fmt.Sprintf(csvData, 1), fmt.Sprintf(csvData, 2)},
				func(tconn clisqlclient.Conn, f func(tconn clisqlclient.Conn)) {
					err := tconn.Exec(ctx, "SET copy_from_atomic_enabled = true")
					require.NoError(t, err)
					orig := sql.SetCopyFromBatchSize(1)
					defer sql.SetCopyFromBatchSize(orig)
					f(tconn)
				},
				decEq,
			},
			{
				"implicit_non_atomic",
				"COPY lineitem FROM STDIN WITH CSV DELIMITER '|';",
				[]string{fmt.Sprintf(csvData, 1), fmt.Sprintf(csvData, 2)},
				func(tconn clisqlclient.Conn, f func(tconn clisqlclient.Conn)) {
					err := tconn.Exec(ctx, "SET copy_from_atomic_enabled = false")
					require.NoError(t, err)
					orig := sql.SetCopyFromBatchSize(1)
					defer sql.SetCopyFromBatchSize(orig)
					f(tconn)
				},
				func(f1, f2 driver.Value) bool { return !decEq(f1, f2) },
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				tconn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, url.String())
				tc.testf(tconn, func(tconn clisqlclient.Conn) {
					// Without this everything comes back as strings
					_ = tconn.SetAlwaysInferResultTypes(true)
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
					partKey, ok := result[0].(int64)
					require.True(t, ok)
					require.Equal(t, int64(155190), partKey)

					results, err := tconn.Query(ctx, "SELECT crdb_internal_mvcc_timestamp FROM lineitem")
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
							require.True(t, tc.result(lastts, vals[0]))
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
	})
}

// slowCopySource is a pgx.CopyFromSource that copies a fixed number of rows
// and sleeps for 500 ms in between each one.
type slowCopySource struct {
	count int
	total int
}

func (s *slowCopySource) Next() bool {
	s.count++
	return s.count < s.total
}

func (s *slowCopySource) Values() ([]interface{}, error) {
	time.Sleep(500 * time.Millisecond)
	return []interface{}{s.count}, nil
}

func (s *slowCopySource) Err() error {
	return nil
}

var _ pgx.CopyFromSource = &slowCopySource{}

// TestCopyFromTimeout checks that COPY FROM respects the statement_timeout
// and transaction_timeout settings.
func TestCopyFromTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()

	pgURL, cleanup := pgurlutils.PGUrl(
		t,
		s.AdvSQLAddr(),
		"TestCopyFromTimeout",
		url.User(username.RootUser),
	)
	defer cleanup()

	t.Run("copy from", func(t *testing.T) {
		conn, err := pgx.Connect(ctx, pgURL.String())
		require.NoError(t, err)

		_, err = conn.Exec(ctx, "CREATE TABLE t (a INT PRIMARY KEY)")
		require.NoError(t, err)

		_, err = conn.Exec(ctx, "SET transaction_timeout = '100ms'")
		require.NoError(t, err)

		tx, err := conn.Begin(ctx)
		require.NoError(t, err)

		_, err = tx.CopyFrom(ctx, pgx.Identifier{"t"}, []string{"a"}, &slowCopySource{total: 2})
		require.ErrorContains(t, err, "query execution canceled due to transaction timeout")

		err = tx.Rollback(ctx)
		require.NoError(t, err)

		_, err = conn.Exec(ctx, "SET statement_timeout = '200ms'")
		require.NoError(t, err)

		_, err = conn.CopyFrom(ctx, pgx.Identifier{"t"}, []string{"a"}, &slowCopySource{total: 2})
		require.ErrorContains(t, err, "query execution canceled due to statement timeout")
	})

	t.Run("copy to", func(t *testing.T) {
		conn, err := pgx.Connect(ctx, pgURL.String())
		require.NoError(t, err)

		_, err = conn.Exec(ctx, "SET transaction_timeout = '100ms'")
		require.NoError(t, err)

		tx, err := conn.Begin(ctx)
		require.NoError(t, err)

		_, err = tx.Exec(ctx, "COPY (SELECT pg_sleep(1) FROM ROWS FROM (generate_series(1, 60)) AS i) TO STDOUT")
		require.ErrorContains(t, err, "query execution canceled due to transaction timeout")

		err = tx.Rollback(ctx)
		require.NoError(t, err)

		_, err = conn.Exec(ctx, "SET statement_timeout = '200ms'")
		require.NoError(t, err)

		_, err = conn.Exec(ctx, "COPY (SELECT pg_sleep(1) FROM ROWS FROM (generate_series(1, 60)) AS i) TO STDOUT")
		require.ErrorContains(t, err, "query execution canceled due to statement timeout")
	})
}

func TestShowQueriesIncludesCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	pgURL, cleanup := pgurlutils.PGUrl(
		t,
		srv.ApplicationLayer().AdvSQLAddr(),
		"TestShowQueriesIncludesCopy",
		url.User(username.RootUser),
	)
	defer cleanup()

	showConn, err := pgx.Connect(ctx, pgURL.String())
	require.NoError(t, err)
	q := pgURL.Query()
	q.Add("application_name", "app_name")
	pgURL.RawQuery = q.Encode()
	copyConn, err := pgx.Connect(ctx, pgURL.String())
	require.NoError(t, err)
	_, err = copyConn.Exec(ctx, "CREATE TABLE t (a INT PRIMARY KEY)")
	require.NoError(t, err)

	t.Run("copy to", func(t *testing.T) {
		g := ctxgroup.WithContext(ctx)
		g.GoCtx(func(ctx context.Context) error {
			_, err := copyConn.Exec(ctx, "COPY (SELECT pg_sleep(1) FROM ROWS FROM (generate_series(1, 60)) AS i) TO STDOUT")
			return err
		})

		// The COPY query should use the specified app name. SucceedsSoon is used
		// since COPY is being executed concurrently.
		var appName string
		testutils.SucceedsSoon(t, func() error {
			err = showConn.QueryRow(ctx, "SELECT application_name FROM [SHOW QUERIES] WHERE query LIKE 'COPY (SELECT pg_sleep(1) %'").Scan(&appName)
			if err != nil {
				return err
			}
			if appName != "app_name" {
				return errors.New("expected COPY to appear in SHOW QUERIES")
			}
			return nil
		})

		err = copyConn.PgConn().CancelRequest(ctx)
		require.NoError(t, err)

		// An error is expected, since the query was canceled.
		err = g.Wait()
		require.ErrorContains(t, err, "query execution canceled")
	})

	t.Run("copy from", func(t *testing.T) {
		g := ctxgroup.WithContext(ctx)
		g.GoCtx(func(ctx context.Context) error {
			_, err := copyConn.CopyFrom(ctx, pgx.Identifier{"t"}, []string{"a"}, &slowCopySource{total: 5})
			return err
		})

		// The COPY query should use the specified app name. SucceedsSoon is used
		// since COPY is being executed concurrently.
		var appName string
		testutils.SucceedsSoon(t, func() error {
			err = showConn.QueryRow(ctx, "SELECT application_name FROM [SHOW QUERIES] WHERE query ILIKE 'COPY%t%a%FROM%'").Scan(&appName)
			if err != nil {
				return err
			}
			if appName != "app_name" {
				return errors.New("expected COPY to appear in SHOW QUERIES")
			}
			return nil
		})

		err = copyConn.PgConn().CancelRequest(ctx)
		require.NoError(t, err)

		// An error is expected, since the query was canceled.
		err = g.Wait()
		require.ErrorContains(t, err, "query execution canceled")
	})
}

// TestLargeDynamicRows ensure that we don't overflow memory with large rows by
// testing that we break the inserts into batches, in this case at least 1
// batch per row.  Also make sure adequately sized buffers just use 1 batch.
func TestLargeDynamicRows(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	var params base.TestServerArgs
	var batchNumber int
	params.Knobs.SQLExecutor = &sql.ExecutorTestingKnobs{
		CopyFromInsertBeforeBatch: func(*kv.Txn) error {
			batchNumber++
			return nil
		},
		CopyFromInsertRetry: func() error {
			batchNumber--
			return nil
		},
	}
	srv := serverutils.StartServerOnly(t, params)
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	url, cleanup := s.PGUrl(t)
	defer cleanup()
	var sqlConnCtx clisqlclient.Context
	conn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, url.String())

	// Only copy-fast-path has proper row accounting, override metamorphic that
	// might turn it off.
	err := conn.Exec(ctx, `SET COPY_FAST_PATH_ENABLED = 'true'`)
	require.NoError(t, err)

	// 4.0 MiB is minimum, but due to #117070 use 5MiB instead to avoid flakes.
	// Copy sets max row size to this value / 3.
	const memLimit = kvserverbase.MaxCommandSizeFloor + 1<<20
	kvserverbase.MaxCommandSize.Override(ctx, &s.ClusterSettings().SV, memLimit)

	err = conn.Exec(ctx, "CREATE TABLE t (s STRING)")
	require.NoError(t, err)

	rng, _ := randutil.NewTestRand()
	str := randutil.RandString(rng, (2<<20)+1, "asdf")

	var sb strings.Builder
	for i := 0; i < 4; i++ {
		sb.WriteString(str)
		sb.WriteString("\n")
	}
	_, err = conn.GetDriverConn().CopyFrom(ctx, strings.NewReader(sb.String()), "COPY t FROM STDIN")
	require.NoError(t, err)
	require.GreaterOrEqual(t, 4, batchNumber)
	batchNumber = 0

	// Reset and make sure we use 1 batch.
	kvserverbase.MaxCommandSize.Override(ctx, &s.ClusterSettings().SV, kvserverbase.MaxCommandSizeDefault)

	// This won't work if the batch size gets set to less than 5. When the batch
	// size is 4, the test hook will count an extra empty batch.
	if sql.CopyBatchRowSize < 5 {
		sql.SetCopyFromBatchSize(5)
	}

	_, err = conn.GetDriverConn().CopyFrom(ctx, strings.NewReader(sb.String()), "COPY t FROM STDIN")
	require.NoError(t, err)
	require.Equal(t, 1, batchNumber)
}

// TestTinyRows ensures batch sizing logic doesn't explode with small table.
func TestTinyRows(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	url, cleanup := pgurlutils.PGUrl(t, srv.ApplicationLayer().AdvSQLAddr(), "copytest", url.User(username.RootUser))
	defer cleanup()
	var sqlConnCtx clisqlclient.Context
	conn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, url.String())

	err := conn.Exec(ctx, "CREATE TABLE t (b BOOL PRIMARY KEY)")
	require.NoError(t, err)

	_, err = conn.GetDriverConn().CopyFrom(ctx, strings.NewReader("true\nfalse\n"), "COPY t FROM STDIN")
	require.NoError(t, err)
}

// TODO(cucaroach): get the rand utilities and ParseAndRequire to be friends
// STRINGS don't roundtrip well, need to figure out proper escaping
// INET doesn't round trip: ERROR: could not parse "70e5:112:5114:7da5:1" as inet. invalid IP (SQLSTATE 22P02)
// DECIMAL(15,2) don't round trip, get too big number errors.
const lineitemSchemaMunged string = `CREATE TABLE lineitem (
	l_orderkey      INT8 NOT NULL,
	l_partkey       INT8 NOT NULL,
	l_suppkey       INT8 NOT NULL,
	l_linenumber    INT8 NOT NULL,
	l_quantity      INT8 NOT NULL,
	l_extendedprice FLOAT NOT NULL,
	l_discount      FLOAT NOT NULL,
	l_tax           FLOAT NOT NULL,
	l_returnflag    TIMESTAMPTZ NOT NULL,
	l_linestatus    TIMESTAMPTZ NOT NULL,
	l_shipdate      DATE NOT NULL,
	l_commitdate    DATE NOT NULL,
	l_receiptdate   DATE NOT NULL,
	l_shipinstruct  INTERVAL NOT NULL,
	l_shipmode      UUID NOT NULL,
	l_comment       UUID NOT NULL,
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

// Perform a COPY of N rows, N can be arbitrarily large to test huge copies.
func TestLargeCopy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// This test can cause timeouts.
	skip.UnderRace(t)
	ctx := context.Background()

	srv, _, kvdb := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	url, cleanup := s.PGUrl(t, serverutils.CertsDirPrefix("copytest"), serverutils.User(username.RootUser))
	defer cleanup()
	var sqlConnCtx clisqlclient.Context
	conn := sqlConnCtx.MakeSQLConn(io.Discard, io.Discard, url.String())

	err := conn.Exec(ctx, lineitemSchemaMunged)
	require.NoError(t, err)

	desc := desctestutils.TestingGetPublicTableDescriptor(kvdb, s.Codec(), "defaultdb", "lineitem")
	require.NotNil(t, desc, "Failed to lookup descriptor")

	err = conn.Exec(ctx, "SET copy_from_atomic_enabled = false")
	require.NoError(t, err)

	rng := rand.New(rand.NewSource(0))
	rows := 100
	numrows, err := conn.GetDriverConn().CopyFrom(ctx,
		&copyReader{rng: rng, cols: desc.PublicColumns(), rows: rows},
		"COPY lineitem FROM STDIN WITH CSV;")
	require.NoError(t, err)
	require.Equal(t, int(numrows), rows)
}

type copyReader struct {
	cols          []catalog.Column
	rng           *rand.Rand
	rows          int
	count         int
	generatedRows io.Reader
}

var _ io.Reader = &copyReader{}

func (c *copyReader) Read(b []byte) (n int, err error) {
	if c.generatedRows == nil {
		c.generateRows()
	}
	n, err = c.generatedRows.Read(b)
	if err == io.EOF && c.count < c.rows {
		c.generateRows()
		n, err = c.generatedRows.Read(b)
	}
	return
}

func (c *copyReader) generateRows() {
	numRows := min(1000, c.rows-c.count)
	sb := strings.Builder{}
	for i := 0; i < numRows; i++ {
		row := make([]string, len(c.cols))
		for j, col := range c.cols {
			t := col.GetType()
			var ds string
			if j == 0 {
				// Special handling for ID field
				ds = strconv.Itoa(c.count + i)
			} else {
				d := randgen.RandDatum(c.rng, t, col.IsNullable())
				ds = tree.AsStringWithFlags(d, tree.FmtBareStrings)
				// Empty string is treated as null
				if len(ds) == 0 && !col.IsNullable() {
					ds = "a"
				}
				switch t.Family() {
				case types.CollatedStringFamily:
					// For collated strings, we just want the raw contents in COPY.
					ds = d.(*tree.DCollatedString).Contents
				case types.FloatFamily:
					ds = strings.TrimSuffix(ds, ".0")
				}
				switch t.Family() {
				case types.BytesFamily,
					types.DateFamily,
					types.IntervalFamily,
					types.INetFamily,
					types.StringFamily,
					types.TimestampFamily,
					types.TimestampTZFamily,
					types.UuidFamily,
					types.CollatedStringFamily:
					var b bytes.Buffer
					if err := sql.EncodeCopy(&b, encoding.UnsafeConvertStringToBytes(ds), ','); err != nil {
						panic(err)
					}
					ds = b.String()
				}
			}
			row[j] = ds
		}
		r := strings.Join(row, ",")
		sb.WriteString(r)
		sb.WriteString("\n")
	}
	c.count += numRows
	c.generatedRows = strings.NewReader(sb.String())
}

func BenchmarkCopyCSVEndToEnd(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(b, base.TestServerArgs{
		DefaultTestTenant: base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(83461),
	})
	defer s.Stopper().Stop(ctx)

	pgURL, cleanup, err := pgurlutils.PGUrlE(
		s.AdvSQLAddr(),
		"BenchmarkCopyEndToEnd", /* prefix */
		url.User(username.RootUser),
	)
	require.NoError(b, err)
	s.Stopper().AddCloser(stop.CloserFn(cleanup))

	_, err = db.Exec("CREATE TABLE t (i INT PRIMARY KEY, s STRING)")
	require.NoError(b, err)

	conn, err := pgx.Connect(ctx, pgURL.String())
	require.NoError(b, err)

	rng, _ := randutil.NewTestRand()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// Create an input of 1_000_000 rows.
		buf := &bytes.Buffer{}
		for j := 0; j < 1_000_000; j++ {
			buf.WriteString(strconv.Itoa(j))
			buf.WriteString(",")
			str := randutil.RandString(rng, rng.Intn(50), "abc123\n")
			buf.WriteString("\"")
			buf.WriteString(str)
			buf.WriteString("\"\n")
		}
		b.StartTimer()

		// Run the COPY.
		_, err = conn.PgConn().CopyFrom(ctx, buf, "COPY t FROM STDIN CSV")
		require.NoError(b, err)

		// Verify that the data was inserted.
		b.StopTimer()
		var count int
		err = db.QueryRow("SELECT count(*) FROM t").Scan(&count)
		require.NoError(b, err)
		require.Equal(b, 1_000_000, count)
		b.StartTimer()
	}
}
