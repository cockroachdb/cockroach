// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestStructuredEventLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// We really need to have the logs go to files, so that -show-logs
	// does not break the "authlog" directives.
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	ctx := context.Background()

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	testStartTs := timeutil.Now()

	// Change the user with SET ROLE to make sure the original logged-in user
	// appears in the logs.
	if _, err := conn.ExecContext(ctx, "CREATE USER other_user"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "GRANT SYSTEM MODIFYCLUSTERSETTING TO other_user"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "SET ROLE other_user"); err != nil {
		t.Fatal(err)
	}

	// Make a prepared statement that changes a cluster setting:
	// - we want a prepared statement to verify that the reporting of
	//   placeholders works during EXECUTE.
	// - we don't care about the particular cluster setting; any
	//   setting that does not otherwise impact the test's semantics
	//   will do.
	const setStmt = `SET CLUSTER SETTING "sql.defaults.default_int_size" = $1`
	const expectedStmt = `SET CLUSTER SETTING "sql.defaults.default_int_size" = $1`
	if _, err := conn.ExecContext(ctx,
		`PREPARE a(INT) AS `+setStmt,
	); err != nil {
		t.Fatal(err)
	}
	// Run the prepared statement. This triggers a structured entry
	// for the cluster setting change.
	if _, err := conn.ExecContext(ctx, `EXECUTE a(8)`); err != nil {
		t.Fatal(err)
	}

	// Ensure that the entries hit the OS so they can be read back below.
	log.FlushFileSinks()

	entries, err := log.FetchEntriesFromFiles(testStartTs.UnixNano(),
		math.MaxInt64, 10000, execLogRe, log.WithMarkedSensitiveData)
	if err != nil {
		t.Fatal(err)
	}

	foundEntry := false
	for _, e := range entries {
		if !strings.Contains(e.Message, "set_cluster_setting") {
			continue
		}
		foundEntry = true
		// TODO(knz): Remove this when crdb-v2 becomes the new format.
		e.Message = strings.TrimPrefix(e.Message, "Structured entry:")
		// crdb-v2 starts json with an equal sign.
		e.Message = strings.TrimPrefix(e.Message, "=")
		jsonPayload := []byte(e.Message)
		var ev eventpb.SetClusterSetting
		if err := json.Unmarshal(jsonPayload, &ev); err != nil {
			t.Errorf("unmarshalling %q: %v", e.Message, err)
		}
		if ev.Statement != expectedStmt {
			t.Errorf("wrong statement: expected %q, got %q", expectedStmt, ev.Statement)
		}
		if ev.User != username.RootUser {
			t.Errorf("wrong user: expected %q, got %q", username.RootUser, ev.User)
		}
		if expected := []string{string(redact.Sprint("8"))}; !reflect.DeepEqual(expected, ev.PlaceholderValues) {
			t.Errorf("wrong placeholders: expected %+v, got %+v", expected, ev.PlaceholderValues)
		}
	}
	if !foundEntry {
		t.Error("structured entry for set_cluster_setting not found in log")
	}
}

var execLogRe = regexp.MustCompile(`event_log.go`)

// Test the SQL_PERF and SQL_INTERNAL_PERF logging channels.
func TestPerfLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var testCases = []struct {
		// Query to execute. query might be empty if setup is not empty, in
		// which case only the setup is performed.
		query string
		// Regular expression the error message must match ("" for no error).
		errRe string
		// Regular expression used to search log messages from all channels.
		logRe string
		// Whether we expect to find any log messages matching logRe.
		logExpected bool
		breakHere   bool
		// Logging channel all log messages matching logRe must be in.
		channel logpb.Channel
		// Optional queries to execute before/after running query.
		setup, cleanup string
	}{
		{
			query:       `SELECT pg_sleep(0.256)`,
			errRe:       ``,
			logRe:       `"EventType":"slow_query","Statement":"SELECT pg_sleep\(‹0.256›\)","Tag":"SELECT","User":"root","ExecMode":"exec","NumRows":1`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `INSERT INTO t VALUES (1, pg_sleep(0.256), 'x')`,
			errRe:       ``,
			logRe:       `"EventType":"slow_query","Statement":"INSERT INTO .*‹t› VALUES \(‹1›, pg_sleep\(‹0.256›\), ‹'x'›\)","Tag":"INSERT","User":"root"`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `INSERT INTO t VALUES (1, pg_sleep(0.256), 'x')`,
			errRe:       `duplicate key`,
			logRe:       `"EventType":"slow_query","Statement":"INSERT INTO .*‹t› VALUES \(‹1›, pg_sleep\(‹0.256›\), ‹'x'›\)","Tag":"INSERT","User":"root"`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `INSERT INTO t VALUES (2, pg_sleep(0.256), 'x')`,
			errRe:       ``,
			logRe:       `"EventType":"slow_query","Statement":"INSERT INTO .*‹t› VALUES \(‹2›, pg_sleep\(‹0.256›\), ‹'x'›\)","Tag":"INSERT","User":"root","ExecMode":"exec","NumRows":1`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `INSERT INTO t VALUES (3, false, repeat('x', 1024))`,
			errRe:       ``,
			logRe:       `"EventType":"large_row","RowSize":\d+,"TableID":\d+,"PrimaryKey":"‹/Table/\d+/1/3/0›"`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `INSERT INTO t VALUES (4, pg_sleep(0.256), repeat('x', 1024))`,
			errRe:       ``,
			logRe:       `"EventType":"slow_query","Statement":"INSERT INTO .*‹t› VALUES \(‹4›, pg_sleep\(‹0.256›\), repeat\(‹'x'›, ‹1024›\)\)","Tag":"INSERT","User":"root","ExecMode":"exec","NumRows":1`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `INSERT INTO t VALUES (5, false, repeat('x', 2048))`,
			errRe:       `row larger than max row size: table \d+ family 0 primary key /Table/\d+/1/5/0 size \d+`,
			logRe:       `"EventType":"large_row","RowSize":\d+,"TableID":\d+,"PrimaryKey":"‹/Table/\d+/1/5/0›"`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `SELECT *, pg_sleep(0.064) FROM t`,
			errRe:       ``,
			logRe:       `"EventType":"slow_query","Statement":"SELECT \*, pg_sleep\(‹0.064›\) FROM .*‹t›","Tag":"SELECT","User":"root","ExecMode":"exec","NumRows":4`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `SELECT *, pg_sleep(0.064) FROM t`,
			errRe:       ``,
			logRe:       `"EventType":"large_row"`,
			logExpected: false,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `INSERT INTO t VALUES (2, false, repeat('x', 2048)) ON CONFLICT (i) DO NOTHING`,
			errRe:       ``,
			logRe:       `"EventType":"large_row"`,
			logExpected: false,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `INSERT INTO t VALUES (2, false, 'x') ON CONFLICT (i) DO UPDATE SET s = repeat('x', 1024)`,
			errRe:       ``,
			logRe:       `"EventType":"large_row","RowSize":\d+,"TableID":\d+,"PrimaryKey":"‹/Table/\d+/1/2/0›"`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `INSERT INTO t VALUES (2, false, 'x') ON CONFLICT (i) DO UPDATE SET s = repeat('x', 2048)`,
			errRe:       `row larger than max row size: table \d+ family 0 primary key /Table/\d+/1/2/0 size \d+`,
			logRe:       `"EventType":"large_row","RowSize":\d+,"TableID":\d+,"PrimaryKey":"‹/Table/\d+/1/2/0›"`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `INSERT INTO t VALUES (2, false, repeat('x', 2048)) ON CONFLICT (i) DO UPDATE SET s = 'x'`,
			errRe:       ``,
			logRe:       `"EventType":"large_row"`,
			logExpected: false,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `UPSERT INTO t VALUES (2, false, repeat('x', 1024))`,
			errRe:       ``,
			logRe:       `"EventType":"large_row","RowSize":\d+,"TableID":\d+,"PrimaryKey":"‹/Table/\d+/1/2/0›"`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `UPSERT INTO t VALUES (2, false, repeat('x', 2048))`,
			errRe:       `row larger than max row size: table \d+ family 0 primary key /Table/\d+/1/2/0 size \d+`,
			logRe:       `"EventType":"large_row","RowSize":\d+,"TableID":\d+,"PrimaryKey":"‹/Table/\d+/1/2/0›"`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `UPSERT INTO t VALUES (2, false, 'x')`,
			errRe:       ``,
			logRe:       `"EventType":"large_row"`,
			logExpected: false,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `UPDATE t SET s = repeat('x', 1024) WHERE i = 2`,
			errRe:       ``,
			logRe:       `"EventType":"large_row","RowSize":\d+,"TableID":\d+,"PrimaryKey":"‹/Table/\d+/1/2/0›"`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `UPDATE t SET s = repeat('x', 2048) WHERE i = 2`,
			errRe:       `row larger than max row size: table \d+ family 0 primary key /Table/\d+/1/2/0 size \d+`,
			logRe:       `"EventType":"large_row","RowSize":\d+,"TableID":\d+,"PrimaryKey":"‹/Table/\d+/1/2/0›"`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `UPDATE t SET s = 'x' WHERE i = 2`,
			errRe:       ``,
			logRe:       `"EventType":"large_row"`,
			logExpected: false,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `DELETE FROM t WHERE i = 2`,
			errRe:       ``,
			logRe:       `"EventType":"large_row"`,
			logExpected: false,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `DELETE FROM t WHERE i = 3`,
			errRe:       ``,
			logRe:       `"EventType":"large_row"`,
			logExpected: false,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `ALTER TABLE t ADD COLUMN f FLOAT DEFAULT 99.999`,
			errRe:       ``,
			logRe:       `"EventType":"large_row_internal","RowSize":\d+,"TableID":\d+,"PrimaryKey":"‹/Table/\d+/1/4/0›"`,
			logExpected: true,
			breakHere:   true,
			channel:     channel.SQL_INTERNAL_PERF,
		},
		{
			query:       `CREATE TABLE t2 (i, s, PRIMARY KEY (i)) AS SELECT i, s FROM t`,
			errRe:       ``,
			logRe:       `"EventType":"large_row_internal","RowSize":\d+,"TableID":\d+,"PrimaryKey":"‹/Table/\d+/1/4/0›"`,
			logExpected: true,
			channel:     channel.SQL_INTERNAL_PERF,
		},
		{
			query:       `ALTER TABLE t2 ADD COLUMN z STRING DEFAULT repeat('z', 2048)`,
			errRe:       ``,
			logRe:       `"EventType":"large_row_internal","RowSize":\d+,"TableID":\d+,"PrimaryKey":"‹/Table/\d+/1/4/0›"`,
			logExpected: true,
			channel:     channel.SQL_INTERNAL_PERF,
		},
		{
			query:       `SELECT * FROM t2`,
			errRe:       ``,
			logRe:       `"EventType":"large_row"`,
			logExpected: false,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `DROP TABLE t2`,
			errRe:       ``,
			logRe:       `"EventType":"large_row"`,
			logExpected: false,
			channel:     channel.SQL_INTERNAL_PERF,
		},
		{
			query:       `TRUNCATE t`,
			errRe:       ``,
			logRe:       `"EventType":"large_row"`,
			logExpected: false,
			channel:     channel.SQL_INTERNAL_PERF,
		},
		{
			query:       `INSERT INTO u VALUES (1, 1, repeat('x', 1024))`,
			errRe:       ``,
			logRe:       `"EventType":"large_row","RowSize":\d+,"TableID":\d+,"FamilyID":1,"PrimaryKey":"‹/Table/\d+/1/1/1/1›"`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `INSERT INTO u VALUES (2, 2, repeat('x', 2048))`,
			errRe:       `pq: row larger than max row size: table \d+ family 1 primary key /Table/\d+/1/2/1/1 size \d+`,
			logRe:       `"EventType":"large_row","RowSize":\d+,"TableID":\d+,"FamilyID":1,"PrimaryKey":"‹/Table/\d+/1/2/1/1›"`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `UPDATE u SET j = j + 1 WHERE i = 1`,
			errRe:       ``,
			logRe:       `"EventType":"large_row"`,
			logExpected: false,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `UPDATE u SET i = i + 1 WHERE i = 1`,
			errRe:       ``,
			logRe:       `"EventType":"large_row","RowSize":\d+,"TableID":\d+,"FamilyID":1,"PrimaryKey":"‹/Table/\d+/1/2/1/1›"`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `UPDATE u SET s = 'x' WHERE i = 2`,
			errRe:       ``,
			logRe:       `"EventType":"large_row"`,
			logExpected: false,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `UPDATE u SET s = repeat('x', 2048) WHERE i = 2`,
			errRe:       `pq: row larger than max row size: table \d+ family 1 primary key /Table/\d+/1/2/1/1 size \d+`,
			logRe:       `"EventType":"large_row","RowSize":\d+,"TableID":\d+,"FamilyID":1,"PrimaryKey":"‹/Table/\d+/1/2/1/1›"`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `CREATE TABLE u2 (i, j, s, PRIMARY KEY (i), FAMILY f1 (i, j), FAMILY f2 (s)) AS SELECT i, j, repeat(s, 2048) FROM u`,
			errRe:       ``,
			logRe:       `"EventType":"large_row_internal","RowSize":\d+,"TableID":\d+,"FamilyID":1,"PrimaryKey":"‹/Table/\d+/1/2/1/1›"`,
			logExpected: true,
			channel:     channel.SQL_INTERNAL_PERF,
		},
		{
			query:       `UPDATE u2 SET j = j + 1 WHERE i = 2`,
			errRe:       ``,
			logRe:       `"EventType":"large_row"`,
			logExpected: false,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `UPDATE u2 SET i = i + 1 WHERE i = 2`,
			errRe:       `row larger than max row size: table \d+ family 1 primary key /Table/\d+/1/3/1/1 size \d+`,
			logRe:       `"EventType":"large_row","RowSize":\d+,"TableID":\d+,"FamilyID":1,"PrimaryKey":"‹/Table/\d+/1/3/1/1›"`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `UPDATE u2 SET s = 'x' WHERE i = 2`,
			errRe:       ``,
			logRe:       `"EventType":"large_row"`,
			logExpected: false,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `DROP TABLE u2`,
			errRe:       ``,
			logRe:       `"EventType":"large_row"`,
			logExpected: false,
			channel:     channel.SQL_INTERNAL_PERF,
		},

		// Tests for the limits on the number of txn rows written/read.
		{
			// Enable the relevant cluster settings and reset the session
			// variables to the values of the cluster settings just set.
			setup: `
                SET CLUSTER SETTING sql.defaults.transaction_rows_written_log = 2;
                SET CLUSTER SETTING sql.defaults.transaction_rows_written_err = 3;
                SET CLUSTER SETTING sql.defaults.transaction_rows_read_log = 2;
                SET CLUSTER SETTING sql.defaults.transaction_rows_read_err = 3;
                RESET transaction_rows_written_log;
                RESET transaction_rows_written_err;
                RESET transaction_rows_read_log;
                RESET transaction_rows_read_err;
            `,
		},
		{
			query:       `INSERT INTO t(i) VALUES (6)`,
			errRe:       ``,
			logRe:       `"EventType":"txn_rows_written_limit","Statement":"INSERT INTO.*","TxnID":".*","SessionID":".*"`,
			logExpected: false,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `INSERT INTO t(i) VALUES (7), (8), (9)`,
			errRe:       ``,
			logRe:       `"EventType":"txn_rows_written_limit","Statement":"INSERT INTO.*","TxnID":".*","SessionID":".*"`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			setup:       `INSERT INTO t(i) VALUES (-1), (-2), (-3)`,
			query:       `UPDATE t SET i = i - 10 WHERE i < 0`,
			errRe:       ``,
			logRe:       `"EventType":"txn_rows_written_limit","Statement":"UPDATE.*","TxnID":".*","SessionID":".*"`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			setup:       `BEGIN`,
			cleanup:     `COMMIT`,
			query:       `INSERT INTO t(i) VALUES (10); INSERT INTO t(i) VALUES (11), (12);`,
			errRe:       ``,
			logRe:       `"EventType":"txn_rows_written_limit","Statement":"INSERT INTO.*","TxnID":".*","SessionID":".*"`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			setup:       `SET transaction_rows_written_log = 1`,
			cleanup:     `RESET transaction_rows_written_log`,
			query:       `INSERT INTO t(i) VALUES (13), (14)`,
			errRe:       ``,
			logRe:       `"EventType":"txn_rows_written_limit","Statement":"INSERT INTO.*","TxnID":".*","SessionID":".*"`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `INSERT INTO t(i) VALUES (15), (16), (17), (18)`,
			errRe:       `pq: txn has written 4 rows, which is above the limit: TxnID .* SessionID .*`,
			logRe:       `"EventType":"txn_rows_written_limit","Statement":"INSERT INTO.*","TxnID":".*","SessionID":".*"`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			setup: `INSERT INTO t(i) VALUES (-1)`,
			// We now have 4 negative values in the table t.
			query:       `DELETE FROM t WHERE i < 0`,
			errRe:       `pq: txn has written 4 rows, which is above the limit: TxnID .* SessionID .*`,
			logRe:       `"EventType":"txn_rows_written_limit","Statement":"DELETE.*","TxnID":".*","SessionID":".*"`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `UPSERT INTO t(i) VALUES (-2), (-3), (-4), (-5)`,
			errRe:       `pq: txn has written 4 rows, which is above the limit: TxnID .* SessionID .*`,
			logRe:       `"EventType":"txn_rows_written_limit","Statement":"UPSERT INTO.*","TxnID":".*","SessionID":".*","NumRows":4`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			setup:       `BEGIN`,
			cleanup:     `ROLLBACK`,
			query:       `INSERT INTO t(i) VALUES (15), (16), (17); INSERT INTO t(i) VALUES (18);`,
			errRe:       `pq: txn has written 4 rows, which is above the limit: TxnID .* SessionID .*`,
			logRe:       `"EventType":"txn_rows_written_limit","Statement":"INSERT INTO.*","TxnID":".*","SessionID":".*","NumRows":3`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			setup:       `SET transaction_rows_written_err = 1`,
			cleanup:     `RESET transaction_rows_written_err`,
			query:       `INSERT INTO t(i) VALUES (15), (16)`,
			errRe:       `pq: txn has written 2 rows, which is above the limit: TxnID .* SessionID .*`,
			logRe:       `"EventType":"txn_rows_written_limit","Statement":"INSERT INTO.*","TxnID":".*","SessionID":".*"`,
			logExpected: false,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `SELECT * FROM t WHERE i = 6`,
			errRe:       ``,
			logRe:       `"EventType":"txn_rows_read_limit","Statement":"SELECT \* FROM .*‹t› WHERE ‹i› = ‹6›","Tag":"SELECT","User":"root","TxnID":.*,"SessionID":.*`,
			logExpected: false,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `SELECT * FROM t WHERE i IN (6, 7, 8)`,
			errRe:       ``,
			logRe:       `"EventType":"txn_rows_read_limit","Statement":"SELECT \* FROM .*‹t› WHERE ‹i› IN \(‹6›, ‹7›, ‹8›\)","Tag":"SELECT","User":"root","TxnID":.*,"SessionID":.*,"NumRows":3`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			setup:       `BEGIN`,
			cleanup:     `COMMIT`,
			query:       `SELECT * FROM t WHERE i = 6; SELECT * FROM t WHERE i = 7; SELECT * FROM t WHERE i = 8;`,
			errRe:       ``,
			logRe:       `"EventType":"txn_rows_read_limit","Statement":"SELECT \* FROM .*‹t› WHERE ‹i› = ‹8›","Tag":"SELECT","User":"root","TxnID":.*,"SessionID":.*,"NumRows":3`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			setup:       `SET transaction_rows_read_log = 1`,
			cleanup:     `RESET transaction_rows_read_log`,
			query:       `SELECT * FROM t WHERE i IN (6, 7)`,
			errRe:       ``,
			logRe:       `"EventType":"txn_rows_read_limit","Statement":"SELECT \* FROM .*‹t› WHERE ‹i› IN \(‹6›, ‹7›\)","Tag":"SELECT","User":"root","TxnID":.*,"SessionID":.*,"NumRows":2`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `SELECT * FROM t WHERE i IN (6, 7, 8, 9)`,
			errRe:       `pq: txn has read 4 rows, which is above the limit: TxnID .* SessionID .*`,
			logRe:       `"EventType":"txn_rows_read_limit","Statement":"SELECT \* FROM .*‹t› WHERE ‹i› IN \(‹6›, ‹7›, ‹8›, ‹9›\)","Tag":"SELECT","User":"root","TxnID":.*,"SessionID":.*,"NumRows":4`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			setup:       `BEGIN`,
			cleanup:     `ROLLBACK`,
			query:       `SELECT * FROM t WHERE i IN (6, 7); SELECT * FROM t WHERE i IN (8, 9)`,
			errRe:       `pq: txn has read 4 rows, which is above the limit: TxnID .* SessionID .*`,
			logRe:       `"EventType":"txn_rows_read_limit","Statement":"SELECT \* FROM .*‹t› WHERE ‹i› IN \(‹8›, ‹9›\)","Tag":"SELECT","User":"root","TxnID":.*,"SessionID":.*,"NumRows":4`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			setup:       `SET transaction_rows_read_err = 1`,
			cleanup:     `RESET transaction_rows_read_err`,
			query:       `SELECT * FROM t WHERE i = 6 OR i = 7`,
			errRe:       `pq: txn has read 2 rows, which is above the limit: TxnID .* SessionID .*`,
			logRe:       `"EventType":"txn_rows_read_limit","Statement":"SELECT \* FROM .*‹t› WHERE ‹i› = ‹6› OR ‹i› = ‹7›","Tag":"SELECT","User":"root","TxnID":.*,"SessionID":.*`,
			logExpected: false,
			channel:     channel.SQL_PERF,
		},
		{
			// Temporarily disable the "written" limits so that we can check
			// that a mutation can run into the "read" limits too.
			setup:       `SET transaction_rows_written_log = 0; SET transaction_rows_written_err = 0;`,
			cleanup:     `SET transaction_rows_written_log = 2; SET transaction_rows_written_err = 3;`,
			query:       `UPDATE t SET i = i - 10 WHERE i < 0`,
			errRe:       `pq: txn has read 4 rows, which is above the limit: TxnID .* SessionID .*`,
			logRe:       `"EventType":"txn_rows_read_limit","Statement":"UPDATE.*","TxnID":".*","SessionID":".*"`,
			logExpected: true,
			channel:     channel.SQL_PERF,
		},
		{
			cleanup:     `DROP TABLE t_copy`,
			query:       `CREATE TABLE t_copy (i PRIMARY KEY) AS SELECT i FROM t`,
			errRe:       ``,
			logRe:       `"EventType":"txn_rows_written_limit","Statement":"CREATE.*","TxnID":".*","SessionID":".*"`,
			logExpected: false,
			channel:     channel.SQL_PERF,
		},
		{
			cleanup:     `DROP TABLE t_copy`,
			query:       `CREATE TABLE t_copy (i PRIMARY KEY) AS SELECT i FROM t`,
			errRe:       ``,
			logRe:       `"EventType":"txn_rows_read_limit","Statement":"CREATE.*","TxnID":".*","SessionID":".*"`,
			logExpected: false,
			channel:     channel.SQL_PERF,
		},
		{
			cleanup:     `DROP TABLE t_copy`,
			query:       `CREATE TABLE t_copy (i PRIMARY KEY) AS SELECT i FROM t`,
			errRe:       ``,
			logRe:       `"EventType":"txn_rows_written_limit","Statement":"CREATE.*","TxnID":".*","SessionID":".*"`,
			logExpected: false,
			channel:     channel.SQL_INTERNAL_PERF,
		},
		{
			cleanup:     `DROP TABLE t_copy`,
			query:       `CREATE TABLE t_copy (i PRIMARY KEY) AS SELECT i FROM t`,
			errRe:       ``,
			logRe:       `"EventType":"txn_rows_read_limit","Statement":"CREATE.*","TxnID":".*","SessionID":".*"`,
			logExpected: false,
			channel:     channel.SQL_INTERNAL_PERF,
		},
		{
			setup:       `CREATE TABLE t_copy (i PRIMARY KEY) AS SELECT i FROM t`,
			query:       `DROP TABLE t_copy`,
			errRe:       ``,
			logRe:       `"EventType":"txn_rows_written_limit","Statement":"DROP.*","TxnID":".*","SessionID":".*"`,
			logExpected: false,
			channel:     channel.SQL_PERF,
		},
		{
			setup:       `CREATE TABLE t_copy (i PRIMARY KEY) AS SELECT i FROM t`,
			query:       `DROP TABLE t_copy`,
			errRe:       ``,
			logRe:       `"EventType":"txn_rows_read_limit","Statement":"DROP.*","TxnID":".*","SessionID":".*"`,
			logExpected: false,
			channel:     channel.SQL_PERF,
		},
		{
			setup:       `CREATE TABLE t_copy (i PRIMARY KEY) AS SELECT i FROM t`,
			query:       `DROP TABLE t_copy`,
			errRe:       ``,
			logRe:       `"EventType":"txn_rows_written_limit","Statement":"DROP.*","TxnID":".*","SessionID":".*"`,
			logExpected: false,
			channel:     channel.SQL_INTERNAL_PERF,
		},
		{
			setup:       `CREATE TABLE t_copy (i PRIMARY KEY) AS SELECT i FROM t`,
			query:       `DROP TABLE t_copy`,
			errRe:       ``,
			logRe:       `"EventType":"txn_rows_read_limit","Statement":"DROP.*","TxnID":".*","SessionID":".*"`,
			logExpected: false,
			channel:     channel.SQL_INTERNAL_PERF,
		},
		{
			query:       `ANALYZE t`,
			errRe:       ``,
			logRe:       `"EventType":"txn_rows_read_limit","Statement":"ANALYZE.*","TxnID":".*","SessionID":".*"`,
			logExpected: false,
			channel:     channel.SQL_PERF,
		},
		{
			query:       `ANALYZE t`,
			errRe:       ``,
			logRe:       `"EventType":"txn_rows_read_limit","Statement":"ANALYZE.*","TxnID":".*","SessionID":".*"`,
			logExpected: false,
			channel:     channel.SQL_INTERNAL_PERF,
		},
		{
			// Disable the relevant cluster settings and reset the session
			// variables to the values of the cluster settings just set.
			setup: `
                SET CLUSTER SETTING sql.defaults.transaction_rows_written_log = DEFAULT;
                SET CLUSTER SETTING sql.defaults.transaction_rows_written_err = DEFAULT;
                SET CLUSTER SETTING sql.defaults.transaction_rows_read_log = DEFAULT;
                SET CLUSTER SETTING sql.defaults.transaction_rows_read_err = DEFAULT;
								RESET transaction_rows_written_log;
                RESET transaction_rows_written_err;
                RESET transaction_rows_read_log;
                RESET transaction_rows_read_err;
            `,
		},
	}

	// Make file sinks for the SQL perf logs.
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)
	log.TestingResetActive()
	cfg := logconfig.DefaultConfig()
	auditable := true
	cfg.Sinks.FileGroups = map[string]*logconfig.FileSinkConfig{
		"sql-slow": {
			FileDefaults: logconfig.FileDefaults{
				CommonSinkConfig: logconfig.CommonSinkConfig{Auditable: &auditable},
			},
			Channels: logconfig.SelectChannels(channel.SQL_PERF, channel.SQL_INTERNAL_PERF),
		},
	}
	dir := sc.GetDirectory()
	if err := cfg.Validate(&dir); err != nil {
		t.Fatal(err)
	}
	cleanup, err := log.ApplyConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()

	// Start a SQL server.
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	// TODO(fqazi): Enable with MVCC back filler support, since max_row_size is
	// not properly enforced right now.
	_, err = sqlDB.Exec("SET CLUSTER SETTING sql.defaults.use_declarative_schema_changer='off'")
	require.NoError(t, err)
	_, err = sqlDB.Exec("SET use_declarative_schema_changer='off'")
	require.NoError(t, err)
	db := sqlutils.MakeSQLRunner(sqlDB)

	// Enable slow query logging and large row logging.
	db.Exec(t, `SET CLUSTER SETTING sql.log.slow_query.latency_threshold = '128ms'`)
	db.Exec(t, `SET CLUSTER SETTING sql.guardrails.max_row_size_log = '1KiB'`)
	db.Exec(t, `SET CLUSTER SETTING sql.guardrails.max_row_size_err = '2KiB'`)
	defer db.Exec(t, `SET CLUSTER SETTING sql.guardrails.max_row_size_err = DEFAULT`)
	defer db.Exec(t, `SET CLUSTER SETTING sql.guardrails.max_row_size_log = DEFAULT`)
	defer db.Exec(t, `SET CLUSTER SETTING sql.log.slow_query.latency_threshold = DEFAULT`)

	// Test schema.
	db.Exec(t, `CREATE TABLE t (i INT PRIMARY KEY, b BOOL, s STRING)`)
	db.Exec(t, `CREATE TABLE u (i INT PRIMARY KEY, j INT, s STRING, FAMILY f1 (i, j), FAMILY f2 (s))`)
	defer db.Exec(t, `DROP TABLE t, u`)

	for _, tc := range testCases {
		if tc.setup != "" {
			t.Log(tc.setup)
			db.ExecMultiple(t, strings.Split(tc.setup, ";")...)
			if tc.query == "" {
				continue
			}
		}
		if tc.breakHere {
			t.Log("FOUND")
		}
		t.Log(tc.query)
		start := timeutil.Now().UnixNano()
		if tc.errRe != "" {
			db.ExpectErr(t, tc.errRe, tc.query)
		} else {
			db.Exec(t, tc.query)
		}

		var logRe = regexp.MustCompile(tc.logRe)
		log.FlushFileSinks()
		entries, err := log.FetchEntriesFromFiles(
			start, math.MaxInt64, 1000, logRe, log.WithMarkedSensitiveData,
		)
		if err != nil {
			t.Fatal(err)
		}

		if (len(entries) > 0) != tc.logExpected {
			expected := "at least one message"
			if !tc.logExpected {
				expected = "zero messages"
			}
			t.Fatal(errors.Newf(
				"%v log messages for query `%s` matching `%s`, expected %s",
				len(entries), tc.query, tc.logRe, expected,
			))
		}

		for _, entry := range entries {
			t.Log(entry)
			if entry.Channel != tc.channel {
				t.Fatal(errors.Newf(
					"log message on channel %v, expected channel %v: %v", entry.Channel, tc.channel, entry,
				))
			}
		}

		if tc.cleanup != "" {
			t.Log(tc.cleanup)
			db.ExecMultiple(t, strings.Split(tc.cleanup, ";")...)
		}
	}
}
