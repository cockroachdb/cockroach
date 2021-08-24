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
	log.Flush()

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
		// Query to execute.
		query string
		// Regular expression the error message must match ("" for no error).
		errRe string
		// Regular expression used to search log messages from all channels.
		logRe string
		// Whether we expect to find any log messages matching logRe.
		logExpected bool
		// Logging channel all log messages matching logRe must be in.
		channel logpb.Channel
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
			query:       `INSERT INTO t VALUES (2, false, repeat('x', 1024)) ON CONFLICT (i) DO NOTHING`,
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
			query:       `INSERT INTO t VALUES (2, false, repeat('x', 1024)) ON CONFLICT (i) DO UPDATE SET s = 'x'`,
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
			Channels: logconfig.ChannelList{
				Channels: []log.Channel{channel.SQL_PERF, channel.SQL_INTERNAL_PERF},
			},
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
	db := sqlutils.MakeSQLRunner(sqlDB)

	// Enable slow query logging and large row logging.
	db.Exec(t, `SET CLUSTER SETTING sql.log.slow_query.latency_threshold = '128ms'`)
	db.Exec(t, `SET CLUSTER SETTING sql.mutations.max_row_size.log = '1KiB'`)

	// Test schema.
	db.Exec(t, `CREATE TABLE t (i INT PRIMARY KEY, b BOOL, s STRING)`)
	db.Exec(t, `CREATE TABLE u (i INT PRIMARY KEY, j INT, s STRING, FAMILY f1 (i, j), FAMILY f2 (s))`)
	defer db.Exec(t, `DROP TABLE t, u`)

	for _, tc := range testCases {
		t.Log(tc.query)
		start := timeutil.Now().UnixNano()
		if tc.errRe != "" {
			db.ExpectErr(t, tc.errRe, tc.query)
		} else {
			db.Exec(t, tc.query)
		}

		var logRe = regexp.MustCompile(tc.logRe)
		log.Flush()
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
	}
}
