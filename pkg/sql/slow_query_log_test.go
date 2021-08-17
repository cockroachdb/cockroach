package sql

import (
	"context"
	"math"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/channel"
	"github.com/cockroachdb/cockroach/pkg/util/log/logconfig"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var testCases = []struct {
	query    string
	errRe    string
	logRe    string
	expected int
	channel  logpb.Channel
}{
	{
		query:    `SELECT pg_sleep(0.256)`,
		errRe:    ``,
		logRe:    `"EventType":"slow_query","Statement":"SELECT pg_sleep\(‹0.256›\)","Tag":"SELECT","User":"root","ExecMode":"exec","NumRows":1`,
		expected: 1,
		channel:  channel.SQL_PERF,
	},
	{
		query:    `SELECT 1`,
		errRe:    ``,
		logRe:    `"EventType":"slow_query"`,
		expected: 0,
		channel:  channel.SQL_PERF,
	},
	// The following testcases must be executed in order.
	{
		query:    `INSERT INTO t SELECT 1, false, 'x'`,
		errRe:    ``,
		logRe:    `"EventType":"slow_query"`,
		expected: 0,
		channel:  channel.SQL_PERF,
	},
	{
		query:    `INSERT INTO t VALUES (1, pg_sleep(0.256), 'x')`,
		errRe:    `duplicate key`,
		logRe:    `"EventType":"slow_query","Statement":"INSERT INTO .*‹t› VALUES \(‹1›, pg_sleep\(‹0.256›\), ‹'x'›\)","Tag":"INSERT","User":"root"`,
		expected: 1,
		channel:  channel.SQL_PERF,
	},
	{
		query:    `INSERT INTO t VALUES (2, pg_sleep(0.256), 'x')`,
		errRe:    ``,
		logRe:    `"EventType":"slow_query","Statement":"INSERT INTO .*‹t› VALUES \(‹2›, pg_sleep\(‹0.256›\), ‹'x'›\)","Tag":"INSERT","User":"root","ExecMode":"exec","NumRows":1`,
		expected: 1,
		channel:  channel.SQL_PERF,
	},
	{
		query:    `INSERT INTO t VALUES (3, false, repeat('x', 2048))`,
		errRe:    ``,
		logRe:    `"EventType":"large_row","RowSize":2061,"MaxRowSize":1024,"TableID":\d+,"IndexID":1,"Key":"‹/Table/\d+/1/3/0›"`,
		expected: 1,
		channel:  channel.SQL_PERF,
	},
	{
		query:    `INSERT INTO t VALUES (4, pg_sleep(0.256), repeat('x', 2048))`,
		errRe:    ``,
		logRe:    `"EventType":"slow_query","Statement":"INSERT INTO .*‹t› VALUES \(‹4›, pg_sleep\(‹0.256›\), repeat\(‹'x'›, ‹2048›\)\)","Tag":"INSERT","User":"root","ExecMode":"exec","NumRows":1`,
		expected: 1,
		channel:  channel.SQL_PERF,
	},
	{
		query:    `SELECT *, pg_sleep(0.064) FROM t`,
		errRe:    ``,
		logRe:    `"EventType":"slow_query","Statement":"SELECT \*, pg_sleep\(‹0.064›\) FROM .*‹t›","Tag":"SELECT","User":"root","ExecMode":"exec","NumRows":4`,
		expected: 1,
		channel:  channel.SQL_PERF,
	},
	{
		query:    `SELECT *, pg_sleep(0.064) FROM t`,
		errRe:    ``,
		logRe:    `"EventType":"large_row"`,
		expected: 0,
		channel:  channel.SQL_PERF,
	},
	{
		query:    `INSERT INTO t VALUES (2, false, repeat('x', 2048)) ON CONFLICT (i) DO NOTHING`,
		errRe:    ``,
		logRe:    `"EventType":"large_row"`,
		expected: 0,
		channel:  channel.SQL_PERF,
	},
	{
		query:    `INSERT INTO t VALUES (2, false, 'x') ON CONFLICT (i) DO UPDATE SET s = repeat('x', 2048)`,
		errRe:    ``,
		logRe:    `"EventType":"large_row","RowSize":\d+,"MaxRowSize":1024,"TableID":\d+,"IndexID":1,"Key":"‹/Table/\d+/1/2/0›"`,
		expected: 1,
		channel:  channel.SQL_PERF,
	},
	{
		query:    `INSERT INTO t VALUES (2, false, repeat('x', 2048)) ON CONFLICT (i) DO UPDATE SET s = 'x'`,
		errRe:    ``,
		logRe:    `"EventType":"large_row"`,
		expected: 0,
		channel:  channel.SQL_PERF,
	},
	{
		query:    `UPSERT INTO t VALUES (2, false, repeat('x', 2048))`,
		errRe:    ``,
		logRe:    `"EventType":"large_row","RowSize":\d+,"MaxRowSize":1024,"TableID":\d+,"IndexID":1,"Key":"‹/Table/\d+/1/2/0›"`,
		expected: 1,
		channel:  channel.SQL_PERF,
	},
	{
		query:    `UPSERT INTO t VALUES (2, false, 'x')`,
		errRe:    ``,
		logRe:    `"EventType":"large_row"`,
		expected: 0,
		channel:  channel.SQL_PERF,
	},
	{
		query:    `UPDATE t SET s = repeat('x', 2048) WHERE i = 2`,
		errRe:    ``,
		logRe:    `"EventType":"large_row","RowSize":\d+,"MaxRowSize":1024,"TableID":\d+,"IndexID":1,"Key":"‹/Table/\d+/1/2/0›"`,
		expected: 1,
		channel:  channel.SQL_PERF,
	},
	{
		query:    `UPDATE t SET s = 'x' WHERE i = 2`,
		errRe:    ``,
		logRe:    `"EventType":"large_row"`,
		expected: 0,
		channel:  channel.SQL_PERF,
	},
	{
		query:    `DELETE FROM t WHERE i = 2`,
		errRe:    ``,
		logRe:    `"EventType":"large_row"`,
		expected: 0,
		channel:  channel.SQL_PERF,
	},
	{
		query:    `DELETE FROM t WHERE i = 3`,
		errRe:    ``,
		logRe:    `"EventType":"large_row"`,
		expected: 0,
		channel:  channel.SQL_PERF,
	},
	{
		query:    `ALTER TABLE t ADD COLUMN f FLOAT DEFAULT 99.999`,
		errRe:    ``,
		logRe:    `"EventType":"large_row_internal","RowSize":\d+,"MaxRowSize":1024,"TableID":\d+,"IndexID":1,"Key":"‹/Table/\d+/1/4/0›"`,
		expected: 1,
		channel:  channel.SQL_INTERNAL_PERF,
	},
	{
		query:    `CREATE TABLE t2 (i, s, PRIMARY KEY (i)) AS SELECT i, s FROM t`,
		errRe:    ``,
		logRe:    `"EventType":"large_row_internal","RowSize":\d+,"MaxRowSize":1024,"TableID":\d+,"IndexID":1,"Key":"‹/Table/\d+/1/4/0›"`,
		expected: 1,
		channel:  channel.SQL_INTERNAL_PERF,
	},
	{
		query:    `DROP TABLE t2`,
		errRe:    ``,
		logRe:    `"EventType":"large_row"`,
		expected: 0,
		channel:  channel.SQL_INTERNAL_PERF,
	},
	{
		query:    `TRUNCATE t`,
		errRe:    ``,
		logRe:    `"EventType":"large_row"`,
		expected: 0,
		channel:  channel.SQL_INTERNAL_PERF,
	},
	{
		query:    `INSERT INTO u VALUES (1, 1, repeat('x', 2048))`,
		errRe:    ``,
		logRe:    `"EventType":"large_row","RowSize":\d+,"MaxRowSize":1024,"TableID":\d+,"IndexID":1,"FamilyID":1,"Key":"‹/Table/\d+/1/1/1/1›"`,
		expected: 1,
		channel:  channel.SQL_PERF,
	},
	{
		query:    `UPDATE u SET j = j + 1 WHERE i = 1`,
		errRe:    ``,
		logRe:    `"EventType":"large_row"`,
		expected: 0,
		channel:  channel.SQL_PERF,
	},
	{
		query:    `UPDATE u SET i = i + 1 WHERE i = 1`,
		errRe:    ``,
		logRe:    `"EventType":"large_row","RowSize":\d+,"MaxRowSize":1024,"TableID":\d+,"IndexID":1,"FamilyID":1,"Key":"‹/Table/\d+/1/2/1/1›"`,
		expected: 1,
		channel:  channel.SQL_PERF,
	},
	{
		query:    `UPDATE u SET s = 'x' WHERE i = 2`,
		errRe:    ``,
		logRe:    `"EventType":"large_row"`,
		expected: 0,
		channel:  channel.SQL_PERF,
	},
}

func TestSlowQueryLog(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Make file sinks for the external and internal slow query logs.
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
	db.Exec(t, `SET CLUSTER SETTING sql.mutations.max_row_size.warn = '1KiB'`)
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

		if len(entries) != tc.expected {
			t.Fatal(errors.Newf(
				"%v log messages for query `%s` matching `%s`, expected %v",
				len(entries), tc.query, tc.logRe, tc.expected,
			))
		}

		for _, entry := range entries {
			t.Log(entry)
			if entry.Channel != tc.channel {
				t.Fatal(errors.Newf(
					"log message on channel %v, expected %v: %v", entry.Channel, tc.channel, entry,
				))
			}
		}
	}
}
