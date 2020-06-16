// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	gosql "database/sql"
	gojson "encoding/json"
	"fmt"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func waitForSchemaChange(
	t testing.TB, sqlDB *sqlutils.SQLRunner, stmt string, arguments ...interface{},
) {
	sqlDB.Exec(t, stmt, arguments...)
	row := sqlDB.QueryRow(t, "SELECT job_id FROM [SHOW JOBS] ORDER BY created DESC LIMIT 1")
	var jobID string
	row.Scan(&jobID)

	testutils.SucceedsSoon(t, func() error {
		row := sqlDB.QueryRow(t, "SELECT status FROM [SHOW JOBS] WHERE job_id = $1", jobID)
		var status string
		row.Scan(&status)
		if status != "succeeded" {
			return fmt.Errorf("Job %s had status %s, wanted 'succeeded'", jobID, status)
		}
		return nil
	})
}

func readNextMessages(t testing.TB, f cdctest.TestFeed, numMessages int, stripTs bool) []string {
	t.Helper()

	var actual []string
	var value []byte
	var message map[string]interface{}
	for len(actual) < numMessages {
		m, err := f.Next()
		if log.V(1) {
			log.Infof(context.Background(), `%v %s: %s->%s`, err, m.Topic, m.Key, m.Value)
		}
		if err != nil {
			t.Fatal(err)
		} else if m == nil {
			t.Fatal(`expected message`)
		} else if len(m.Key) > 0 || len(m.Value) > 0 {
			if stripTs {
				if err := gojson.Unmarshal(m.Value, &message); err != nil {
					t.Fatalf(`%s: %s`, m.Value, err)
				}
				delete(message, "updated")
				value, err = cdctest.ReformatJSON(message)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				value = m.Value
			}
			actual = append(actual, fmt.Sprintf(`%s: %s->%s`, m.Topic, m.Key, value))
		}
	}
	return actual
}

func assertPayloadsBase(t testing.TB, f cdctest.TestFeed, expected []string, stripTs bool) {
	t.Helper()
	actual := readNextMessages(t, f, len(expected), stripTs)
	sort.Strings(expected)
	sort.Strings(actual)
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected\n  %s\ngot\n  %s",
			strings.Join(expected, "\n  "), strings.Join(actual, "\n  "))
	}
}

func assertPayloads(t testing.TB, f cdctest.TestFeed, expected []string) {
	t.Helper()
	assertPayloadsBase(t, f, expected, false)
}

func assertPayloadsStripTs(t testing.TB, f cdctest.TestFeed, expected []string) {
	t.Helper()
	assertPayloadsBase(t, f, expected, true)
}

func avroToJSON(t testing.TB, reg *testSchemaRegistry, avroBytes []byte) []byte {
	if len(avroBytes) == 0 {
		return nil
	}
	native, err := reg.encodedAvroToNative(avroBytes)
	if err != nil {
		t.Fatal(err)
	}
	// The avro textual format is a more natural fit, but it's non-deterministic
	// because of go's randomized map ordering. Instead, we use gojson.Marshal,
	// which sorts its object keys and so is deterministic.
	json, err := gojson.Marshal(native)
	if err != nil {
		t.Fatal(err)
	}
	return json
}

func assertPayloadsAvro(
	t testing.TB, reg *testSchemaRegistry, f cdctest.TestFeed, expected []string,
) {
	t.Helper()

	var actual []string
	for len(actual) < len(expected) {
		m, err := f.Next()
		if err != nil {
			t.Fatal(err)
		} else if m == nil {
			t.Fatal(`expected message`)
		} else if m.Key != nil {
			key, value := avroToJSON(t, reg, m.Key), avroToJSON(t, reg, m.Value)
			actual = append(actual, fmt.Sprintf(`%s: %s->%s`, m.Topic, key, value))
		}
	}

	// The tests that use this aren't concerned with order, just that these are
	// the next len(expected) messages.
	sort.Strings(expected)
	sort.Strings(actual)
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected\n  %s\ngot\n  %s",
			strings.Join(expected, "\n  "), strings.Join(actual, "\n  "))
	}
}

func parseTimeToHLC(t testing.TB, s string) hlc.Timestamp {
	t.Helper()
	d, _, err := apd.NewFromString(s)
	if err != nil {
		t.Fatal(err)
	}
	ts, err := tree.DecimalToHLC(d)
	if err != nil {
		t.Fatal(err)
	}
	return ts
}

func expectResolvedTimestamp(t testing.TB, f cdctest.TestFeed) hlc.Timestamp {
	t.Helper()
	m, err := f.Next()
	if err != nil {
		t.Fatal(err)
	} else if m == nil {
		t.Fatal(`expected message`)
	}
	if m.Key != nil {
		t.Fatalf(`unexpected row %s: %s -> %s`, m.Topic, m.Key, m.Value)
	}
	if m.Resolved == nil {
		t.Fatal(`expected a resolved timestamp notification`)
	}

	var resolvedRaw struct {
		Resolved string `json:"resolved"`
	}
	if err := gojson.Unmarshal(m.Resolved, &resolvedRaw); err != nil {
		t.Fatal(err)
	}

	return parseTimeToHLC(t, resolvedRaw.Resolved)
}

func expectResolvedTimestampAvro(
	t testing.TB, reg *testSchemaRegistry, f cdctest.TestFeed,
) hlc.Timestamp {
	t.Helper()
	m, err := f.Next()
	if err != nil {
		t.Fatal(err)
	} else if m == nil {
		t.Fatal(`expected message`)
	}
	if m.Key != nil {
		key, value := avroToJSON(t, reg, m.Key), avroToJSON(t, reg, m.Value)
		t.Fatalf(`unexpected row %s: %s -> %s`, m.Topic, key, value)
	}
	if m.Resolved == nil {
		t.Fatal(`expected a resolved timestamp notification`)
	}
	resolvedNative, err := reg.encodedAvroToNative(m.Resolved)
	if err != nil {
		t.Fatal(err)
	}
	resolved := resolvedNative.(map[string]interface{})[`resolved`]
	return parseTimeToHLC(t, resolved.(map[string]interface{})[`string`].(string))
}

func sinklessTest(testFn func(*testing.T, *gosql.DB, cdctest.TestFeedFactory)) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		knobs := base.TestingKnobs{DistSQL: &execinfra.TestingKnobs{Changefeed: &TestingKnobs{}}}
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			Knobs:       knobs,
			UseDatabase: `d`,
		})
		defer s.Stopper().Stop(ctx)
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
		// TODO(dan): We currently have to set this to an extremely conservative
		// value because otherwise schema changes become flaky (they don't commit
		// their txn in time, get pushed by closed timestamps, and retry forever).
		// This is more likely when the tests run slower (race builds or inside
		// docker). The conservative value makes our tests take a lot longer,
		// though. Figure out some way to speed this up.
		sqlDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s'`)
		// TODO(dan): This is still needed to speed up table_history, that should be
		// moved to RangeFeed as well.
		sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '10ms'`)
		sqlDB.Exec(t, `CREATE DATABASE d`)

		sink, cleanup := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
		defer cleanup()
		f := cdctest.MakeSinklessFeedFactory(s, sink)
		testFn(t, db, f)
	}
}

func enterpriseTest(testFn func(*testing.T, *gosql.DB, cdctest.TestFeedFactory)) func(*testing.T) {
	return enterpriseTestWithServerArgs(nil, testFn)
}

func enterpriseTestWithServerArgs(
	argsFn func(args *base.TestServerArgs),
	testFn func(*testing.T, *gosql.DB, cdctest.TestFeedFactory),
) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		flushCh := make(chan struct{}, 1)
		defer close(flushCh)
		knobs := base.TestingKnobs{DistSQL: &execinfra.TestingKnobs{Changefeed: &TestingKnobs{
			AfterSinkFlush: func() error {
				select {
				case flushCh <- struct{}{}:
				default:
				}
				return nil
			},
		}}}
		args := base.TestServerArgs{
			UseDatabase: "d",
			Knobs:       knobs,
		}
		if argsFn != nil {
			argsFn(&args)
		}
		s, db, _ := serverutils.StartServer(t, args)
		defer s.Stopper().Stop(ctx)
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
		sqlDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s'`)
		sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '10ms'`)
		sqlDB.Exec(t, `CREATE DATABASE d`)
		sink, cleanup := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
		defer cleanup()
		f := cdctest.MakeTableFeedFactory(s, db, flushCh, sink)

		testFn(t, db, f)
	}
}

func cloudStorageTest(
	testFn func(*testing.T, *gosql.DB, cdctest.TestFeedFactory),
) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		dir, dirCleanupFn := testutils.TempDir(t)
		defer dirCleanupFn()

		flushCh := make(chan struct{}, 1)
		defer close(flushCh)
		knobs := base.TestingKnobs{DistSQL: &execinfra.TestingKnobs{Changefeed: &TestingKnobs{
			AfterSinkFlush: func() error {
				select {
				case flushCh <- struct{}{}:
				default:
				}
				return nil
			},
		}}}

		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase:   "d",
			ExternalIODir: dir,
			Knobs:         knobs,
		})
		defer s.Stopper().Stop(ctx)
		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
		sqlDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s'`)
		sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '10ms'`)
		sqlDB.Exec(t, `CREATE DATABASE d`)

		f := cdctest.MakeCloudFeedFactory(s, db, dir, flushCh)
		testFn(t, db, f)
	}
}

func feed(
	t testing.TB, f cdctest.TestFeedFactory, create string, args ...interface{},
) cdctest.TestFeed {
	t.Helper()
	feed, err := f.Feed(create, args...)
	if err != nil {
		t.Fatal(err)
	}
	return feed
}

func closeFeed(t testing.TB, f cdctest.TestFeed) {
	t.Helper()
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func forceTableGC(
	t testing.TB,
	tsi serverutils.TestServerInterface,
	sqlDB *sqlutils.SQLRunner,
	database, table string,
) {
	t.Helper()
	if err := tsi.ForceTableGC(context.Background(), database, table, tsi.Clock().Now()); err != nil {
		t.Fatal(err)
	}
}
