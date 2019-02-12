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
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
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

func assertPayloads(t testing.TB, f testfeed, expected []string) {
	t.Helper()

	var actual []string
	for len(actual) < len(expected) {
		topic, _, key, value, _, ok := f.Next(t)
		if log.V(1) {
			log.Infof(context.TODO(), `%v %s: %s->%s`, ok, topic, key, value)
		}
		if !ok {
			t.Fatalf(`expected another row: %s`, f.Err())
		} else if key != nil || value != nil {
			actual = append(actual, fmt.Sprintf(`%s: %s->%s`, topic, key, value))
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

func assertPayloadsAvro(t testing.TB, reg *testSchemaRegistry, f testfeed, expected []string) {
	t.Helper()

	var actual []string
	for len(actual) < len(expected) {
		topic, _, keyBytes, valueBytes, _, ok := f.Next(t)
		if !ok {
			break
		} else if keyBytes != nil || valueBytes != nil {
			key, value := avroToJSON(t, reg, keyBytes), avroToJSON(t, reg, valueBytes)
			actual = append(actual, fmt.Sprintf(`%s: %s->%s`, topic, key, value))
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

func skipResolvedTimestamps(t *testing.T, f testfeed) {
	for {
		table, _, key, value, _, ok := f.Next(t)
		if !ok {
			break
		}
		if key != nil || value != nil {
			t.Errorf(`unexpected row %s: %s->%s`, table, key, value)
		}
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

func expectResolvedTimestamp(t testing.TB, f testfeed) hlc.Timestamp {
	t.Helper()
	topic, _, key, value, resolved, _ := f.Next(t)
	if key != nil || value != nil {
		t.Fatalf(`unexpected row %s: %s -> %s`, topic, key, value)
	}
	if resolved == nil {
		t.Fatal(`expected a resolved timestamp notification`)
	}

	var valueRaw struct {
		Resolved string `json:"resolved"`
	}
	if err := gojson.Unmarshal(resolved, &valueRaw); err != nil {
		t.Fatal(err)
	}

	return parseTimeToHLC(t, valueRaw.Resolved)
}

func expectResolvedTimestampAvro(t testing.TB, reg *testSchemaRegistry, f testfeed) hlc.Timestamp {
	t.Helper()
	topic, _, keyBytes, valueBytes, resolvedBytes, _ := f.Next(t)
	if keyBytes != nil || valueBytes != nil {
		key, value := avroToJSON(t, reg, keyBytes), avroToJSON(t, reg, valueBytes)
		t.Fatalf(`unexpected row %s: %s -> %s`, topic, key, value)
	}
	if resolvedBytes == nil {
		t.Fatal(`expected a resolved timestamp notification`)
	}
	resolvedNative, err := reg.encodedAvroToNative(resolvedBytes)
	if err != nil {
		t.Fatal(err)
	}
	resolved := resolvedNative.(map[string]interface{})[`resolved`]
	return parseTimeToHLC(t, resolved.(map[string]interface{})[`string`].(string))
}

func sinklessTest(testFn func(*testing.T, *gosql.DB, testfeedFactory)) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()
		knobs := base.TestingKnobs{DistSQL: &distsqlrun.TestingKnobs{Changefeed: &TestingKnobs{}}}
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

		f := makeSinkless(s)
		testFn(t, db, f)
	}
}

func enterpriseTest(testFn func(*testing.T, *gosql.DB, testfeedFactory)) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		flushCh := make(chan struct{}, 1)
		defer close(flushCh)
		knobs := base.TestingKnobs{DistSQL: &distsqlrun.TestingKnobs{Changefeed: &TestingKnobs{
			AfterSinkFlush: func() error {
				select {
				case flushCh <- struct{}{}:
				default:
				}
				return nil
			},
		}}}

		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
			UseDatabase: "d",
			Knobs:       knobs,
		})
		defer s.Stopper().Stop(ctx)
		sqlDB := sqlutils.MakeSQLRunner(db)
		// TODO(dan): Switch this to RangeFeed, too. It seems wasteful right now
		// because the RangeFeed version of the tests take longer due to
		// closed_timestamp.target_duration's interaction with schema changes.
		sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.push.enabled = false`)
		sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '10ms'`)
		sqlDB.Exec(t, `CREATE DATABASE d`)
		f := makeTable(s, db, flushCh)

		testFn(t, db, f)
	}
}

func pollerTest(
	metaTestFn func(func(*testing.T, *gosql.DB, testfeedFactory)) func(*testing.T),
	testFn func(*testing.T, *gosql.DB, testfeedFactory),
) func(*testing.T) {
	return func(t *testing.T) {
		metaTestFn(func(t *testing.T, db *gosql.DB, f testfeedFactory) {
			sqlDB := sqlutils.MakeSQLRunner(db)
			sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.push.enabled = false`)
			sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '10ms'`)
			testFn(t, db, f)
		})(t)
	}
}

func forceTableGC(
	t testing.TB,
	tsi serverutils.TestServerInterface,
	sqlDB *sqlutils.SQLRunner,
	database, table string,
) {
	t.Helper()
	tblID := sqlutils.QueryTableID(t, sqlDB.DB, database, table)

	tblKey := roachpb.Key(keys.MakeTablePrefix(tblID))
	gcr := roachpb.GCRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    tblKey,
			EndKey: tblKey.PrefixEnd(),
		},
		Threshold: tsi.Clock().Now(),
	}
	if _, err := client.SendWrapped(context.Background(), tsi.DistSender(), &gcr); err != nil {
		t.Fatal(err)
	}
}
