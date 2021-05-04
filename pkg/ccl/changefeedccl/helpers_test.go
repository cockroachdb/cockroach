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
	"time"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdctest"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	// Imported to allow locality-related table mutations
	_ "github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/partitionccl"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var testSinkFlushFrequency = 100 * time.Millisecond

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

func readNextMessages(f cdctest.TestFeed, numMessages int, stripTs bool) ([]string, error) {
	var actual []string
	for len(actual) < numMessages {
		m, err := f.Next()
		if log.V(1) {
			if m != nil {
				log.Infof(context.Background(), `msg %s: %s->%s (%s)`, m.Topic, m.Key, m.Value, m.Resolved)
			} else {
				log.Infof(context.Background(), `err %v`, err)
			}
		}
		if err != nil {
			return nil, err
		}
		if m == nil {
			return nil, errors.AssertionFailedf(`expected message`)
		}
		if len(m.Key) > 0 || len(m.Value) > 0 {
			var value []byte
			if stripTs {
				var message map[string]interface{}
				if err := gojson.Unmarshal(m.Value, &message); err != nil {
					return nil, errors.Newf(`unmarshal: %s: %s`, m.Value, err)
				}
				delete(message, "updated")
				value, err = reformatJSON(message)
				if err != nil {
					return nil, err
				}
			} else {
				value = m.Value
			}
			actual = append(actual, fmt.Sprintf(`%s: %s->%s`, m.Topic, m.Key, value))
		}
	}
	return actual, nil
}

func assertPayloadsBase(t testing.TB, f cdctest.TestFeed, expected []string, stripTs bool) {
	t.Helper()
	require.NoError(t, assertPayloadsBaseErr(f, expected, stripTs))
}

func assertPayloadsBaseErr(f cdctest.TestFeed, expected []string, stripTs bool) error {
	actual, err := readNextMessages(f, len(expected), stripTs)
	if err != nil {
		return err
	}
	sort.Strings(expected)
	sort.Strings(actual)
	if !reflect.DeepEqual(expected, actual) {
		return errors.Newf("expected\n  %s\ngot\n  %s",
			strings.Join(expected, "\n  "), strings.Join(actual, "\n  "))
	}
	return nil
}

func assertPayloads(t testing.TB, f cdctest.TestFeed, expected []string) {
	t.Helper()
	assertPayloadsBase(t, f, expected, false)
}

func assertPayloadsStripTs(t testing.TB, f cdctest.TestFeed, expected []string) {
	t.Helper()
	assertPayloadsBase(t, f, expected, true)
}

func avroToJSON(t testing.TB, reg *cdctest.SchemaRegistry, avroBytes []byte) []byte {
	json, err := reg.AvroToJSON(avroBytes)
	require.NoError(t, err)
	return json
}

func assertPayloadsAvro(
	t testing.TB, reg *cdctest.SchemaRegistry, f cdctest.TestFeed, expected []string,
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

func assertRegisteredSubjects(t testing.TB, reg *cdctest.SchemaRegistry, expected []string) {
	t.Helper()

	actual := reg.Subjects()
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
	return extractResolvedTimestamp(t, m)
}

func extractResolvedTimestamp(t testing.TB, m *cdctest.TestFeedMessage) hlc.Timestamp {
	t.Helper()
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
	t testing.TB, reg *cdctest.SchemaRegistry, f cdctest.TestFeed,
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
	resolvedNative, err := reg.EncodedAvroToNative(m.Resolved)
	if err != nil {
		t.Fatal(err)
	}
	resolved := resolvedNative.(map[string]interface{})[`resolved`]
	return parseTimeToHLC(t, resolved.(map[string]interface{})[`string`].(string))
}

type cdcTestFn func(*testing.T, *gosql.DB, cdctest.TestFeedFactory)
type updateArgsFn func(args *base.TestServerArgs)

func startTestServer(
	t testing.TB, argsFn updateArgsFn,
) (serverutils.TestServerInterface, *gosql.DB, func()) {
	knobs := base.TestingKnobs{DistSQL: &execinfra.TestingKnobs{Changefeed: &TestingKnobs{}}}
	args := base.TestServerArgs{
		Knobs:       knobs,
		UseDatabase: `d`,
	}
	if argsFn != nil {
		argsFn(&args)
	}

	ctx := context.Background()
	resetFlushFrequency := changefeedbase.TestingSetDefaultFlushFrequency(testSinkFlushFrequency)
	resetAdoptionIntervals := jobs.TestingSetAdoptAndCancelIntervals(
		10*time.Millisecond, 10*time.Millisecond)
	s, db, _ := serverutils.StartServer(t, args)

	cleanup := func() {
		s.Stopper().Stop(ctx)
		resetAdoptionIntervals()
		resetFlushFrequency()
	}
	var err error
	defer func() {
		if err != nil {
			cleanup()
			require.NoError(t, err)
		}
	}()

	_, err = db.ExecContext(ctx, `
SET CLUSTER SETTING kv.rangefeed.enabled = true;
SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s';
SET CLUSTER SETTING changefeed.experimental_poll_interval = '10ms';
SET CLUSTER SETTING sql.defaults.vectorize=on;
CREATE DATABASE d;
`)

	if region := serverArgsRegion(args); region != "" {
		_, err = db.ExecContext(ctx, fmt.Sprintf(`ALTER DATABASE d PRIMARY REGION "%s"`, region))
	}

	return s, db, cleanup
}

func sinklessTestWithServerArgs(
	argsFn func(args *base.TestServerArgs), testFn cdcTestFn,
) func(*testing.T) {
	return func(t *testing.T) {
		s, db, stopServer := startTestServer(t, argsFn)
		defer stopServer()

		sink, cleanup := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
		defer cleanup()
		f := makeSinklessFeedFactory(s, sink)
		testFn(t, db, f)
	}
}

func sinklessTest(testFn cdcTestFn) func(*testing.T) {
	return sinklessTestWithServerArgs(nil, testFn)
}

func enterpriseTest(testFn cdcTestFn) func(*testing.T) {
	return enterpriseTestWithServerArgs(nil, testFn)
}

func enterpriseTestWithServerArgs(
	argsFn func(args *base.TestServerArgs), testFn cdcTestFn,
) func(*testing.T) {
	return func(t *testing.T) {
		s, db, stopServer := startTestServer(t, argsFn)
		defer stopServer()

		sink, cleanup := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
		defer cleanup()
		f := makeTableFeedFactory(s, db, sink)

		testFn(t, db, f)
	}
}

func serverArgsRegion(args base.TestServerArgs) string {
	for _, tier := range args.Locality.Tiers {
		if tier.Key == "region" {
			return tier.Value
		}
	}
	return ""
}

func cloudStorageTest(testFn cdcTestFn) func(*testing.T) {
	return cloudStorageTestWithServerArg(nil, testFn)
}

func cloudStorageTestWithServerArg(
	argsFn func(args *base.TestServerArgs), testFn cdcTestFn,
) func(*testing.T) {
	return func(t *testing.T) {
		dir, dirCleanupFn := testutils.TempDir(t)
		defer dirCleanupFn()

		setExternalDir := func(args *base.TestServerArgs) {
			if argsFn != nil {
				argsFn(args)
			}
			args.ExternalIODir = dir
		}

		s, db, stopServer := startTestServer(t, setExternalDir)
		defer stopServer()

		f := makeCloudFeedFactory(s, db, dir)
		testFn(t, db, f)
	}
}

func kafkaTest(testFn cdcTestFn) func(t *testing.T) {
	return kafkaTestWithServerArgs(nil, testFn)
}

func kafkaTestWithServerArgs(
	argsFn func(args *base.TestServerArgs), testFn cdcTestFn,
) func(*testing.T) {
	return func(t *testing.T) {
		s, db, stopServer := startTestServer(t, argsFn)
		defer stopServer()
		f := makeKafkaFeedFactory(s, db)
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
