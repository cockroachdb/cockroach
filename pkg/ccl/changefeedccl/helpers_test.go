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

	"github.com/Shopify/sarama"
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
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
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

func readNextMessages(t testing.TB, f cdctest.TestFeed, numMessages int, stripTs bool) []string {
	t.Helper()

	var actual []string
	var value []byte
	var message map[string]interface{}
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

func sinklessTestWithServerArgs(
	argsFn func(args *base.TestServerArgs), testFn cdcTestFn,
) func(*testing.T) {
	return func(t *testing.T) {
		defer changefeedbase.TestingSetDefaultFlushFrequency(testSinkFlushFrequency)()
		ctx := context.Background()
		knobs := base.TestingKnobs{DistSQL: &execinfra.TestingKnobs{Changefeed: &TestingKnobs{}}}
		args := base.TestServerArgs{
			Knobs:       knobs,
			UseDatabase: `d`,
		}
		if argsFn != nil {
			argsFn(&args)
		}
		s, db, _ := serverutils.StartServer(t, args)
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
		// Change a couple of settings related to the vectorized engine in
		// order to ensure that changefeeds work as expected with them (note
		// that we'll still use the row-by-row engine, see #55605).
		sqlDB.Exec(t, `SET CLUSTER SETTING sql.defaults.vectorize=on`)
		sqlDB.Exec(t, `CREATE DATABASE d`)
		if region := serverArgsRegion(args); region != "" {
			sqlDB.Exec(t, fmt.Sprintf(`ALTER DATABASE d PRIMARY REGION "%s"`, region))
		}
		sink, cleanup := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
		defer cleanup()
		f := cdctest.MakeSinklessFeedFactory(s, sink)
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
		defer changefeedbase.TestingSetDefaultFlushFrequency(testSinkFlushFrequency)()
		defer jobs.TestingSetAdoptAndCancelIntervals(10*time.Millisecond, 10*time.Millisecond)()
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

		if region := serverArgsRegion(args); region != "" {
			sqlDB.Exec(t, fmt.Sprintf(`ALTER DATABASE d PRIMARY REGION "%s"`, region))
		}
		sink, cleanup := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(security.RootUser))
		defer cleanup()
		f := cdctest.MakeTableFeedFactory(s, db, flushCh, sink)

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
	return func(t *testing.T) {
		defer changefeedbase.TestingSetDefaultFlushFrequency(testSinkFlushFrequency)()
		defer jobs.TestingSetAdoptAndCancelIntervals(10*time.Millisecond, 10*time.Millisecond)()
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

type fakeKafkaClient struct{}

func (c *fakeKafkaClient) Partitions(topic string) ([]int32, error) {
	return []int32{0}, nil
}

func (c *fakeKafkaClient) RefreshMetadata(topics ...string) error {
	return nil
}

func (c *fakeKafkaClient) Close() error {
	return nil
}

var _ kafkaClient = (*fakeKafkaClient)(nil)

type ignoreCloseProducer struct {
	*asyncProducerMock
}

func (p *ignoreCloseProducer) Close() error {
	return nil
}

// teeGroup facilitates reading messages from input channel
// and sending them to one or more output channels.
type teeGroup struct {
	reader      ctxgroup.Group
	readerExit  chan struct{}
	writers     ctxgroup.Group
	writersExit chan struct{}
}

func newTeeGroup() *teeGroup {
	return &teeGroup{
		reader:      ctxgroup.WithContext(context.Background()),
		readerExit:  make(chan struct{}),
		writers:     ctxgroup.WithContext(context.Background()),
		writersExit: make(chan struct{}),
	}
}

// tee reads incoming messages from input channel and sends them out to one or more output channels.
func (tg *teeGroup) tee(in <-chan *sarama.ProducerMessage, out ...chan<- *sarama.ProducerMessage) {
	pipes := make([]chan *sarama.ProducerMessage, len(out))
	for i := range out {
		pipes[i] = make(chan *sarama.ProducerMessage)
	}

	// One reader to read messages from "in" and write them to each pipe.
	tg.reader.Go(func() error {
		for {
			select {
			case <-tg.readerExit:
				// Signal writers to exit and return.
				for _, pipe := range pipes {
					close(pipe)
				}
				return nil
			case m := <-in:
				for _, pipe := range pipes {
					pipe <- m
				}
			}
		}
	})

	// Add writers for each of the outputs.
	for i := range pipes {
		in, out := pipes[i], out[i]
		tg.writers.Go(func() error {
			var m *sarama.ProducerMessage
			for {
				if m != nil {
					select {
					case <-tg.writersExit:
						return nil
					case out <- m:
						m = nil
					}
				}

				select {
				case <-tg.writersExit:
					return nil
				case m = <-in:
				}
			}
		})
	}
}

// wait shuts down tee group.
func (tg *teeGroup) wait() error {
	// First, close the reader -- these signal writers to exit
	// by closing tee channels.
	close(tg.readerExit)
	producersErr := tg.reader.Wait()
	// Signal and wait for writers.
	close(tg.writersExit)
	consumersErr := tg.writers.Wait()
	return errors.CombineErrors(producersErr, consumersErr)
}

// kafkaFeedState ties together state necessary for synchronization between
// this helper, kafka test feed, and the kafka sink.
// It exists in order to support creating of multiple instances of Feed()s started
// from the same TestFeedFactory (as happens when creating subtests inside cdcTestFn).
type kafkaFeedState struct {
	feedCh chan *sarama.ProducerMessage
	pg     *teeGroup
}

func (fs *kafkaFeedState) wait() error {
	defer close(fs.feedCh)
	return fs.pg.wait()
}

// startProduces starts kafka message production and arranges for synchronization between
// this helper and kafka sink.
func (fs *kafkaFeedState) startProducer() sarama.AsyncProducer {
	// The producer we give to kafka sink ignores close call.
	// This is because normally, kafka sinks owns the producer and so it closes it.
	// But in this case, this test is the producer, and if we let the sink close
	// it, the test may panic while trying to send an acknowledgement to the closed channel.
	producer := &ignoreCloseProducer{newAsyncProducerMock(unbuffered)}

	// TODO(yevgeniy): Support error injection either by acknowledging on the "errors"
	//  channel, or by injecting error message into sarama.ProducerMessage.Metadata.
	fs.pg.tee(producer.inputCh, fs.feedCh, producer.successesCh)

	return producer
}

func kafkaTest(testFn cdcTestFn) func(t *testing.T) {
	return kafkaTestWithServerArgs(nil, testFn)
}

func kafkaTestWithServerArgs(
	argsFn func(args *base.TestServerArgs), testFn cdcTestFn,
) func(*testing.T) {
	return func(t *testing.T) {
		defer changefeedbase.TestingSetDefaultFlushFrequency(testSinkFlushFrequency)()
		defer jobs.TestingSetAdoptAndCancelIntervals(10*time.Millisecond, 10*time.Millisecond)()
		ctx := context.Background()

		// There can be only 1 active feed.  We don't support parallel tests.
		var activeFeed *kafkaFeedState

		createFeed := func() (feedCh chan *sarama.ProducerMessage, cleanup func() error) {
			fs := &kafkaFeedState{
				feedCh: make(chan *sarama.ProducerMessage),
				pg:     newTeeGroup(),
			}
			activeFeed = fs
			return fs.feedCh, fs.wait
		}

		knobs := base.TestingKnobs{DistSQL: &execinfra.TestingKnobs{Changefeed: &TestingKnobs{
			Dial: func(s Sink) {
				kafka := s.(*kafkaSink)
				kafka.client = &fakeKafkaClient{}
				kafka.producer = activeFeed.startProducer()
				kafka.start()
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

		defer func() {
			s.Stopper().Stop(ctx)
		}()

		sqlDB := sqlutils.MakeSQLRunner(db)
		sqlDB.Exec(t, `SET CLUSTER SETTING kv.rangefeed.enabled = true`)
		sqlDB.Exec(t, `SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s'`)
		sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '10ms'`)
		sqlDB.Exec(t, `CREATE DATABASE d`)

		if region := serverArgsRegion(args); region != "" {
			sqlDB.Exec(t, fmt.Sprintf(`ALTER DATABASE d PRIMARY REGION "%s"`, region))
		}

		f := cdctest.MakeKafkaFeedFactory(s, db, createFeed)
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
