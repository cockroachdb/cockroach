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
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
)

func init() {
	testProducersHook = make(map[string]sarama.SyncProducer)
}

func TestChangefeedBasics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer utilccl.TestingEnableEnterprise()()

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: "d"})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)

	k := newTestKafkaProducer()
	testProducersHook[t.Name()] = k
	sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '0ns'`)

	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

	sqlDB.Exec(t, `CREATE CHANGEFEED FOR foo INTO $1`, `kafka://`+t.Name())

	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b')`)
	assertPayloads(t, k.WaitUntilNewMessages(), []string{
		`[1]->{"a": 1, "b": "a"}`,
		`[2]->{"a": 2, "b": "b"}`,
	})

	sqlDB.Exec(t, `UPSERT INTO foo VALUES (2, 'c'), (3, 'd')`)
	assertPayloads(t, k.WaitUntilNewMessages(), []string{
		`[2]->{"a": 2, "b": "c"}`,
		`[3]->{"a": 3, "b": "d"}`,
	})

	sqlDB.Exec(t, `DELETE FROM foo WHERE a = 1`)
	assertPayloads(t, k.WaitUntilNewMessages(), []string{
		`[1]->`,
	})
}

func TestChangefeedEnvelope(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer utilccl.TestingEnableEnterprise()()

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: "d"})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)

	sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '0ns'`)
	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a')`)

	testHost := t.Name()
	t.Run(`envelope=row`, func(t *testing.T) {
		k := newTestKafkaProducer()
		testProducersHook[testHost+`_row`] = k
		sqlDB.Exec(t,
			`CREATE CHANGEFEED FOR DATABASE d INTO $1 WITH envelope='row'`,
			`kafka://`+testHost+`_row`)
		assertPayloads(t, k.WaitUntilNewMessages(), []string{`[1]->{"a": 1, "b": "a"}`})
	})
	t.Run(`envelope=key_only`, func(t *testing.T) {
		k := newTestKafkaProducer()
		testProducersHook[testHost+`_key_only`] = k
		sqlDB.Exec(t,
			`CREATE CHANGEFEED FOR DATABASE d INTO $1 WITH envelope='key_only'`,
			`kafka://`+testHost+`_key_only`)
		assertPayloads(t, k.WaitUntilNewMessages(), []string{`[1]->`})
	})
}

func TestChangefeedMultiTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer utilccl.TestingEnableEnterprise()()

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: "d"})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)

	k := newTestKafkaProducer()
	testProducersHook[t.Name()] = k
	sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '0ns'`)

	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a')`)
	sqlDB.Exec(t, `CREATE TABLE bar (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `INSERT INTO bar VALUES (2, 'b')`)

	sqlDB.Exec(t, `CREATE CHANGEFEED FOR DATABASE d INTO $1`, `kafka://`+t.Name())

	assertPayloads(t, k.WaitUntilNewMessages(), []string{
		`[1]->{"a": 1, "b": "a"}`,
		`[2]->{"a": 2, "b": "b"}`,
	})
}

func TestChangefeedAsOfSystemTime(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer utilccl.TestingEnableEnterprise()()

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: "d"})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)

	k := newTestKafkaProducer()
	testProducersHook[t.Name()] = k
	sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '0ns'`)

	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'before')`)
	var ts string
	sqlDB.QueryRow(t, `SELECT cluster_logical_timestamp()`).Scan(&ts)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (2, 'after')`)

	sqlDB.Exec(t, `CREATE CHANGEFEED FOR foo INTO $1 WITH cursor=$2`, `kafka://`+t.Name(), ts)

	assertPayloads(t, k.WaitUntilNewMessages(), []string{
		`[2]->{"a": 2, "b": "after"}`,
	})
}

func TestChangefeedPauseUnpause(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer utilccl.TestingEnableEnterprise()()

	defer func(prev time.Duration) { jobs.DefaultAdoptInterval = prev }(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 10 * time.Millisecond

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase: "d",
	})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)

	k := newTestKafkaProducer()
	testProducersHook[t.Name()] = k
	sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '0ns'`)

	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b'), (4, 'c'), (7, 'd'), (8, 'e')`)

	var jobID int
	sqlDB.QueryRow(t, `CREATE CHANGEFEED FOR foo INTO $1`, `kafka://`+t.Name()).Scan(&jobID)

	<-k.flushCh
	assertPayloads(t, k.WaitUntilNewMessages(), []string{
		`[1]->{"a": 1, "b": "a"}`,
		`[2]->{"a": 2, "b": "b"}`,
		`[4]->{"a": 4, "b": "c"}`,
		`[7]->{"a": 7, "b": "d"}`,
		`[8]->{"a": 8, "b": "e"}`,
	})

	// PAUSE JOB is asynchronous, so wait out a few polling intervals for it to
	// notice the pause state and shut down.
	sqlDB.Exec(t, `PAUSE JOB $1`, jobID)
	time.Sleep(10 * time.Millisecond)

	// Nothing should happen if the job is paused.
	sqlDB.Exec(t, `INSERT INTO foo VALUES (16, 'f')`)
	time.Sleep(10 * time.Millisecond)
	assertPayloads(t, k.Messages(), nil)

	k.Reset()
	sqlDB.Exec(t, `RESUME JOB $1`, jobID)
	assertPayloads(t, k.WaitUntilNewMessages(), []string{
		`[16]->{"a": 16, "b": "f"}`,
	})
}

func TestChangefeedSchemaChange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer utilccl.TestingEnableEnterprise()()

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: "d"})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)

	k := newTestKafkaProducer()
	testProducersHook[t.Name()] = k
	sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '0ns'`)

	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY)`)
	sqlDB.Exec(t, `CREATE CHANGEFEED FOR foo INTO $1`, `kafka://`+t.Name())

	sqlDB.Exec(t, `INSERT INTO foo (a) VALUES (1)`)
	sqlDB.Exec(t, `ALTER TABLE foo ADD COLUMN b INT`)
	sqlDB.Exec(t, `INSERT INTO foo (a) VALUES (2)`)
	sqlDB.Exec(t, `INSERT INTO foo (a, b) VALUES (3, 4)`)
	time.Sleep(100 * time.Millisecond)
	assertPayloads(t, k.WaitUntilNewMessages(), []string{
		`[1]->{"a": 1}`,
		`[2]->{"a": 2, "b": null}`,
		`[3]->{"a": 3, "b": 4}`,
	})

	// TODO(dan): Test a schema change that uses a backfill once we figure out
	// the user facing semantics of that.
}

func TestChangefeedErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer utilccl.TestingEnableEnterprise()()

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{UseDatabase: "d"})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)

	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)

	if _, err := sqlDB.DB.Exec(
		`CREATE CHANGEFEED FOR foo INTO $1`, `kafka://nope`,
	); !testutils.IsError(err, `no test producer: nope`) {
		t.Fatalf(`expected 'no test producer: nope' error got: %+v`, err)
	}
}

// testKafkaProducer is an implementation of sarama.SyncProducer used for
// testing.
type testKafkaProducer struct {
	mu struct {
		syncutil.Mutex
		msgs   []*sarama.ProducerMessage
		closed bool
	}
	scratch bufalloc.ByteAllocator
	flushCh chan struct{}
}

func newTestKafkaProducer() *testKafkaProducer {
	return &testKafkaProducer{flushCh: make(chan struct{}, 1)}
}

func (k *testKafkaProducer) Reset() {
	k.scratch = k.scratch[:0]
	k.mu.Lock()
	k.mu.closed = false
	k.mu.Unlock()
}

// SendMessage implements the KafkaProducer interface.
func (k *testKafkaProducer) SendMessage(
	msg *sarama.ProducerMessage,
) (partition int32, offset int64, err error) {
	key, err := msg.Key.Encode()
	if err != nil {
		return 0, 0, err
	}
	k.scratch, key = k.scratch.Copy(key, 0 /* extraCap */)
	msg.Key = sarama.ByteEncoder(key)
	value, err := msg.Value.Encode()
	if err != nil {
		return 0, 0, err
	}
	k.scratch, value = k.scratch.Copy(value, 0 /* extraCap */)
	msg.Value = sarama.ByteEncoder(value)

	k.mu.Lock()
	k.mu.msgs = append(k.mu.msgs, msg)
	closed := k.mu.closed
	k.mu.Unlock()
	if closed {
		return 0, 0, errors.New(`cannot send to closed producer`)
	}
	return 0, 0, nil
}

// SendMessages implements the KafkaProducer interface.
func (k *testKafkaProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	k.mu.Lock()
	msgLen := len(k.mu.msgs)
	closed := k.mu.closed
	k.mu.Unlock()
	if closed {
		return errors.New(`cannot send to closed producer`)
	}
	if msgLen == 0 {
		// Make sure that WaitUntilNewMessages (which wakes up on the fluchCh
		// trigger) always gets at least one full scan. Without this check,
		// there is a race where the changefeed finishes a poll, triggers this,
		// then some data is written and the changefeed is in the middle of
		// handing them all to SendMessage when WaitUntilNewMessages wakes up
		// and sees a partial poll.
		//
		// TODO(dan): It's become quite clear that the tests are brittle when
		// they make assumptions about this underlying implementation of a
		// periodic, consistent incremental scan. This assumption will also no
		// longer hold once we switch to a push-based implementation. Rework the
		// tests then.
		return nil
	}
	if msgs == nil {
		select {
		case k.flushCh <- struct{}{}:
		default:
			// flushCh has already been notified, we don't need to do it again.
		}
		return nil
	}
	panic("unimplemented")
}

// Close implements the KafkaProducer interface.
func (k *testKafkaProducer) Close() error {
	k.mu.Lock()
	k.mu.closed = true
	k.mu.Unlock()
	return nil
}

func (k *testKafkaProducer) Messages() []*sarama.ProducerMessage {
	k.mu.Lock()
	msgs := append([]*sarama.ProducerMessage(nil), k.mu.msgs...)
	k.mu.msgs = k.mu.msgs[:0]
	k.mu.Unlock()
	return msgs
}

// WaitUntilNewMessages waits until one full poll has finished (every new
// message has been added) and there is at least one message waiting to be read.
// Then, all messages waiting to be read are consumed and returned.
func (k *testKafkaProducer) WaitUntilNewMessages() []*sarama.ProducerMessage {
	for {
		<-k.flushCh
		if msgs := k.Messages(); len(msgs) > 0 {
			return msgs
		}
	}
}

func assertPayloads(t testing.TB, messages []*sarama.ProducerMessage, expected []string) {
	t.Helper()
	var actual []string
	for _, m := range messages {
		key, err := m.Key.Encode()
		if err != nil {
			t.Fatal(err)
		}
		value, err := m.Value.Encode()
		if err != nil {
			t.Fatal(err)
		}
		actual = append(actual, string(key)+`->`+string(value))
	}
	sort.Strings(actual)
	sort.Strings(expected)
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected\n  %s\ngot\n  %s",
			strings.Join(expected, "\n  "), strings.Join(actual, "\n  "))
	}
}
