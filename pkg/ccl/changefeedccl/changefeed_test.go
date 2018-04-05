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
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

func init() {
	testProducersHook = make(map[string]sarama.SyncProducer)
}

func TestChangefeed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer utilccl.TestingEnableEnterprise()()

	const testPollingInterval = 10 * time.Millisecond
	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = testPollingInterval

	ctx := context.Background()
	s, sqlDBRaw, _ := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase: "d",
	})
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(sqlDBRaw)

	k := newTestKafkaProducer()
	testProducersHook[t.Name()] = k
	sqlDB.Exec(t, `SET CLUSTER SETTING external.kafka.bootstrap_servers = $1`, t.Name())
	sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = $1`, testPollingInterval.String())

	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b'), (4, 'c'), (7, 'd'), (8, 'e')`)

	var jobID int
	sqlDB.QueryRow(t, `CREATE EXPERIMENTAL_CHANGEFEED EMIT foo TO KAFKA WITH topic_prefix=$1`, `bar_`).Scan(&jobID)

	<-k.flushCh
	assertPayloads(t, k.Messages(), []string{
		`{"a": 1, "b": "a"}`,
		`{"a": 2, "b": "b"}`,
		`{"a": 4, "b": "c"}`,
		`{"a": 7, "b": "d"}`,
		`{"a": 8, "b": "e"}`,
	})
	<-k.flushCh

	// PAUSE JOB is asynchronous, so wait out a few polling intervals for it to
	// notice the pause state and shut down. Then make sure that changedCh is
	// cleared.
	sqlDB.Exec(t, `PAUSE JOB $1`, jobID)
	time.Sleep(10 * testPollingInterval)
	select {
	case <-k.flushCh:
	default:
	}

	// Nothing should happen if the job is paused.
	sqlDB.Exec(t, `INSERT INTO foo VALUES (16, 'f')`)
	time.Sleep(10 * testPollingInterval)
	assertPayloads(t, k.Messages(), nil)

	sqlDB.Exec(t, `RESUME JOB $1`, jobID)
	<-k.flushCh
	assertPayloads(t, k.Messages(), []string{
		`{"a": 16, "b": "f"}`,
	})
}

// testKafkaProducer is an implementation of sarama.SyncProducer used for
// testing.
type testKafkaProducer struct {
	mu struct {
		syncutil.Mutex
		msgs []*sarama.ProducerMessage
	}
	flushCh chan struct{}
}

func newTestKafkaProducer() *testKafkaProducer {
	return &testKafkaProducer{flushCh: make(chan struct{}, 1)}
}

// SendMessage implements the KafkaProducer interface.
func (k *testKafkaProducer) SendMessage(
	msg *sarama.ProducerMessage,
) (partition int32, offset int64, err error) {
	k.mu.Lock()
	k.mu.msgs = append(k.mu.msgs, msg)
	k.mu.Unlock()
	return 0, 0, nil
}

// SendMessages implements the KafkaProducer interface.
func (k *testKafkaProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
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
	return nil
}

func (k *testKafkaProducer) Messages() []*sarama.ProducerMessage {
	k.mu.Lock()
	msgs := append([]*sarama.ProducerMessage(nil), k.mu.msgs...)
	k.mu.msgs = k.mu.msgs[:0]
	k.mu.Unlock()
	return msgs
}

func assertPayloads(t testing.TB, messages []*sarama.ProducerMessage, expected []string) {
	t.Helper()
	var actual []string
	for _, m := range messages {
		value, err := m.Value.Encode()
		if err != nil {
			t.Fatal(err)
		}
		actual = append(actual, string(value))
	}
	sort.Strings(actual)
	sort.Strings(expected)
	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected\n  %s\ngot\n  %s",
			strings.Join(expected, "\n  "), strings.Join(actual, "\n  "))
	}
}
