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
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/bufalloc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
)

func init() {
	testKafkaProducersHook = make(map[string]sarama.SyncProducer)
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
	testKafkaProducersHook[t.Name()] = k
	sqlDB.Exec(t, `SET CLUSTER SETTING changefeed.experimental_poll_interval = '0ns'`)

	sqlDB.Exec(t, `CREATE DATABASE d`)
	sqlDB.Exec(t, `CREATE TABLE foo (a INT PRIMARY KEY, b STRING)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1, 'a'), (2, 'b'), (4, 'c'), (7, 'd'), (8, 'e')`)

	var jobID int
	sqlDB.QueryRow(t, `CREATE CHANGEFEED FOR foo INTO $1`, `kafka://`+t.Name()).Scan(&jobID)

	k.assertPayloads(t, []string{
		`foo: [1]->{"a": 1, "b": "a"}`,
		`foo: [2]->{"a": 2, "b": "b"}`,
		`foo: [4]->{"a": 4, "b": "c"}`,
		`foo: [7]->{"a": 7, "b": "d"}`,
		`foo: [8]->{"a": 8, "b": "e"}`,
	})

	// TODO(dan): To ensure the test restarts from after this point (so the
	// below assertion doesn't flake), we need to wait for the highwater mark on
	// the job to be updated after the initial scan. Once we support emitting
	// resolved timestamps, we can just wait for one of those here, but in the
	// meantime, introspect the jobs table.
	testutils.SucceedsSoon(t, func() error {
		var progressBytes []byte
		if err := sqlDB.DB.QueryRow(
			`SELECT progress FROM system.jobs WHERE id = $1`, jobID,
		).Scan(&progressBytes); err != nil {
			log.Info(ctx, err)
			return err
		}
		var progress jobspb.Progress
		if err := protoutil.Unmarshal(progressBytes, &progress); err != nil {
			return err
		}
		if progress.GetChangefeed().Highwater == (hlc.Timestamp{}) {
			return errors.New(`waiting for initial scan to finish`)
		}
		return nil
	})

	// PAUSE JOB is asynchronous, so wait for it to notice the pause state and shut
	// down.
	sqlDB.Exec(t, `PAUSE JOB $1`, jobID)
	testutils.SucceedsSoon(t, func() error {
		k.mu.Lock()
		closed := k.mu.closed
		k.mu.Unlock()
		if !closed {
			return errors.New(`waiting for job to shut down the changefeed flow`)
		}
		return nil
	})

	// Nothing should happen if the job is paused.
	sqlDB.Exec(t, `INSERT INTO foo VALUES (16, 'f')`)
	const numPausedChecks = 5
	for i := 0; i < numPausedChecks; i++ {
		if m := k.Message(); m != nil {
			t.Fatalf(`expected no messages got %v`, m)
		}
	}

	k.Reset()
	sqlDB.Exec(t, `RESUME JOB $1`, jobID)
	k.assertPayloads(t, []string{
		`foo: [16]->{"a": 16, "b": "f"}`,
	})
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
}

func newTestKafkaProducer() *testKafkaProducer {
	return &testKafkaProducer{}
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

func (k *testKafkaProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	for _, msg := range msgs {
		if _, _, err := k.SendMessage(msg); err != nil {
			return err
		}
	}
	return nil
}

// Close implements the KafkaProducer interface.
func (k *testKafkaProducer) Close() error {
	k.mu.Lock()
	k.mu.closed = true
	k.mu.Unlock()
	return nil
}

func (k *testKafkaProducer) Message() *sarama.ProducerMessage {
	k.mu.Lock()
	var msg *sarama.ProducerMessage
	if len(k.mu.msgs) > 0 {
		msg = k.mu.msgs[0]
		k.mu.msgs = k.mu.msgs[1:]
	}
	k.mu.Unlock()
	return msg
}

func (k *testKafkaProducer) assertPayloads(t *testing.T, expected []string) {
	t.Helper()

	var actual []string
	for r := retry.Start(retry.Options{}); len(actual) < len(expected) && r.Next(); {
		if m := k.Message(); m != nil {
			key, err := m.Key.Encode()
			if err != nil {
				t.Fatal(err)
			}
			value, err := m.Value.Encode()
			if err != nil {
				t.Fatal(err)
			}
			actual = append(actual, fmt.Sprintf(`%s: %s->%s`, m.Topic, key, value))
		}
	}

	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("expected\n  %s\ngot\n  %s",
			strings.Join(expected, "\n  "), strings.Join(actual, "\n  "))
	}
}
