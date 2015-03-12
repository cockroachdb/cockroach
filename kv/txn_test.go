// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package kv

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
)

// TestTxnDBBasics verifies that a simple transaction can be run and
// either committed or aborted. On commit, mutations are visible; on
// abort, mutations are never visible. During the txn, verify that
// uncommitted writes cannot be read outside of the txn but can be
// read from inside the txn.
func TestTxnDBBasics(t *testing.T) {
	db, _, _, _, _, transport, err := createTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	value := []byte("value")

	for _, commit := range []bool{true, false} {
		key := []byte(fmt.Sprintf("key-%t", commit))

		// Use snapshot isolation so non-transactional read can always push.
		txnOpts := &client.TransactionOptions{
			Name:      "test",
			Isolation: proto.SNAPSHOT,
		}
		err := db.RunTransaction(txnOpts, func(txn *client.KV) error {
			// Put transactional value.
			if err := txn.Call(proto.Put, proto.PutArgs(key, value), &proto.PutResponse{}); err != nil {
				return err
			}

			// Attempt to read outside of txn.
			gr := &proto.GetResponse{}
			if err := db.Call(proto.Get, proto.GetArgs(key), gr); err != nil {
				return err
			}
			if gr.Value != nil {
				return util.Errorf("expected nil value; got %+v", gr.Value)
			}

			// Read within the transaction.
			if err := txn.Call(proto.Get, proto.GetArgs(key), gr); err != nil {
				return err
			}
			if gr.Value == nil || !bytes.Equal(gr.Value.Bytes, value) {
				return util.Errorf("expected value %q; got %q", value, gr.Value.Bytes)
			}

			if !commit {
				return errors.New("purposefully failing transaction")
			}
			return nil
		})

		if commit != (err == nil) {
			t.Errorf("expected success? %t; got %s", commit, err)
		} else if !commit && err.Error() != "purposefully failing transaction" {
			t.Errorf("unexpected failure with !commit: %s", err)
		}

		// Verify the value is now visible on commit == true, and not visible otherwise.
		gr := &proto.GetResponse{}
		err = db.Call(proto.Get, proto.GetArgs(key), gr)
		if commit {
			if err != nil || gr.Value == nil || !bytes.Equal(gr.Value.Bytes, value) {
				t.Errorf("expected success reading value: %+v, %s", gr.Value, err)
			}
		} else {
			if err != nil || gr.Value != nil {
				t.Errorf("expected success and nil value: %+v, %s", gr.Value, err)
			}
		}
	}
}

// BenchmarkTxnWrites benchmarks a number of transactions writing to the
// same key back to back, without using Prepare/Flush.
func BenchmarkTxnWrites(b *testing.B) {
	db, _, _, mClock, _, transport, err := createTestDB()
	if err != nil {
		b.Fatal(err)
	}
	defer transport.Close()
	key := proto.Key("key")
	txnOpts := &client.TransactionOptions{
		Name: "benchWrite",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mClock.Increment(1)
		if tErr := db.RunTransaction(txnOpts, func(txn *client.KV) error {
			pr := &proto.PutResponse{}
			pa := proto.PutArgs(key, []byte(fmt.Sprintf("value-%d", i)))
			if err := txn.Call(proto.Put, pa, pr); err != nil {
				b.Fatal(err)
			}
			return nil
		}); tErr != nil {
			b.Fatal(err)
		}
	}
}

// verifyUncertainty writes values to a key in 5ns intervals and then launches
// a transaction at each value's timestamp reading that value with
// the maximumOffset given, verifying in the process that the correct values
// are read (usually after one transaction restart).
func verifyUncertainty(concurrency int, maxOffset time.Duration, t *testing.T) {
	db, _, clock, _, lSender, transport, err := createTestDB()
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()

	txnOpts := &client.TransactionOptions{
		Name: "test",
	}

	key := []byte("key-test")
	// wgStart waits for all transactions to line up, wgEnd has the main
	// function wait for them to finish.
	var wgStart, wgEnd sync.WaitGroup
	wgStart.Add(concurrency + 1)
	wgEnd.Add(concurrency)

	// Initial high offset to allow for future writes.
	clock.SetMaxOffset(999 * time.Nanosecond)
	for i := 0; i < concurrency; i++ {
		value := []byte(fmt.Sprintf("value-%d", i))
		// Values will be written with 5ns spacing.
		futureTS := clock.Now().Add(5, 0)
		clock.Update(futureTS)
		// Expected number of versions skipped.
		skipCount := int(maxOffset) / 5
		if i+skipCount >= concurrency {
			skipCount = concurrency - i - 1
		}
		readValue := []byte(fmt.Sprintf("value-%d", i+skipCount))
		pr := proto.PutResponse{}
		db.Call(proto.Put, &proto.PutRequest{
			RequestHeader: proto.RequestHeader{
				Key: key,
			},
			Value: proto.Value{Bytes: value},
		}, &pr)
		if err := pr.GoError(); err != nil {
			t.Errorf("%d: got write error: %v", i, err)
		}
		gr := proto.GetResponse{}
		db.Call(proto.Get, &proto.GetRequest{
			RequestHeader: proto.RequestHeader{
				Key:       key,
				Timestamp: clock.Now(),
			},
		}, &gr)
		if gr.GoError() != nil || gr.Value == nil || !bytes.Equal(gr.Value.Bytes, value) {
			t.Fatalf("%d: expected success reading value %+v: %v", i, gr.Value, gr.GoError())
		}

		go func(i int) {
			defer wgEnd.Done()
			wgStart.Done()
			// Wait until the other goroutines are running.
			wgStart.Wait()

			txnManual := hlc.NewManualClock(futureTS.WallTime)
			txnClock := hlc.NewClock(txnManual.UnixNano)
			// Make sure to incorporate the logical component if the wall time
			// hasn't changed (i=0). The logical component will change
			// internally in a way we can't track, but we want to be just
			// ahead.
			txnClock.Update(futureTS.Add(0, 999))
			// The written values are spaced out in intervals of 5ns, so
			// setting <5ns here should make do without any restarts while
			// higher values require roughly offset/5 restarts.
			txnClock.SetMaxOffset(maxOffset)

			sender := NewTxnCoordSender(lSender, txnClock, false)
			txnDB := client.NewKV(sender, nil)
			txnDB.User = storage.UserRoot

			if err := txnDB.RunTransaction(txnOpts, func(txn *client.KV) error {
				// Read within the transaction.
				gr := proto.GetResponse{}
				txn.Call(proto.Get, &proto.GetRequest{
					RequestHeader: proto.RequestHeader{
						Key:       key,
						Timestamp: futureTS,
					},
				}, &gr)
				if err := gr.GoError(); err != nil {
					if _, ok := gr.GoError().(*proto.ReadWithinUncertaintyIntervalError); ok {
						return err
					}
					return util.Errorf("unexpected read error of type %s: %v", reflect.TypeOf(err), err)
				}
				if gr.Value == nil || gr.Value.Bytes == nil {
					return util.Errorf("no value read")
				}
				if !bytes.Equal(gr.Value.Bytes, readValue) {
					return util.Errorf("%d: read wrong value %q at %v, wanted %q", i, gr.Value.Bytes, futureTS, readValue)
				}
				return nil
			}); err != nil {
				t.Error(err)
			}
		}(i)
	}
	// Kick the goroutines loose.
	wgStart.Done()
	// Wait for the goroutines to finish.
	wgEnd.Wait()
}

// TestTxnDBUncertainty verifies that transactions restart correctly and
// finally read the correct value when encountering writes in the near future.
func TestTxnDBUncertainty(t *testing.T) {
	// Make sure that we notice immediately if any kind of backing off is
	// happening. Restore the previous options after this test is done to avoid
	// interfering with other tests.
	defaultRetryOptions := client.TxnRetryOptions
	defer func() {
		client.TxnRetryOptions = defaultRetryOptions
	}()
	client.TxnRetryOptions.Backoff = 100 * time.Second
	client.TxnRetryOptions.MaxBackoff = 100 * time.Second

	// < 5ns means no uncertainty & no restarts.
	verifyUncertainty(1, 3*time.Nanosecond, t)
	verifyUncertainty(8, 4*time.Nanosecond, t)
	verifyUncertainty(80, 3*time.Nanosecond, t)

	// Below we'll see restarts.

	// Spencer's originally suggested test:
	// Three transactions at t=0, t=5 and t=10 with a MaxOffset of 10ns.
	// They all need to read the latest value for this test to pass, requiring
	// a restart for the first two.
	verifyUncertainty(3, 10*time.Nanosecond, t)
	verifyUncertainty(4, 9*time.Nanosecond, t)

	// Some more, just for kicks.
	verifyUncertainty(7, 12*time.Nanosecond, t)
	verifyUncertainty(100, 10*time.Nanosecond, t)
}

// TestUncertaintyRestarts verifies that transactional reads within the
// uncertainty interval cause exactly one restart. The test runs a transaction
// which attempts to read a single key, but just before that read, a future
// version of that key is written directly through the MVCC layer.

// Indirectly this tests that the transaction remembers the NodeID of the node
// being read from correctly, at least in this simple case. Not remembering the
// node would lead to thousands of transaction restarts and almost certainly a
// test timeout.
func TestUncertaintyRestarts(t *testing.T) {
	{
		db, eng, clock, mClock, _, transport, err := createTestDB()
		if err != nil {
			t.Fatal(err)
		}
		defer transport.Close()
		// Set a large offset so that a busy restart-loop
		// really shows. Also makes sure that the values
		// we write in the future below don't actually
		// wind up in the past.
		offset := 4000 * time.Millisecond
		clock.SetMaxOffset(offset)
		key := proto.Key("key")
		value := proto.Value{
			Bytes: nil, // Set for each Put
		}
		// With the correct restart behaviour, we see only one restart
		// and the value read is the very first one (as nothing else
		// has been written)
		wantedBytes := []byte("value-0")

		txnOpts := &client.TransactionOptions{
			Name: "uncertainty",
		}
		gr := &proto.GetResponse{}
		i := -1
		tErr := db.RunTransaction(txnOpts, func(txn *client.KV) error {
			i++
			mClock.Increment(1)
			futureTS := clock.Now()
			futureTS.WallTime++
			value.Bytes = []byte(fmt.Sprintf("value-%d", i))
			err = engine.MVCCPut(eng, nil, key, futureTS, value, nil)
			if err != nil {
				t.Fatal(err)
			}
			gr.Reset()
			if err := txn.Call(proto.Get, proto.GetArgs(key), gr); err != nil {
				return err
			}
			if gr.Value == nil || !bytes.Equal(gr.Value.Bytes, wantedBytes) {
				t.Fatalf("%d: read wrong value: %v, wanted %q", i,
					gr.Value, wantedBytes)
			}
			return nil
		})
		if i != 1 {
			t.Errorf("txn restarted %d times, expected only one restart", i)
		}
		if tErr != nil {
			t.Fatal(tErr)
		}
	}
}

// TestUncertaintyMaxTimestampForwarding checks that we correctly read from
// hosts which for which we control the uncertainty by checking that when a
// transaction restarts after an uncertain read, it will also take into account
// the target node's clock at the time of the failed read when forwarding the
// read timestamp.
// This is a prerequisite for being able to prevent further uncertainty
// restarts for that node and transaction without sacrificing correctness.
// See proto.Transaction.CertainNodes for details.
func TestUncertaintyMaxTimestampForwarding(t *testing.T) {
	db, eng, clock, mClock, _, transport, err := createTestDB()
	defer transport.Close()
	// Large offset so that any value in the future is an uncertain read.
	// Also makes sure that the values we write in the future below don't
	// actually wind up in the past.
	clock.SetMaxOffset(50000 * time.Millisecond)

	txnOpts := &client.TransactionOptions{
		Name: "uncertainty",
	}

	offsetNS := int64(100)
	keySlow := proto.Key("slow")
	keyFast := proto.Key("fast")
	valSlow := []byte("wols")
	valFast := []byte("tsaf")

	// Write keySlow at now+offset, keyFast at now+2*offset
	futureTS := clock.Now()
	futureTS.WallTime += offsetNS
	err = engine.MVCCPut(eng, nil, keySlow, futureTS,
		proto.Value{Bytes: valSlow}, nil)
	if err != nil {
		t.Fatal(err)
	}
	futureTS.WallTime += offsetNS
	err = engine.MVCCPut(eng, nil, keyFast, futureTS,
		proto.Value{Bytes: valFast}, nil)
	if err != nil {
		t.Fatal(err)
	}

	i := 0
	if tErr := db.RunTransaction(txnOpts, func(txn *client.KV) error {
		i++
		// The first command serves to start a Txn, fixing the timestamps.
		// There will be a restart, but this is idempotent.
		sr := &proto.ScanResponse{}
		if err = txn.Call(proto.Scan, proto.ScanArgs(proto.Key("t"), proto.Key("t"),
			0), sr); err != nil {
			t.Fatal(err)
		}

		// The server's clock suddenly jumps ahead of keyFast's timestamp.
		// There will be a restart, but this is idempotent.
		mClock.Set(2*offsetNS + 1)

		// Now read slowKey first. It should read at 0, catch an uncertainty error,
		// and get keySlow's timestamp in that error, but upgrade it to the larger
		// node clock (which is ahead of keyFast as well). If the last part does
		// not happen, the read of keyFast should fail (i.e. read nothing).
		// There will be exactly one restart here.
		gr := &proto.GetResponse{}
		if err = txn.Call(proto.Get, proto.GetArgs(keySlow), gr); err != nil {
			if i != 1 {
				t.Errorf("unexpected transaction error: %v", err)
			}
			return err
		}
		if gr.Value == nil || !bytes.Equal(gr.Value.Bytes, valSlow) {
			t.Errorf("read of %q returned %v, wanted value %q", keySlow, gr.Value,
				valSlow)
		}

		gr.Reset()
		// The node should already be certain, so we expect no restart here
		// and to read the correct key.
		if err = txn.Call(proto.Get, proto.GetArgs(keyFast), gr); err != nil {
			t.Errorf("second Get failed with %v", err)
		}
		if gr.Value == nil || !bytes.Equal(gr.Value.Bytes, valFast) {
			t.Errorf("read of %q returned %v, wanted value %q", keyFast, gr.Value,
				valFast)
		}
		return nil
	}); tErr != nil {
		t.Fatal(tErr)
	}
}
