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
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
)

// TestTxnDBBasics verifies that a simple transaction can be run and
// either committed or aborted. On commit, mutations are visible; on
// abort, mutations are never visible. During the txn, verify that
// uncommitted writes cannot be read outside of the txn but can be
// read from inside the txn.
func TestTxnDBBasics(t *testing.T) {
	db, _, _, _ := createTestDB(t)
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

func addTS(ts proto.Timestamp, d time.Duration) proto.Timestamp {
	return proto.Timestamp{
		WallTime: ts.WallTime + d.Nanoseconds(),
		Logical:  ts.Logical + 1,
	}
}

// verifyUncertainty writes values to a key in 5ns intervals and then launches
// a transaction at each value's timestamp reading that value with
// the maximumOffset given, verifying in the process that the correct values
// are read (usually after one transaction restart).
func verifyUncertainty(concurrency int, maxOffset time.Duration, t *testing.T) {
	db, clock, manualClock := createTestDB(t)
	*manualClock = 0

	txnOpts := &storage.TransactionOptions{
		Name:         "test",
		User:         storage.UserRoot,
		UserPriority: 1,
		Retry: &util.RetryOptions{
			// Nothing should backoff here.
			Backoff:     10 * time.Second,
			MaxBackoff:  10 * time.Second,
			Constant:    2,
			MaxAttempts: 10,
		},
	}

	key := []byte("key-test")
	var wgI, wgO sync.WaitGroup
	wgO.Add(concurrency)
	wgI.Add(concurrency + 1)

	// Initial high offset to allow for future writes.
	clock.SetMaxOffset(999 * time.Nanosecond)
	for i := 0; i < concurrency; i++ {
		value := []byte(fmt.Sprintf("value-%d", i))
		// Values will be written with 5ns spacing.
		futureTS := addTS(clock.Now(), 5*time.Nanosecond)
		// Expected number of versions skipped.
		skipCount := int(maxOffset) / 5 * (1 - (i+1)/concurrency)
		if i+skipCount >= concurrency {
			skipCount = concurrency - i - 1
		}
		readValue := []byte(fmt.Sprintf("value-%d", i+skipCount))
		if err := (<-db.Put(&proto.PutRequest{
			RequestHeader: proto.RequestHeader{
				Key:       key,
				Timestamp: futureTS,
			},
			Value: proto.Value{Bytes: value},
		})).GoError(); err != nil {
			t.Errorf("%d: got write error: %v", i, err)
		}
		if gr := <-db.Get(&proto.GetRequest{
			RequestHeader: proto.RequestHeader{
				Key:       key,
				Timestamp: futureTS,
			},
		}); gr.GoError() != nil || gr.Value == nil || !bytes.Equal(gr.Value.Bytes, value) {
			t.Fatalf("%d: expected success reading value %+v: %v", i, gr.Value, gr.GoError())
		}

		go func(i int) {
			defer wgO.Done()
			wgI.Wait()
			txnManual := hlc.ManualClock(futureTS.WallTime)
			txnClock := hlc.NewClock(txnManual.UnixNano)
			// Make sure to incorporate the logical component if the wall time
			// hasn't changed (i=0).
			txnClock.Update(futureTS)
			// The written values are spaced out in intervals of 5ns, so
			// setting <5ns here should make do without any restarts while
			// higher values require roughly offset/5 restarts.
			txnClock.SetMaxOffset(maxOffset)

			if err := runTransaction(NewDB(db.kv, txnClock), txnOpts, func(txn storage.DB) error {
				// Read within the transaction.
				gr := <-txn.Get(&proto.GetRequest{
					RequestHeader: proto.RequestHeader{
						Key:       key,
						Timestamp: futureTS,
					},
				})
				if err := gr.GoError(); err != nil {
					if _, ok := gr.GoError().(*proto.ReadWithinUncertaintyIntervalError); ok {
						return err
					}
					return util.Errorf("unexpected read error of type %s: %v", reflect.TypeOf(err), err)
				}
				// TODO: figure out correct values and compare them.
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
		wgI.Done()
	}
	// Kick the goroutines loose.
	wgI.Done()
	// Wait for the goroutines to finish.
	wgO.Wait()
}

// TestTxnDBUncertainty verifies that transactions restart correctly and
// finally read the correct value when encountering writes in the near future.
func TestTxnDBUncertainty(t *testing.T) {
	// < 5ns means no uncertainty & no restarts.
	// Those run very fast since no restarts are required.
	verifyUncertainty(1, 3*time.Nanosecond, t)
	verifyUncertainty(8, 4*time.Nanosecond, t)
	verifyUncertainty(80, 3*time.Nanosecond, t)
	// Below we'll see restarts.
	// TODO(Spencer): This is awfully slow for a single restart (even with the
	// first parameter set to 2). Anything to be done?
	verifyUncertainty(7, 12*time.Nanosecond, t)
}
