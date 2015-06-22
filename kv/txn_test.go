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
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// TestTxnDBBasics verifies that a simple transaction can be run and
// either committed or aborted. On commit, mutations are visible; on
// abort, mutations are never visible. During the txn, verify that
// uncommitted writes cannot be read outside of the txn but can be
// read from inside the txn.
func TestTxnDBBasics(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := createTestDB(t)
	defer s.Stop()
	value := []byte("value")

	for _, commit := range []bool{true, false} {
		key := []byte(fmt.Sprintf("key-%t", commit))

		err := s.DB.Txn(func(txn *client.Txn) error {
			// Use snapshot isolation so non-transactional read can always push.
			txn.SetSnapshotIsolation()

			// Put transactional value.
			if err := txn.Put(key, value); err != nil {
				return err
			}

			// Attempt to read outside of txn.
			if gr, err := s.DB.Get(key); err != nil {
				return err
			} else if gr.Exists() {
				return util.Errorf("expected nil value; got %v", gr.Value)
			}

			// Read within the transaction.
			if gr, err := txn.Get(key); err != nil {
				return err
			} else if !gr.Exists() || !bytes.Equal(gr.ValueBytes(), value) {
				return util.Errorf("expected value %q; got %q", value, gr.Value)
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
		gr, err := s.DB.Get(key)
		if commit {
			if err != nil || !gr.Exists() || !bytes.Equal(gr.ValueBytes(), value) {
				t.Errorf("expected success reading value: %+v, %s", gr.ValueBytes(), err)
			}
		} else {
			if err != nil || gr.Exists() {
				t.Errorf("expected success and nil value: %s, %s", gr, err)
			}
		}
	}
}

// BenchmarkTxnWrites benchmarks a number of transactions writing to the
// same key back to back, without using Prepare/Flush.
func BenchmarkTxnWrites(b *testing.B) {
	s := createTestDB(b)
	defer s.Stop()
	key := proto.Key("key")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Manual.Increment(1)
		if tErr := s.DB.Txn(func(txn *client.Txn) error {
			b := &client.Batch{}
			b.Put(key, fmt.Sprintf("value-%d", i))
			return txn.Commit(b)
		}); tErr != nil {
			b.Fatal(tErr)
		}
	}
}

// verifyUncertainty writes values to a key in 5ns intervals and then launches
// a transaction at each value's timestamp reading that value with
// the maximumOffset given, verifying in the process that the correct values
// are read (usually after one transaction restart).
func verifyUncertainty(concurrency int, maxOffset time.Duration, t *testing.T) {
	s := createTestDB(t)
	defer s.Stop()

	key := []byte("key-test")
	// wgStart waits for all transactions to line up, wgEnd has the main
	// function wait for them to finish.
	var wgStart, wgEnd sync.WaitGroup
	wgStart.Add(concurrency + 1)
	wgEnd.Add(concurrency)

	// Initial high offset to allow for future writes.
	s.Clock.SetMaxOffset(999 * time.Nanosecond)
	s.Manual.Set(s.Clock.MaxOffset().Nanoseconds() + 1)
	for i := 0; i < concurrency; i++ {
		value := []byte(fmt.Sprintf("value-%d", i))
		// Values will be written with 5ns spacing.
		futureTS := s.Clock.Now().Add(5, 0)
		s.Clock.Update(futureTS)
		// Expected number of versions skipped.
		skipCount := int(maxOffset) / 5
		if i+skipCount >= concurrency {
			skipCount = concurrency - i - 1
		}
		readValue := []byte(fmt.Sprintf("value-%d", i+skipCount))
		if err := s.DB.Put(key, value); err != nil {
			t.Errorf("%d: got write error: %s", i, err)
		}
		if gr, err := s.DB.Get(key); err != nil {
			t.Fatalf("%d: expected success reading value: %s", i, err)
		} else if !gr.Exists() || !bytes.Equal(gr.ValueBytes(), value) {
			t.Fatalf("%d: expected success reading value: %v", i, gr.Value)
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

			sender := NewTxnCoordSender(s.lSender, txnClock, false, s.Stopper)
			txnDB, err := client.Open("//root@", client.SenderOpt(sender))
			if err != nil {
				t.Fatal(err)
			}

			if err := txnDB.Txn(func(txn *client.Txn) error {
				// Read within the transaction.
				gr, err := txn.Get(key)
				if err != nil {
					if _, ok := err.(*proto.ReadWithinUncertaintyIntervalError); ok {
						return err
					}
					return util.Errorf("unexpected read error of type %s: %s", reflect.TypeOf(err), err)
				}
				if !gr.Exists() {
					return util.Errorf("no value read")
				}
				if !bytes.Equal(gr.ValueBytes(), readValue) {
					return util.Errorf("%d: read wrong value %v at %s, wanted %q",
						i, gr.Value, futureTS, readValue)
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
	defer leaktest.AfterTest(t)
	// Make sure that we notice immediately if any kind of backing off is
	// happening. Restore the previous options after this test is done to avoid
	// interfering with other tests.
	defaultRetryOptions := client.DefaultTxnRetryOptions
	defer func() {
		client.DefaultTxnRetryOptions = defaultRetryOptions
	}()
	client.DefaultTxnRetryOptions.Backoff = 100 * time.Second
	client.DefaultTxnRetryOptions.MaxBackoff = 100 * time.Second

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
	defer leaktest.AfterTest(t)
	s := createTestDB(t)
	defer s.Stop()
	// Set a large offset so that a busy restart-loop
	// really shows. Also makes sure that the values
	// we write in the future below don't actually
	// wind up in the past.
	offset := 4000 * time.Millisecond
	s.Clock.SetMaxOffset(offset)
	key := proto.Key("key")
	value := proto.Value{
		Bytes: nil, // Set for each Put
	}
	// With the correct restart behaviour, we see only one restart
	// and the value read is the very first one (as nothing else
	// has been written)
	wantedBytes := []byte("value-0")

	i := -1
	tErr := s.DB.Txn(func(txn *client.Txn) error {
		i++
		s.Manual.Increment(1)
		futureTS := s.Clock.Now()
		futureTS.WallTime++
		value.Bytes = []byte(fmt.Sprintf("value-%d", i))
		if err := engine.MVCCPut(s.Eng, nil, key, futureTS, value, nil); err != nil {
			t.Fatal(err)
		}
		gr, err := txn.Get(key)
		if err != nil {
			return err
		}
		if !gr.Exists() || !bytes.Equal(gr.ValueBytes(), wantedBytes) {
			t.Fatalf("%d: read wrong value: %v, wanted %q", i, gr.Value, wantedBytes)
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

// TestUncertaintyMaxTimestampForwarding checks that we correctly read
// from hosts for which we control the uncertainty by checking that
// when a transaction restarts after an uncertain read, it will also
// take into account the target node's clock at the time of the failed
// read when forwarding the read timestamp.
//
// This is a prerequisite for being able to prevent further uncertainty
// restarts for that node and transaction without sacrificing correctness.
// See proto.Transaction.CertainNodes for details.
func TestUncertaintyMaxTimestampForwarding(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := createTestDB(t)
	defer s.Stop()
	// Large offset so that any value in the future is an uncertain read.
	// Also makes sure that the values we write in the future below don't
	// actually wind up in the past.
	s.Clock.SetMaxOffset(50 * time.Second)

	offsetNS := int64(100)
	keySlow := proto.Key("slow")
	keyFast := proto.Key("fast")
	valSlow := []byte("wols")
	valFast := []byte("tsaf")

	// Write keySlow at now+offset, keyFast at now+2*offset
	futureTS := s.Clock.Now()
	futureTS.WallTime += offsetNS
	if err := engine.MVCCPut(s.Eng, nil, keySlow, futureTS, proto.Value{Bytes: valSlow}, nil); err != nil {
		t.Fatal(err)
	}
	futureTS.WallTime += offsetNS
	if err := engine.MVCCPut(s.Eng, nil, keyFast, futureTS, proto.Value{Bytes: valFast}, nil); err != nil {
		t.Fatal(err)
	}

	i := 0
	if tErr := s.DB.Txn(func(txn *client.Txn) error {
		i++
		// The first command serves to start a Txn, fixing the timestamps.
		// There will be a restart, but this is idempotent.
		if _, err := txn.Scan("t", proto.Key("t").Next(), 0); err != nil {
			t.Fatal(err)
		}

		// The server's clock suddenly jumps ahead of keyFast's timestamp.
		s.Manual.Set(2*offsetNS + 1)

		// Now read slowKey first. It should read at 0, catch an uncertainty error,
		// and get keySlow's timestamp in that error, but upgrade it to the larger
		// node clock (which is ahead of keyFast as well). If the last part does
		// not happen, the read of keyFast should fail (i.e. read nothing).
		// There will be exactly one restart here.
		if gr, err := txn.Get(keySlow); err != nil {
			if i != 1 {
				t.Errorf("unexpected transaction error: %s", err)
			}
			return err
		} else if !gr.Exists() || !bytes.Equal(gr.ValueBytes(), valSlow) {
			t.Errorf("read of %q returned %v, wanted value %q", keySlow, gr.Value, valSlow)
		}

		// The node should already be certain, so we expect no restart here
		// and to read the correct key.
		if gr, err := txn.Get(keyFast); err != nil {
			t.Errorf("second Get failed with %s", err)
		} else if !gr.Exists() || !bytes.Equal(gr.ValueBytes(), valFast) {
			t.Errorf("read of %q returned %v, wanted value %q", keyFast, gr.Value, valFast)
		}
		return nil
	}); tErr != nil {
		t.Fatal(tErr)
	}
}

// TestTxnTimestampRegression verifies that if a transaction's
// timestamp is pushed forward by a concurrent read, it may still
// commit. A bug in the EndTransaction implementation used to compare
// the transaction's current timestamp instead of original timestamp.
func TestTxnTimestampRegression(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := createTestDB(t)
	defer s.Stop()

	keyA := "a"
	keyB := "b"
	err := s.DB.Txn(func(txn *client.Txn) error {
		// Use snapshot isolation so non-transactional read can always push.
		txn.SetSnapshotIsolation()
		// Put transactional value.
		if err := txn.Put(keyA, "value1"); err != nil {
			return err
		}

		// Attempt to read outside of txn (this will push timestamp of transaction).
		if _, err := s.DB.Get(keyA); err != nil {
			return err
		}

		// Now, read again outside of txn to warmup timestamp cache with higher timestamp.
		if _, err := s.DB.Get(keyB); err != nil {
			return err
		}

		// Write now to keyB, which will get a higher timestamp than keyB was written at.
		if err := txn.Put(keyB, "value2"); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestTxnLongDelayBetweenWritesWithConcurrentRead simulates a
// situation where the delay between two writes in a txn is longer
// than 10 seconds.
// See issue #676 for full details about original bug.
func TestTxnLongDelayBetweenWritesWithConcurrentRead(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := createTestDB(t)
	defer s.Stop()

	keyA := proto.Key("a")
	keyB := proto.Key("b")
	ch := make(chan struct{})
	go func() {
		err := s.DB.Txn(func(txn *client.Txn) error {
			// Use snapshot isolation.
			txn.SetSnapshotIsolation()
			// Put transactional value.
			if err := txn.Put(keyA, "value1"); err != nil {
				return err
			}
			// Notify txnB do 1st get(b).
			ch <- struct{}{}
			// Wait for txnB notify us to put(b).
			<-ch
			// Write now to keyB.
			if err := txn.Put(keyB, "value2"); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		// Notify txnB do 2nd get(b).
		ch <- struct{}{}
	}()

	// Wait till txnA finish put(a).
	<-ch
	// Delay for longer than the cache window.
	s.Manual.Set((storage.MinTSCacheWindow + time.Second).Nanoseconds())
	err := s.DB.Txn(func(txn *client.Txn) error {
		// Use snapshot isolation.
		txn.SetSnapshotIsolation()

		// Attempt to get first keyB.
		gr1, err := txn.Get(keyB)
		if err != nil {
			return err
		}
		// Notify txnA put(b).
		ch <- struct{}{}
		// Wait for txnA finish commit.
		<-ch
		// get(b) again.
		gr2, err := txn.Get(keyB)
		if err != nil {
			return err
		}

		if !gr1.Exists() && gr2.Exists() {
			t.Fatalf("Repeat read same key in same txn but get different value gr1 nil gr2 %v", gr2.Value)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestTxnRepeatGetWithRangeSplit simulates two writes in a single
// transaction, with a range split occurring between. The second write
// is sent to the new range. The test verifies that another transaction
// reading before and after the split will read the same values.
// See issue #676 for full details about original bug.
func TestTxnRepeatGetWithRangeSplit(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := createTestDB(t)
	defer s.Stop()

	keyA := proto.Key("a")
	keyC := proto.Key("c")
	splitKey := proto.Key("b")
	ch := make(chan struct{})
	go func() {
		err := s.DB.Txn(func(txn *client.Txn) error {
			// Use snapshot isolation.
			txn.SetSnapshotIsolation()
			// Put transactional value.
			if err := txn.Put(keyA, "value1"); err != nil {
				return err
			}
			// Notify txnB do 1st get(c).
			ch <- struct{}{}
			// Wait for txnB notify us to put(c).
			<-ch
			// Write now to keyC, which will keep timestamp.
			if err := txn.Put(keyC, "value2"); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		// Notify txnB do 2nd get(c).
		ch <- struct{}{}
	}()

	// Wait till txnA finish put(a).
	<-ch

	err := s.DB.Txn(func(txn *client.Txn) error {
		// Use snapshot isolation.
		txn.SetSnapshotIsolation()

		// First get keyC, value will be nil.
		gr1, err := txn.Get(keyC)
		if err != nil {
			return err
		}
		s.Manual.Set(time.Second.Nanoseconds())
		// Split range by keyB.
		if err := s.DB.AdminSplit(splitKey); err != nil {
			t.Fatal(err)
		}
		// Wait till split complete.
		// Check that we split 1 times in allotted time.
		if err := util.IsTrueWithin(func() bool {
			// Scan the meta records.
			rows, err := s.DB.Scan(keys.Meta2Prefix, keys.MetaMax, 0)
			if err != nil {
				t.Fatalf("failed to scan meta2 keys: %s", err)
			}
			return len(rows) >= 2
		}, 6*time.Second); err != nil {
			t.Errorf("failed to split 1 times: %s", err)
		}
		// Notify txnA put(c).
		ch <- struct{}{}
		// Wait for txnA finish commit.
		<-ch
		// Get(c) again.
		gr2, err := txn.Get(keyC)
		if err != nil {
			return err
		}

		if !gr1.Exists() && gr2.Exists() {
			t.Fatalf("Repeat read same key in same txn but get different value gr1 nil gr2 %v", gr2.Value)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestTxnRestartedSerializableTimestampRegression verifies that there is
// no timestamp regression error in the event that a pushed txn record disagrees
// with the original timestamp of a restarted transaction.
func TestTxnRestartedSerializableTimestampRegression(t *testing.T) {
	defer leaktest.AfterTest(t)
	s := createTestDB(t)
	defer s.Stop()

	keyA := "a"
	keyB := "b"
	ch := make(chan struct{})
	go func() {
		err := s.DB.Txn(func(txn *client.Txn) error {
			// Use a low priority for the transaction so that it can be pushed.
			txn.InternalSetPriority(1)

			// Put transactional value.
			if err := txn.Put(keyA, "value1"); err != nil {
				return err
			}
			// Notify txnB to push txnA on get(a).
			ch <- struct{}{}
			// Wait for txnB notify us to commit.
			<-ch
			// Do a write to keyB, which will forward txn timestamp.
			if err := txn.Put(keyB, "value2"); err != nil {
				return err
			}
			// Now commit...
			return nil
		})
		if err != nil {
			close(ch)
			t.Fatal(err)
		}
		ch <- struct{}{}
	}()

	// Wait until txnA finishes put(a).
	<-ch
	// Attempt to get keyA, which will push txnA.
	if _, err := s.DB.Get(keyA); err != nil {
		t.Fatal(err)
	}
	// Do a read at keyB to cause txnA to forward timestamp.
	if _, err := s.DB.Get(keyB); err != nil {
		t.Fatal(err)
	}
	// Notify txnA to commit.
	ch <- struct{}{}
	// Wait for txnA to restart.
	<-ch
	// Notify txnA to commit.
	ch <- struct{}{}
	// Wait for txnA to finish.
	<-ch
}
