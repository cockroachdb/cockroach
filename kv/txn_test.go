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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package kv

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// TestTxnDBBasics verifies that a simple transaction can be run and
// either committed or aborted. On commit, mutations are visible; on
// abort, mutations are never visible. During the txn, verify that
// uncommitted writes cannot be read outside of the txn but can be
// read from inside the txn.
func TestTxnDBBasics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()
	value := []byte("value")

	for _, commit := range []bool{true, false} {
		key := []byte(fmt.Sprintf("key-%t", commit))

		pErr := s.DB.Txn(func(txn *client.Txn) *roachpb.Error {
			// Use snapshot isolation so non-transactional read can always push.
			if err := txn.SetIsolation(roachpb.SNAPSHOT); err != nil {
				return roachpb.NewError(err)
			}

			// Put transactional value.
			if pErr := txn.Put(key, value); pErr != nil {
				return pErr
			}

			// Attempt to read outside of txn.
			if gr, pErr := s.DB.Get(key); pErr != nil {
				return pErr
			} else if gr.Exists() {
				return roachpb.NewErrorf("expected nil value; got %v", gr.Value)
			}

			// Read within the transaction.
			if gr, pErr := txn.Get(key); pErr != nil {
				return pErr
			} else if !gr.Exists() || !bytes.Equal(gr.ValueBytes(), value) {
				return roachpb.NewErrorf("expected value %q; got %q", value, gr.Value)
			}

			if !commit {
				return roachpb.NewErrorf("purposefully failing transaction")
			}
			return nil
		})

		if commit != (pErr == nil) {
			t.Errorf("expected success? %t; got %s", commit, pErr)
		} else if !commit && !testutils.IsPError(pErr, "purposefully failing transaction") {
			t.Errorf("unexpected failure with !commit: %s", pErr)
		}

		// Verify the value is now visible on commit == true, and not visible otherwise.
		gr, pErr := s.DB.Get(key)
		if commit {
			if pErr != nil || !gr.Exists() || !bytes.Equal(gr.ValueBytes(), value) {
				t.Errorf("expected success reading value: %+v, %s", gr.ValueBytes(), pErr)
			}
		} else {
			if pErr != nil || gr.Exists() {
				t.Errorf("expected success and nil value: %s, %s", gr, pErr)
			}
		}
	}
}

// benchmarkSingleRoundtripWithLatency runs a number of transactions writing to
// the same key back to back in a single round-trip. Latency is simulated
// by pausing before each RPC sent.
func benchmarkSingleRoundtripWithLatency(b *testing.B, latency time.Duration) {
	s := &LocalTestCluster{}
	s.Latency = latency
	s.Start(b)
	defer s.Stop()
	defer b.StopTimer()
	key := roachpb.Key("key")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if tErr := s.DB.Txn(func(txn *client.Txn) *roachpb.Error {
			b := txn.NewBatch()
			b.Put(key, fmt.Sprintf("value-%d", i))
			return txn.CommitInBatch(b)
		}); tErr != nil {
			b.Fatal(tErr)
		}
	}
}

func BenchmarkSingleRoundtripTxnWithLatency_0(b *testing.B) {
	benchmarkSingleRoundtripWithLatency(b, 0)
}

func BenchmarkSingleRoundtripTxnWithLatency_10(b *testing.B) {
	benchmarkSingleRoundtripWithLatency(b, 10*time.Millisecond)
}

// TestSnapshotIsolationIncrement verifies that Increment with snapshot
// isolation yields an increment based on the original timestamp of
// the transaction, not the forwarded timestamp.
func TestSnapshotIsolationIncrement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()

	var key = roachpb.Key("a")
	var key2 = roachpb.Key("b")

	done := make(chan *roachpb.Error)
	start := make(chan struct{})

	go func() {
		<-start
		done <- s.DB.Txn(func(txn *client.Txn) *roachpb.Error {
			if _, pErr := txn.Inc(key, 1); pErr != nil {
				return pErr
			}
			if _, pErr := txn.Get(key2); pErr != nil {
				return pErr
			}
			return nil
		})
	}()

	if pErr := s.DB.Txn(func(txn *client.Txn) *roachpb.Error {
		if err := txn.SetIsolation(roachpb.SNAPSHOT); err != nil {
			t.Fatal(err)
		}

		// Issue a read to get initial value.
		if _, pErr := txn.Get(key); pErr != nil {
			t.Fatal(pErr)
		}

		if txn.Proto.Epoch == 0 {
			close(start) // let someone write into our future
			// When they're done writing, increment.
			if pErr := <-done; pErr != nil {
				t.Fatal(pErr)
			}
		} else if txn.Proto.Epoch > 1 {
			t.Fatal("should experience just one restart")
		}

		// Start by writing key2, which will move our txn timestamp forward.
		if pErr := txn.Put(key2, "foo"); pErr != nil {
			t.Fatal(pErr)
		}
		// Now, increment with txn.Timestamp equal to key2's timestamp
		// cache entry + 1. We want to be sure that we still see a value
		// of 0 on our first increment (as txn.OrigTimestamp was set
		// before anything else happened in the concurrent writer
		// goroutine). The second iteration of the txn should read the
		// correct value and commit.
		if txn.Proto.Epoch == 0 && !txn.Proto.OrigTimestamp.Less(txn.Proto.Timestamp) {
			t.Fatalf("expected orig timestamp less than timestamp: %s", txn.Proto)
		}
		ir, pErr := txn.Inc(key, 1)
		if pErr != nil {
			t.Fatal(pErr)
		}
		if vi := ir.ValueInt(); vi != int64(txn.Proto.Epoch+1) {
			t.Errorf("expected %d; got %d", txn.Proto.Epoch+1, vi)
		}
		// Verify that the WriteTooOld boolean is set on the txn.
		if (txn.Proto.Epoch == 0) != txn.Proto.WriteTooOld {
			t.Fatalf("expected write too old=%t; got %t", (txn.Proto.Epoch == 0), txn.Proto.WriteTooOld)
		}
		return nil
	}); pErr != nil {
		t.Fatal(pErr)
	}
}

// TestSnapshotIsolationLostUpdate verifies that snapshot isolation
// transactions are not susceptible to the lost update anomaly.
//
// The transaction history looks as follows ("2" refers to the
// independent goroutine's actions)
//
//   R1(A) W2(A,"hi") W1(A,"oops!") C1 [serializable restart] R1(A) W1(A,"correct") C1
func TestSnapshotIsolationLostUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()
	var key = roachpb.Key("a")

	done := make(chan *roachpb.Error)
	start := make(chan struct{})
	go func() {
		<-start
		done <- s.DB.Txn(func(txn *client.Txn) *roachpb.Error {
			return txn.Put(key, "hi")
		})
	}()

	if pErr := s.DB.Txn(func(txn *client.Txn) *roachpb.Error {
		if err := txn.SetIsolation(roachpb.SNAPSHOT); err != nil {
			t.Fatal(err)
		}

		// Issue a read to get initial value.
		gr, pErr := txn.Get(key)
		if pErr != nil {
			t.Fatal(pErr)
		}
		if txn.Proto.Epoch == 0 {
			close(start) // let someone write into our future
			// When they're done, write based on what we read.
			if pErr := <-done; pErr != nil {
				t.Fatal(pErr)
			}
		} else if txn.Proto.Epoch > 1 {
			t.Fatal("should experience just one restart")
		}

		if gr.Exists() && bytes.Equal(gr.ValueBytes(), []byte("hi")) {
			if pErr := txn.Put(key, "correct"); pErr != nil {
				t.Fatal(pErr)
			}
		} else {
			if pErr := txn.Put(key, "oops!"); pErr != nil {
				t.Fatal(pErr)
			}
		}
		// Verify that the WriteTooOld boolean is set on the txn.
		if (txn.Proto.Epoch == 0) != txn.Proto.WriteTooOld {
			t.Fatalf("expected write too old set (%t): got %t", (txn.Proto.Epoch == 0), txn.Proto.WriteTooOld)
		}
		return nil
	}); pErr != nil {
		t.Fatal(pErr)
	}

	// Verify final value.
	gr, pErr := s.DB.Get(key)
	if pErr != nil {
		t.Fatal(pErr)
	}
	if !gr.Exists() || !bytes.Equal(gr.ValueBytes(), []byte("correct")) {
		t.Fatalf("expected \"correct\", got %q", gr.ValueBytes())
	}
}

// TestPriorityRatchetOnAbortOrPush verifies that the priority of
// a transaction is ratcheted by successive aborts or pushes. In
// particular, we want to ensure ratcheted priorities when the txn
// discovers it's been aborted or pushed through a poisoned sequence
// cache. This happens when a concurrent writer aborts an intent or a
// concurrent reader pushes an intent.
func TestPriorityRatchetOnAbortOrPush(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()

	const pusheePri = 1
	const pusherPri = 10 // pusher will win

	pushByReading := func(key roachpb.Key) {
		if pErr := s.DB.Txn(func(txn *client.Txn) *roachpb.Error {
			txn.InternalSetPriority(pusherPri)
			_, pErr := txn.Get(key)
			return pErr
		}); pErr != nil {
			t.Fatal(pErr)
		}
	}
	abortByWriting := func(key roachpb.Key) {
		if pErr := s.DB.Txn(func(txn *client.Txn) *roachpb.Error {
			txn.InternalSetPriority(pusherPri)
			return txn.Put(key, "foo")
		}); pErr != nil {
			t.Fatal(pErr)
		}
	}

	// Try all combinations of read/write and snapshot/serializable isolation.
	for _, read := range []bool{true, false} {
		for _, iso := range []roachpb.IsolationType{roachpb.SNAPSHOT, roachpb.SERIALIZABLE} {
			var iteration int
			if pErr := s.DB.Txn(func(txn *client.Txn) *roachpb.Error {
				defer func() { iteration++ }()
				key := roachpb.Key(fmt.Sprintf("read=%t, iso=%s", read, iso))

				// Only set our priority on first try.
				if iteration == 0 {
					txn.InternalSetPriority(pusheePri)
				}
				if err := txn.SetIsolation(iso); err != nil {
					t.Fatal(err)
				}

				// Write to lay down an intent (this will send the begin
				// transaction which gets the updated priority).
				if pErr := txn.Put(key, "bar"); pErr != nil {
					return pErr
				}

				if iteration == 1 {
					// Verify our priority has ratcheted to one less than the pusher's priority
					if pri := txn.Proto.Priority; pri != pusherPri-1 {
						t.Fatalf("%s: expected priority on retry to ratchet to %d; got %d", key, pusherPri-1, pri)
					}
					return nil
				}

				// Now simulate a concurrent reader or writer. Our txn will
				// either be pushed or aborted. Then issue a read and verify
				// that if we've been pushed, no error is returned and if we
				// have been aborted, we get an aborted error.
				var pErr *roachpb.Error
				if read {
					pushByReading(key)
					_, pErr = txn.Get(key)
					if pErr != nil {
						t.Fatalf("%s: expected no error; got %s", key, pErr)
					}
				} else {
					abortByWriting(key)
					_, pErr = txn.Get(key)
					if _, ok := pErr.GetDetail().(*roachpb.TransactionAbortedError); !ok {
						t.Fatalf("%s: expected transaction aborted error; got %s", key, pErr)
					}
				}

				return pErr
			}); pErr != nil {
				t.Fatal(pErr)
			}
		}
	}
}

// disableOwnNodeCertain is used in tests which want to verify uncertainty
// related logic on single-node installations. In regular operation, trans-
// actional requests automatically have the "own" NodeID marked as free from
// uncertainty, which interferes with the tests. The feature is disabled simply
// by poisoning the gossip NodeID; this may break other functionality which
// is usually not relevant in uncertainty tests.
func disableOwnNodeCertain(tc *LocalTestCluster) {
	desc := tc.distSender.getNodeDescriptor()
	desc.NodeID = 999
	tc.distSender.gossip.ResetNodeID(desc.NodeID)
	if err := tc.distSender.gossip.SetNodeDescriptor(desc); err != nil {
		panic(err)
	}
}

// TestUncertaintyRestart verifies that a transaction which finds a write in
// its near future will restart exactly once, meaning that it's made a note of
// that node's clock for its new timestamp.
func TestUncertaintyRestart(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	disableOwnNodeCertain(s)
	defer s.Stop()
	const maxOffset = 250 * time.Millisecond
	s.Clock.SetMaxOffset(maxOffset)
	s.Manual.Set(maxOffset.Nanoseconds() + 1)

	var key = roachpb.Key("a")

	done := make(chan struct{})
	start := make(chan struct{})
	go func() {
		defer close(done)
		<-start
		if pErr := s.DB.Txn(func(txn *client.Txn) *roachpb.Error {
			return txn.Put(key, "hi")
		}); pErr != nil {
			t.Fatal(pErr)
		}
	}()

	if pErr := s.DB.Txn(func(txn *client.Txn) *roachpb.Error {
		if txn.Proto.Epoch > 2 {
			t.Fatal("expected only one restart")
		}
		// Issue a read to pick a timestamp.
		if _, pErr := txn.Get(key.Next()); pErr != nil {
			t.Fatal(pErr)
		}
		if txn.Proto.Epoch == 0 {
			close(start) // let someone write into our future
			<-done       // when they're done, try to read
		}
		_, pErr := txn.Get(key.Next())
		if pErr != nil {
			if _, ok := pErr.GetDetail().(*roachpb.ReadWithinUncertaintyIntervalError); !ok {
				t.Fatalf("unexpected error: %T: %s", pErr, pErr)
			}
		}
		return pErr
	}); pErr != nil {
		t.Fatal(pErr)
	}
}

// TestUncertaintyObservedTimestampForwarding checks that when receiving an
// uncertainty restart on a node, the next attempt to read (at the increased
// timestamp) is free from uncertainty. See roachpb.Transaction for details.
func TestUncertaintyMaxTimestampForwarding(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	disableOwnNodeCertain(s)
	defer s.Stop()
	// Large offset so that any value in the future is an uncertain read.
	// Also makes sure that the values we write in the future below don't
	// actually wind up in the past.
	s.Clock.SetMaxOffset(50 * time.Second)

	offsetNS := int64(100)
	keySlow := roachpb.Key("slow")
	keyFast := roachpb.Key("fast")
	valSlow := []byte("wols")
	valFast := []byte("tsaf")

	// Write keySlow at now+offset, keyFast at now+2*offset
	futureTS := s.Clock.Now()
	futureTS.WallTime += offsetNS
	val := roachpb.MakeValueFromBytes(valSlow)
	if err := engine.MVCCPut(context.Background(), s.Eng, nil, keySlow, futureTS, val, nil); err != nil {
		t.Fatal(err)
	}
	futureTS.WallTime += offsetNS
	val.SetBytes(valFast)
	if err := engine.MVCCPut(context.Background(), s.Eng, nil, keyFast, futureTS, val, nil); err != nil {
		t.Fatal(err)
	}

	i := 0
	if tErr := s.DB.Txn(func(txn *client.Txn) *roachpb.Error {
		i++
		// The first command serves to start a Txn, fixing the timestamps.
		// There will be a restart, but this is idempotent.
		if _, pErr := txn.Scan("t", roachpb.Key("t").Next(), 0); pErr != nil {
			t.Fatal(pErr)
		}
		// This is a bit of a hack for the sake of this test: By visiting the
		// node above, we've made a note of its clock, which allows us to
		// prevent the restart. But we want to catch the restart, so reset the
		// observed timestamps.
		txn.Proto.ResetObservedTimestamps()

		// The server's clock suddenly jumps ahead of keyFast's timestamp.
		s.Manual.Set(2*offsetNS + 1)

		// Now read slowKey first. It should read at 0, catch an uncertainty error,
		// and get keySlow's timestamp in that error, but upgrade it to the larger
		// node clock (which is ahead of keyFast as well). If the last part does
		// not happen, the read of keyFast should fail (i.e. read nothing).
		// There will be exactly one restart here.
		if gr, pErr := txn.Get(keySlow); pErr != nil {
			if i != 1 {
				t.Fatalf("unexpected transaction error: %s", pErr)
			}
			return pErr
		} else if !gr.Exists() || !bytes.Equal(gr.ValueBytes(), valSlow) {
			t.Fatalf("read of %q returned %v, wanted value %q", keySlow, gr.Value, valSlow)
		}

		// The node should already be certain, so we expect no restart here
		// and to read the correct key.
		if gr, pErr := txn.Get(keyFast); pErr != nil {
			t.Fatalf("second Get failed with %s", pErr)
		} else if !gr.Exists() || !bytes.Equal(gr.ValueBytes(), valFast) {
			t.Fatalf("read of %q returned %v, wanted value %q", keyFast, gr.Value, valFast)
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
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()

	keyA := "a"
	keyB := "b"
	pErr := s.DB.Txn(func(txn *client.Txn) *roachpb.Error {
		// Use snapshot isolation so non-transactional read can always push.
		if err := txn.SetIsolation(roachpb.SNAPSHOT); err != nil {
			return roachpb.NewError(err)
		}
		// Put transactional value.
		if pErr := txn.Put(keyA, "value1"); pErr != nil {
			return pErr
		}

		// Attempt to read outside of txn (this will push timestamp of transaction).
		if _, pErr := s.DB.Get(keyA); pErr != nil {
			return pErr
		}

		// Now, read again outside of txn to warmup timestamp cache with higher timestamp.
		if _, pErr := s.DB.Get(keyB); pErr != nil {
			return pErr
		}

		// Write now to keyB, which will get a higher timestamp than keyB was written at.
		if pErr := txn.Put(keyB, "value2"); pErr != nil {
			return pErr
		}
		return nil
	})
	if pErr != nil {
		t.Fatal(pErr)
	}
}

// TestTxnLongDelayBetweenWritesWithConcurrentRead simulates a
// situation where the delay between two writes in a txn is longer
// than 10 seconds.
// See issue #676 for full details about original bug.
func TestTxnLongDelayBetweenWritesWithConcurrentRead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()

	keyA := roachpb.Key("a")
	keyB := roachpb.Key("b")
	ch := make(chan struct{})
	go func() {
		pErr := s.DB.Txn(func(txn *client.Txn) *roachpb.Error {
			// Use snapshot isolation.
			if err := txn.SetIsolation(roachpb.SNAPSHOT); err != nil {
				return roachpb.NewError(err)
			}
			// Put transactional value.
			if pErr := txn.Put(keyA, "value1"); pErr != nil {
				return pErr
			}
			// Notify txnB do 1st get(b).
			ch <- struct{}{}
			// Wait for txnB notify us to put(b).
			<-ch
			// Write now to keyB.
			if pErr := txn.Put(keyB, "value2"); pErr != nil {
				return pErr
			}
			return nil
		})
		if pErr != nil {
			t.Fatal(pErr)
		}
		// Notify txnB do 2nd get(b).
		ch <- struct{}{}
	}()

	// Wait till txnA finish put(a).
	<-ch
	// Delay for longer than the cache window.
	s.Manual.Set((storage.MinTSCacheWindow + time.Second).Nanoseconds())
	pErr := s.DB.Txn(func(txn *client.Txn) *roachpb.Error {
		// Use snapshot isolation.
		if err := txn.SetIsolation(roachpb.SNAPSHOT); err != nil {
			return roachpb.NewError(err)
		}

		// Attempt to get first keyB.
		gr1, pErr := txn.Get(keyB)
		if pErr != nil {
			return pErr
		}
		// Notify txnA put(b).
		ch <- struct{}{}
		// Wait for txnA finish commit.
		<-ch
		// get(b) again.
		gr2, pErr := txn.Get(keyB)
		if pErr != nil {
			return pErr
		}

		if gr1.Exists() || gr2.Exists() {
			t.Fatalf("Repeat read same key in same txn but get different value gr1: %q, gr2 %q", gr1.Value, gr2.Value)
		}
		return nil
	})
	if pErr != nil {
		t.Fatal(pErr)
	}
}

// TestTxnRepeatGetWithRangeSplit simulates two writes in a single
// transaction, with a range split occurring between. The second write
// is sent to the new range. The test verifies that another transaction
// reading before and after the split will read the same values.
// See issue #676 for full details about original bug.
func TestTxnRepeatGetWithRangeSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()

	keyA := roachpb.Key("a")
	keyC := roachpb.Key("c")
	splitKey := roachpb.Key("b")
	ch := make(chan struct{})
	go func() {
		pErr := s.DB.Txn(func(txn *client.Txn) *roachpb.Error {
			// Use snapshot isolation.
			if err := txn.SetIsolation(roachpb.SNAPSHOT); err != nil {
				return roachpb.NewError(err)
			}
			// Put transactional value.
			if pErr := txn.Put(keyA, "value1"); pErr != nil {
				return pErr
			}
			// Notify txnB do 1st get(c).
			ch <- struct{}{}
			// Wait for txnB notify us to put(c).
			<-ch
			// Write now to keyC, which will keep timestamp.
			if pErr := txn.Put(keyC, "value2"); pErr != nil {
				return pErr
			}
			return nil
		})
		if pErr != nil {
			t.Fatal(pErr)
		}
		// Notify txnB do 2nd get(c).
		ch <- struct{}{}
	}()

	// Wait till txnA finish put(a).
	<-ch

	pErr := s.DB.Txn(func(txn *client.Txn) *roachpb.Error {
		// Use snapshot isolation.
		if err := txn.SetIsolation(roachpb.SNAPSHOT); err != nil {
			return roachpb.NewError(err)
		}

		// First get keyC, value will be nil.
		gr1, pErr := txn.Get(keyC)
		if pErr != nil {
			return pErr
		}
		s.Manual.Set(time.Second.Nanoseconds())
		// Split range by keyB.
		if pErr := s.DB.AdminSplit(splitKey); pErr != nil {
			t.Fatal(pErr)
		}
		// Wait till split complete.
		// Check that we split 1 times in allotted time.
		util.SucceedsSoon(t, func() error {
			// Scan the meta records.
			rows, spErr := s.DB.Scan(keys.Meta2Prefix, keys.MetaMax, 0)
			if spErr != nil {
				t.Fatalf("failed to scan meta2 keys: %s", spErr)
			}
			if len(rows) >= 2 {
				return nil
			}
			return util.Errorf("failed to split")
		})
		// Notify txnA put(c).
		ch <- struct{}{}
		// Wait for txnA finish commit.
		<-ch
		// Get(c) again.
		gr2, pErr := txn.Get(keyC)
		if pErr != nil {
			return pErr
		}

		if !gr1.Exists() && gr2.Exists() {
			t.Fatalf("Repeat read same key in same txn but get different value gr1 nil gr2 %v", gr2.Value)
		}
		return nil
	})
	if pErr != nil {
		t.Fatal(pErr)
	}
}

// TestTxnRestartedSerializableTimestampRegression verifies that there is
// no timestamp regression error in the event that a pushed txn record disagrees
// with the original timestamp of a restarted transaction.
func TestTxnRestartedSerializableTimestampRegression(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := createTestDB(t)
	defer s.Stop()

	keyA := "a"
	keyB := "b"
	ch := make(chan struct{})
	var count int
	go func() {
		pErr := s.DB.Txn(func(txn *client.Txn) *roachpb.Error {
			count++
			// Use a low priority for the transaction so that it can be pushed.
			txn.InternalSetPriority(1)

			// Put transactional value.
			if pErr := txn.Put(keyA, "value1"); pErr != nil {
				return pErr
			}
			if count <= 2 {
				// Notify concurrent getter to push txnA on get(a).
				ch <- struct{}{}
				// Wait for txnB notify us to commit.
				<-ch
			}
			// Do a write to keyB, which will forward txn timestamp.
			if pErr := txn.Put(keyB, "value2"); pErr != nil {
				return pErr
			}
			// Now commit...
			return nil
		})
		close(ch)
		if pErr != nil {
			t.Fatal(pErr)
		}
	}()

	// Wait until txnA finishes put(a).
	<-ch
	// Attempt to get keyA, which will push txnA.
	if _, pErr := s.DB.Get(keyA); pErr != nil {
		t.Fatal(pErr)
	}
	// Do a read at keyB to cause txnA to forward timestamp.
	if _, pErr := s.DB.Get(keyB); pErr != nil {
		t.Fatal(pErr)
	}
	// Notify txnA to commit.
	ch <- struct{}{}
	// Wait for txnA to restart.
	<-ch
	// Notify txnA to commit.
	ch <- struct{}{}
	// Wait for txnA to finish.
	for range ch {
	}
	// We expect one restart (so a count of two). The transaction continues
	// despite the push and timestamp forwarding in order to lay down all
	// intents in the first pass. On the first EndTransaction, the difference
	// in timestamps causes the serializable transaction to retry.
	const expCount = 2
	if count != expCount {
		t.Fatalf("expected %d restarts, but got %d", expCount, count)
	}
}
