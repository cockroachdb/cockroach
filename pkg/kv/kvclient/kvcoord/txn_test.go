// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/tscache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/kvclientutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/localtestcluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestTxnDBBasics verifies that a simple transaction can be run and
// either committed or aborted. On commit, mutations are visible; on
// abort, mutations are never visible. During the txn, verify that
// uncommitted writes cannot be read outside of the txn but can be
// read from inside the txn.
func TestTxnDBBasics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := createTestDB(t)
	defer s.Stop()
	value := []byte("value")

	for _, commit := range []bool{true, false} {
		key := []byte(fmt.Sprintf("key-%t", commit))

		err := s.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
			// Put transactional value.
			if err := txn.Put(ctx, key, value); err != nil {
				return err
			}

			// Attempt to read in another txn.
			conflictTxn := kv.NewTxn(ctx, s.DB, 0 /* gatewayNodeID */)
			conflictTxn.TestingSetPriority(enginepb.MaxTxnPriority)
			if gr, err := conflictTxn.Get(ctx, key); err != nil {
				return err
			} else if gr.Exists() {
				return errors.Errorf("expected nil value; got %v", gr.Value)
			}

			// Read within the transaction.
			if gr, err := txn.Get(ctx, key); err != nil {
				return err
			} else if !gr.Exists() || !bytes.Equal(gr.ValueBytes(), value) {
				return errors.Errorf("expected value %q; got %q", value, gr.Value)
			}

			if !commit {
				return errors.Errorf("purposefully failing transaction")
			}
			return nil
		})

		if commit != (err == nil) {
			t.Errorf("expected success? %t; got %s", commit, err)
		} else if !commit && !testutils.IsError(err, "purposefully failing transaction") {
			t.Errorf("unexpected failure with !commit: %v", err)
		}

		// Verify the value is now visible on commit == true, and not visible otherwise.
		gr, err := s.DB.Get(context.Background(), key)
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

// BenchmarkSingleRoundtripWithLatency runs a number of transactions writing
// to the same key back to back in a single round-trip. Latency is simulated
// by pausing before each RPC sent.
func BenchmarkSingleRoundtripWithLatency(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	for _, latency := range []time.Duration{0, 10 * time.Millisecond} {
		b.Run(fmt.Sprintf("latency=%s", latency), func(b *testing.B) {
			var s localtestcluster.LocalTestCluster
			s.Latency = latency
			s.Start(b, kvcoord.InitFactoryForLocalTestCluster)
			defer s.Stop()
			defer b.StopTimer()
			key := roachpb.Key("key")
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := s.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
					b := txn.NewBatch()
					b.Put(key, fmt.Sprintf("value-%d", i))
					return txn.CommitInBatch(ctx, b)
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestTxnLostIncrement verifies that Increment with any isolation level is not
// susceptible to the lost update anomaly between the value that the increment
// reads and the value that it writes. In other words, the increment is atomic,
// regardless of isolation level.
//
// The transaction history looks as follows for Snapshot and Serializable:
//
//	R1(A) W2(A,+1) C2 W1(A,+1) [write-write restart] R1(A) W1(A,+1) C1
//
// The transaction history looks as follows for Read Committed:
//
//	R1(A) W2(A,+1) C2 W1(A,+1) C1
func TestTxnLostIncrement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	run := func(t *testing.T, isoLevel isolation.Level, commitInBatch bool) {
		s := createTestDB(t)
		defer s.Stop()
		ctx := context.Background()
		key := roachpb.Key("a")

		incrementKey := func() {
			err := s.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				_, err := txn.Inc(ctx, key, 1)
				require.NoError(t, err)
				return nil
			})
			require.NoError(t, err)
		}

		err := s.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			epoch := txn.Epoch()
			require.LessOrEqual(t, epoch, enginepb.TxnEpoch(1), "should experience just one restart")
			require.NoError(t, txn.SetIsoLevel(isoLevel))

			// Issue a read to get initial value.
			gr, err := txn.Get(ctx, key)
			require.NoError(t, err)
			// NOTE: expect 0 during first attempt, 1 during second attempt.
			require.Equal(t, int64(epoch), gr.ValueInt())

			// During the first attempt, perform a conflicting increment in a
			// different transaction.
			if epoch == 0 {
				incrementKey()
			}

			// Increment the key.
			b := txn.NewBatch()
			b.Inc(key, 1)
			if commitInBatch {
				err = txn.CommitInBatch(ctx, b)
			} else {
				err = txn.Run(ctx, b)
			}
			ir := b.Results[0].Rows[0]

			// During the first attempt, this should encounter a write-write conflict
			// and force a transaction retry for Snapshot and Serializable isolation
			// transactions. For ReadCommitted transactions which allow each batch to
			// operate using a different read snapshot, the increment will be applied
			// to a newer version of the key than that returned by the get, but the
			// increment itself will still be atomic.
			if epoch == 0 && isoLevel != isolation.ReadCommitted {
				require.Error(t, err)
				require.Regexp(t, "TransactionRetryWithProtoRefreshError: .*WriteTooOldError", err)
				return err
			}

			// During the second attempt (or first for Read Committed), this should
			// succeed.
			require.NoError(t, err)
			require.Equal(t, int64(2), ir.ValueInt())
			return nil
		})
		require.NoError(t, err)
	}

	isolation.RunEachLevel(t, func(t *testing.T, isoLevel isolation.Level) {
		testutils.RunTrueAndFalse(t, "commitInBatch", func(t *testing.T, commitInBatch bool) {
			run(t, isoLevel, commitInBatch)
		})
	})
}

// TestTxnLostUpdate verifies that transactions are not susceptible to the
// lost update anomaly if they run at Snapshot isolation or stronger, but
// are susceptible to the lost update anomaly if they run at Read Committed
// isolation.
//
// The transaction history looks as follows for Snapshot and Serializable:
//
//	R1(A) W2(A,"hi") C2 W1(A,"oops!") [write-write restart] R1(A) W1(A,"correct") C1
//
// The transaction history looks as follows for Read Committed:
//
//	R1(A) W2(A,"hi") C2 W1(A,"oops!") C1
func TestTxnLostUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	run := func(t *testing.T, isoLevel isolation.Level, commitInBatch bool) {
		s := createTestDB(t)
		defer s.Stop()
		ctx := context.Background()
		key := roachpb.Key("a")

		putKey := func() {
			err := s.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
				return txn.Put(ctx, key, "hi")
			})
			require.NoError(t, err)
		}

		err := s.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			epoch := txn.Epoch()
			require.LessOrEqual(t, epoch, enginepb.TxnEpoch(1))
			require.NoError(t, txn.SetIsoLevel(isoLevel))

			// Issue a read to get initial value.
			gr, err := txn.Get(ctx, key)
			require.NoError(t, err)
			var newVal string
			if epoch == 0 {
				require.False(t, gr.Exists())
				newVal = "oops!"
			} else {
				require.True(t, gr.Exists())
				require.Equal(t, []byte("hi"), gr.ValueBytes())
				newVal = "correct"
			}

			// During the first attempt, perform a conflicting write.
			if epoch == 0 {
				putKey()
			}

			// Write to the key.
			b := txn.NewBatch()
			b.Put(key, newVal)
			if commitInBatch {
				err = txn.CommitInBatch(ctx, b)
			} else {
				err = txn.Run(ctx, b)
			}

			// During the first attempt, this should encounter a write-write conflict
			// and force a transaction retry for Snapshot and Serializable isolation
			// transactions. For ReadCommitted transactions which allow each batch to
			// operate using a different read snapshot, the write will succeed.
			if epoch == 0 && isoLevel != isolation.ReadCommitted {
				require.Error(t, err)
				require.Regexp(t, "TransactionRetryWithProtoRefreshError: .*WriteTooOldError", err)
				return err
			}

			// During the second attempt (or first for Read Committed), this should
			// succeed.
			require.NoError(t, err)
			return nil
		})
		require.NoError(t, err)

		// Verify final value.
		var expVal string
		if isoLevel != isolation.ReadCommitted {
			expVal = "correct"
		} else {
			expVal = "oops!"
		}
		gr, err := s.DB.Get(ctx, key)
		require.NoError(t, err)
		require.True(t, gr.Exists())
		require.Equal(t, []byte(expVal), gr.ValueBytes())
	}

	isolation.RunEachLevel(t, func(t *testing.T, isoLevel isolation.Level) {
		testutils.RunTrueAndFalse(t, "commitInBatch", func(t *testing.T, commitInBatch bool) {
			run(t, isoLevel, commitInBatch)
		})
	})
}

// TestTxnWeakIsolationLevelsTolerateWriteSkew verifies that transactions run
// under weak isolation levels (snapshot and read committed) can tolerate their
// write timestamp being skewed from their read timestamp, while transaction run
// under strong isolation levels (serializable) cannot.
func TestTxnWeakIsolationLevelsTolerateWriteSkew(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	run := func(t *testing.T, isoLevel isolation.Level) {
		s := createTestDB(t)
		defer s.Stop()
		ctx := context.Background()

		// Begin the test's transaction.
		txn1 := s.DB.NewTxn(ctx, "txn1")
		require.NoError(t, txn1.SetIsoLevel(isoLevel))

		// Read from key "a" in txn1 and then write to key "a" in txn2. This
		// establishes an anti-dependency from txn1 to txn2, meaning that txn1's
		// read snapshot must be ordered before txn2's commit. In practice, this
		// prevents txn1 from refreshing.
		{
			res, err := txn1.Get(ctx, "a")
			require.NoError(t, err)
			require.False(t, res.Exists())

			txn2 := s.DB.NewTxn(ctx, "txn2")
			require.NoError(t, txn2.Put(ctx, "a", "value"))
			require.NoError(t, txn2.Commit(ctx))
		}

		// Now read from key "b" in a txn3 before writing to key "b" in txn1. This
		// establishes an anti-dependency from txn3 to txn1, meaning that txn3's
		// read snapshot must be ordered before txn1's commit. In practice, this
		// pushes txn1's write timestamp forward through the timestamp cache.
		{
			txn3 := s.DB.NewTxn(ctx, "txn3")
			res, err := txn3.Get(ctx, "b")
			require.NoError(t, err)
			require.False(t, res.Exists())
			require.NoError(t, txn3.Commit(ctx))

			require.NoError(t, txn1.Put(ctx, "b", "value"))
		}

		// Finally, try to commit. This should succeed for isolation levels that
		// allow for write skew. It should fail for isolation levels that do not.
		err := txn1.Commit(ctx)
		if isoLevel != isolation.Serializable {
			require.NoError(t, err)
		} else {
			require.Error(t, err)
			require.IsType(t, &kvpb.TransactionRetryWithProtoRefreshError{}, err)
		}
	}

	isolation.RunEachLevel(t, run)
}

// TestTxnReadCommittedPerStatementReadSnapshot verifies that transactions run
// under the read committed isolation level observe a new read snapshot on each
// statement (or kv batch), while transactions run under stronger isolation
// levels (snapshot and serializable) observe a single read snapshot for their
// entire duration.
func TestTxnReadCommittedPerStatementReadSnapshot(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	run := func(
		t *testing.T,
		// The transaction's isolation level.
		isoLevel isolation.Level,
		// Is the transaction's read timestamp fixed?
		fixedReadTs bool,
		// Is manual stepping enabled?
		mode kv.SteppingMode,
		// If manual stepping is enabled, is the transaction actually stepped?
		step bool,
		// Do we expect the transaction to observe intermediate, external writes?
		expObserveExternalWrites bool,
	) {
		s := createTestDB(t)
		defer s.Stop()
		ctx := context.Background()
		key := roachpb.Key("a")

		incrementKey := func() {
			_, err := s.DB.Inc(ctx, key, 1)
			require.NoError(t, err)
		}
		incrementKey()

		// Begin the test's transaction.
		txn1 := s.DB.NewTxn(ctx, "txn1")
		require.NoError(t, txn1.SetIsoLevel(isoLevel))
		initReadTs := txn1.ReadTimestamp()
		if fixedReadTs {
			require.NoError(t, txn1.SetFixedTimestamp(ctx, initReadTs))
		}
		txn1.ConfigureStepping(ctx, mode)

		// In a loop, increment the key outside the transaction, then read it in the
		// transaction. If stepping is enabled, step the transaction before each read.
		var readVals []int64
		for i := 0; i < 3; i++ {
			incrementKey()

			if step {
				require.NoError(t, txn1.Step(ctx, true /* allowReadTimestampStep */))
			}

			// Read the key twice in the same batch, to demonstrate that regardless of
			// isolation level or stepping mode, a single batch observes a single read
			// snapshot.
			b := txn1.NewBatch()
			b.Get(key)
			b.Scan(key, key.Next())
			require.NoError(t, txn1.Run(ctx, b))
			require.Equal(t, 2, len(b.Results))
			require.Equal(t, 1, len(b.Results[0].Rows))
			require.Equal(t, 1, len(b.Results[1].Rows))
			require.Equal(t, b.Results[0].Rows[0], b.Results[1].Rows[0])
			readVals = append(readVals, b.Results[0].Rows[0].ValueInt())
		}

		// Commit the transaction.
		require.NoError(t, txn1.Commit(ctx))

		// Verify that the transaction read the correct values.
		var expVals []int64
		if expObserveExternalWrites {
			expVals = []int64{2, 3, 4}
		} else {
			expVals = []int64{1, 1, 1}
		}
		require.Equal(t, expVals, readVals)

		// Check whether the transaction's read timestamp changed.
		sameReadTs := initReadTs == txn1.ReadTimestamp()
		require.Equal(t, expObserveExternalWrites, !sameReadTs)
	}

	isolation.RunEachLevel(t, func(t *testing.T, isoLevel isolation.Level) {
		testutils.RunTrueAndFalse(t, "fixedReadTs", func(t *testing.T, fixedReadTs bool) {
			testutils.RunTrueAndFalse(t, "steppingMode", func(t *testing.T, modeBool bool) {
				mode := kv.SteppingMode(modeBool)
				if mode == kv.SteppingEnabled {
					// If stepping is enabled, run a variant of the test where the
					// transaction is stepped between reads and a variant of the test
					// where it is not.
					testutils.RunTrueAndFalse(t, "step", func(t *testing.T, step bool) {
						// Expect a new read snapshot on each kv operation if the
						// transaction is read committed, its read timestamp is not
						// fixed, and it is manually stepped.
						expObserveExternalWrites := isoLevel == isolation.ReadCommitted && !fixedReadTs && step
						run(t, isoLevel, fixedReadTs, mode, step, expObserveExternalWrites)
					})
				} else {
					// Expect a new read snapshot on each kv operation if the
					// transaction is read committed and its read timestamp is not
					// fixed.
					expObserveExternalWrites := isoLevel == isolation.ReadCommitted && !fixedReadTs
					run(t, isoLevel, fixedReadTs, mode, false, expObserveExternalWrites)
				}
			})
		})
	})
}

// TestTxnWriteReadConflict verifies that write-read conflicts are non-blocking
// to the reader, except when both the writer and reader are both serializable
// transactions. In that case, the reader will block until the writer completes.
//
// NOTE: the test does not exercise different priority levels. For that, see
// TestCanPushWithPriority.
func TestTxnWriteReadConflict(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	run := func(t *testing.T, writeIsoLevel, readIsoLevel isolation.Level) {
		s := createTestDB(t)
		defer s.Stop()
		ctx := context.Background()

		// Begin the test's writer and reader transactions.
		writeTxn := s.DB.NewTxn(ctx, "writer")
		require.NoError(t, writeTxn.SetIsoLevel(writeIsoLevel))
		readTxn := s.DB.NewTxn(ctx, "reader")
		require.NoError(t, readTxn.SetIsoLevel(readIsoLevel))

		// Perform a write to key "a" in the writer transaction.
		require.NoError(t, writeTxn.Put(ctx, "a", "value"))

		// Read from key "a" in the reader transaction.
		expBlocking := writeIsoLevel == isolation.Serializable && readIsoLevel == isolation.Serializable
		readCtx := ctx
		if expBlocking {
			var cancel func()
			readCtx, cancel = context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()
		}
		res, err := readTxn.Get(readCtx, "a")

		// Verify the expected blocking behavior.
		if expBlocking {
			require.Error(t, err)
			require.ErrorIs(t, err, context.DeadlineExceeded)
		} else {
			require.NoError(t, err)
			require.False(t, res.Exists())
		}

		require.NoError(t, writeTxn.Rollback(ctx))
		require.NoError(t, readTxn.Rollback(ctx))
	}

	for _, writeIsoLevel := range isolation.Levels() {
		for _, readIsoLevel := range isolation.Levels() {
			name := fmt.Sprintf("writeIso=%s,readIso=%s", writeIsoLevel, readIsoLevel)
			t.Run(name, func(t *testing.T) { run(t, writeIsoLevel, readIsoLevel) })
		}
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
	defer log.Scope(t).Close(t)
	s := createTestDBWithKnobs(t, &kvserver.StoreTestingKnobs{
		TestingRequestFilter: func(_ context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
			// Reject transaction heartbeats, which can make the test flaky when they
			// detect an aborted transaction before the Get operation does. See #68584
			// for an explanation.
			if ba.IsSingleHeartbeatTxnRequest() {
				return kvpb.NewErrorf("rejected")
			}
			return nil
		},
	})
	defer s.Stop()

	pushByReading := func(key roachpb.Key) {
		if err := s.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
			if err := txn.SetUserPriority(roachpb.MaxUserPriority); err != nil {
				t.Fatal(err)
			}
			_, err := txn.Get(ctx, key)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	abortByWriting := func(key roachpb.Key) {
		if err := s.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
			if err := txn.SetUserPriority(roachpb.MaxUserPriority); err != nil {
				t.Fatal(err)
			}
			return txn.Put(ctx, key, "foo")
		}); err != nil {
			t.Fatal(err)
		}
	}

	// Try both read and write.
	for _, read := range []bool{true, false} {
		var iteration int
		if err := s.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
			defer func() { iteration++ }()
			key := roachpb.Key(fmt.Sprintf("read=%t", read))

			// Write to lay down an intent (this will send the begin
			// transaction which gets the updated priority).
			if err := txn.Put(ctx, key, "bar"); err != nil {
				return err
			}

			if iteration == 1 {
				// Verify our priority has ratcheted to one less than the pusher's priority
				expPri := enginepb.MaxTxnPriority - 1
				if pri := txn.TestingCloneTxn().Priority; pri != expPri {
					t.Fatalf("%s: expected priority on retry to ratchet to %d; got %d", key, expPri, pri)
				}
				return nil
			}

			// Now simulate a concurrent reader or writer. Our txn will
			// either be pushed or aborted. Then issue a read and verify
			// that if we've been pushed, no error is returned and if we
			// have been aborted, we get an aborted error.
			var err error
			if read {
				pushByReading(key)
				_, err = txn.Get(ctx, key)
				if err != nil {
					t.Fatalf("%s: expected no error; got %s", key, err)
				}
			} else {
				abortByWriting(key)
				_, err = txn.Get(ctx, key)
				assertTransactionAbortedError(t, err)
			}

			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
}

// TestTxnTimestampRegression verifies that if a transaction's timestamp is
// pushed forward by a concurrent read, it may still commit. A bug in the EndTxn
// implementation used to compare the transaction's current timestamp instead of
// original timestamp.
func TestTxnTimestampRegression(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := createTestDB(t)
	defer s.Stop()

	keyA := "a"
	keyB := "b"
	err := s.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		// Put transactional value.
		if err := txn.Put(ctx, keyA, "value1"); err != nil {
			return err
		}

		// Attempt to read in another txn (this will push timestamp of transaction).
		conflictTxn := kv.NewTxn(ctx, s.DB, 0 /* gatewayNodeID */)
		conflictTxn.TestingSetPriority(enginepb.MaxTxnPriority)
		if _, err := conflictTxn.Get(context.Background(), keyA); err != nil {
			return err
		}

		// Now, read again outside of txn to warmup timestamp cache with higher timestamp.
		if _, err := s.DB.Get(context.Background(), keyB); err != nil {
			return err
		}

		// Write now to keyB, which will get a higher timestamp than keyB was written at.
		return txn.Put(ctx, keyB, "value2")
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
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := createTestDB(t)
	defer s.Stop()

	keyA := roachpb.Key("a")
	keyB := roachpb.Key("b")
	ch := make(chan struct{})
	errChan := make(chan error)
	go func() {
		errChan <- s.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
			// Put transactional value.
			if err := txn.Put(ctx, keyA, "value1"); err != nil {
				return err
			}
			// Notify txnB do 1st get(b).
			ch <- struct{}{}
			// Wait for txnB notify us to put(b).
			<-ch
			// Write now to keyB.
			return txn.Put(ctx, keyB, "value2")
		})
	}()

	// Wait till txnA finish put(a).
	<-ch
	// Delay for longer than the cache window.
	s.Manual.Advance(tscache.MinRetentionWindow + time.Second)
	if err := s.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		// Attempt to get first keyB.
		gr1, err := txn.Get(ctx, keyB)
		if err != nil {
			return err
		}
		// Notify txnA put(b).
		ch <- struct{}{}
		// Wait for txnA finish commit.
		if err := <-errChan; err != nil {
			t.Fatal(err)
		}
		// get(b) again.
		gr2, err := txn.Get(ctx, keyB)
		if err != nil {
			return err
		}

		if gr1.Exists() || gr2.Exists() {
			t.Fatalf("Repeat read same key in same txn but get different value gr1: %q, gr2 %q", gr1.Value, gr2.Value)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestTxnRepeatGetWithRangeSplit simulates two writes in a single
// transaction, with a range split occurring between. The second write
// is sent to the new range. The test verifies that another transaction
// reading before and after the split will read the same values.
// See issue #676 for full details about original bug.
func TestTxnRepeatGetWithRangeSplit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := createTestDBWithKnobs(t, &kvserver.StoreTestingKnobs{
		DisableScanner:    true,
		DisableSplitQueue: true,
		DisableMergeQueue: true,
	})
	defer s.Stop()

	keyA := roachpb.Key("a")
	keyC := roachpb.Key("c")
	splitKey := roachpb.Key("b")
	ch := make(chan struct{})
	errChan := make(chan error)
	go func() {
		errChan <- s.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
			// Put transactional value.
			if err := txn.Put(ctx, keyA, "value1"); err != nil {
				return err
			}
			// Notify txnB do 1st get(c).
			ch <- struct{}{}
			// Wait for txnB notify us to put(c).
			<-ch
			// Write now to keyC, which will keep timestamp.
			return txn.Put(ctx, keyC, "value2")
		})
	}()

	// Wait till txnA finish put(a).
	<-ch

	if err := s.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		// First get keyC, value will be nil.
		gr1, err := txn.Get(ctx, keyC)
		if err != nil {
			return err
		}
		s.Manual.Advance(time.Second)
		// Split range by keyB.
		if err := s.DB.AdminSplit(
			context.Background(),
			splitKey,
			hlc.MaxTimestamp, /* expirationTime */
		); err != nil {
			t.Fatal(err)
		}
		// Wait till split complete.
		// Check that we split 1 times in allotted time.
		testutils.SucceedsSoon(t, func() error {
			// Scan the meta records.
			rows, serr := s.DB.Scan(context.Background(), keys.Meta2Prefix, keys.MetaMax, 0)
			if serr != nil {
				t.Fatalf("failed to scan meta2 keys: %s", serr)
			}
			if len(rows) >= 2 {
				return nil
			}
			return errors.Errorf("failed to split")
		})
		// Notify txnA put(c).
		ch <- struct{}{}
		// Wait for txnA finish commit.
		if err := <-errChan; err != nil {
			t.Fatal(err)
		}
		// Get(c) again.
		gr2, err := txn.Get(ctx, keyC)
		if err != nil {
			return err
		}

		if !gr1.Exists() && gr2.Exists() {
			t.Fatalf("Repeat read same key in same txn but get different value gr1 nil gr2 %v", gr2.Value)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// TestTxnRestartedSerializableTimestampRegression verifies that there is
// no timestamp regression error in the event that a pushed txn record disagrees
// with the original timestamp of a restarted transaction.
func TestTxnRestartedSerializableTimestampRegression(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := createTestDB(t)
	defer s.Stop()

	keyA := "a"
	keyB := "b"
	ch := make(chan struct{})
	errChan := make(chan error)
	var count int
	go func() {
		errChan <- s.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
			count++
			// Use a low priority for the transaction so that it can be pushed.
			if err := txn.SetUserPriority(roachpb.MinUserPriority); err != nil {
				t.Fatal(err)
			}

			// Put transactional value.
			if err := txn.Put(ctx, keyA, "value1"); err != nil {
				return err
			}
			if count <= 1 {
				// Notify concurrent getter to push txnA on get(a).
				ch <- struct{}{}
				// Wait for txnB notify us to commit.
				<-ch
			}
			// Do a write to keyB, which will forward txn timestamp.
			return txn.Put(ctx, keyB, "value2")
		})
	}()

	// Wait until txnA finishes put(a).
	<-ch
	// Attempt to get keyA, which will push txnA.
	if _, err := s.DB.Get(context.Background(), keyA); err != nil {
		t.Fatal(err)
	}
	// Do a read at keyB to cause txnA to forward timestamp.
	if _, err := s.DB.Get(context.Background(), keyB); err != nil {
		t.Fatal(err)
	}
	// Notify txnA to commit.
	ch <- struct{}{}

	// Wait for txnA to finish.
	if err := <-errChan; err != nil {
		t.Fatal(err)
	}
	// We expect no restarts (so a count of one). The transaction continues
	// despite the push and timestamp forwarding in order to lay down all
	// intents in the first pass. On the first EndTxn, the difference in
	// timestamps would cause the serializable transaction to update spans, but
	// only writes occurred during the transaction, so the commit succeeds.
	const expCount = 1
	if count != expCount {
		t.Fatalf("expected %d restarts, but got %d", expCount, count)
	}
}

// TestTxnResolveIntentsFromMultipleEpochs verifies that that intents
// from earlier epochs are cleaned up on transaction commit.
func TestTxnResolveIntentsFromMultipleEpochs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := createTestDB(t)
	defer s.Stop()
	ctx := context.Background()

	writeSkewKey := "write-skew"
	keys := []string{"a", "b", "c"}
	ch := make(chan struct{})
	errChan := make(chan error, 1)
	// Launch goroutine to write the three keys on three successive epochs.
	go func() {
		var count int
		err := s.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			// Read the write skew key, which will be written by another goroutine
			// to ensure transaction restarts.
			if _, err := txn.Get(ctx, writeSkewKey); err != nil {
				return err
			}
			// Signal that the transaction has (re)started.
			ch <- struct{}{}
			// Wait for concurrent writer to write key.
			<-ch
			// Now write our version over the top (will get a pushed timestamp).
			if err := txn.Put(ctx, keys[count], "txn"); err != nil {
				return err
			}
			count++
			return nil
		})
		if err != nil {
			errChan <- err
		} else if count < len(keys) {
			errChan <- fmt.Errorf(
				"expected to have to retry %d times and only retried %d times", len(keys), count-1)
		} else {
			errChan <- nil
		}
	}()

	step := func(key string, causeWriteSkew bool) {
		// Wait for transaction to start.
		<-ch
		if causeWriteSkew {
			// Write to the write skew key to ensure a restart.
			if err := s.DB.Put(ctx, writeSkewKey, "skew-"+key); err != nil {
				t.Fatal(err)
			}
		}
		// Read key to push txn's timestamp forward on its write.
		if _, err := s.DB.Get(ctx, key); err != nil {
			t.Fatal(err)
		}
		// Signal the transaction to continue.
		ch <- struct{}{}
	}

	// Step 1 causes a restart.
	step(keys[0], true)
	// Step 2 causes a restart.
	step(keys[1], true)
	// Step 3 does not result in a restart.
	step(keys[2], false)

	// Wait for txn to finish.
	if err := <-errChan; err != nil {
		t.Fatal(err)
	}

	// Read values for three keys. The first two should be empty, the last should be "txn".
	for i, k := range keys {
		v, err := s.DB.Get(ctx, k)
		if err != nil {
			t.Fatal(err)
		}
		str := string(v.ValueBytes())
		if i < len(keys)-1 {
			if str != "" {
				t.Errorf("key %s expected \"\"; got %s", k, str)
			}
		} else {
			if str != "txn" {
				t.Errorf("key %s expected \"txn\"; got %s", k, str)
			}
		}
	}
}

// Test that txn.CommitTimestamp() reflects refreshes.
func TestTxnCommitTimestampAdvancedByRefresh(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// We're going to inject an uncertainty error, expect the refresh to succeed,
	// and then check that the txn.CommitTimestamp() value reflects the refresh.
	injected := false
	var refreshTS hlc.Timestamp
	errKey := roachpb.Key("inject_err")
	s := createTestDBWithKnobs(t, &kvserver.StoreTestingKnobs{
		TestingRequestFilter: func(_ context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
			if g, ok := ba.GetArg(kvpb.Get); ok && g.(*kvpb.GetRequest).Key.Equal(errKey) {
				if injected {
					return nil
				}
				injected = true
				txn := ba.Txn.Clone()
				refreshTS = txn.WriteTimestamp.Add(0, 1)
				pErr := kvpb.NewReadWithinUncertaintyIntervalError(
					txn.ReadTimestamp,
					hlc.ClockTimestamp{},
					txn,
					refreshTS,
					hlc.ClockTimestamp{})
				return kvpb.NewErrorWithTxn(pErr, txn)
			}
			return nil
		},
	})
	defer s.Stop()

	err := s.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		_, err := txn.Get(ctx, errKey)
		if err != nil {
			return err
		}
		if !injected {
			return errors.Errorf("didn't inject err")
		}
		commitTS, err := txn.CommitTimestamp()
		require.NoError(t, err)
		// We expect to have refreshed just after the timestamp injected by the error.
		expTS := refreshTS.Add(0, 1)
		if !commitTS.Equal(expTS) {
			return errors.Errorf("expected refreshTS: %s, got: %s", refreshTS, commitTS)
		}
		return nil
	})
	require.NoError(t, err)
}

// Test that a transaction can be used after a CPut returns a
// ConditionFailedError. This is not generally allowed for other errors, but
// ConditionFailedError is special.
func TestTxnContinueAfterCputError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s := createTestDB(t)
	defer s.Stop()

	txn := s.DB.NewTxn(ctx, "test txn")
	// Note: Since we're expecting the CPut to fail, the massaging done by
	// StrToCPutExistingValue() is not actually necessary.
	expVal := kvclientutils.StrToCPutExistingValue("dummy")
	err := txn.CPut(ctx, "a", "val", expVal)
	require.IsType(t, &kvpb.ConditionFailedError{}, err)

	require.NoError(t, txn.Put(ctx, "a", "b'"))
	require.NoError(t, txn.Commit(ctx))
}

// Test that a transaction can be used after a locking request returns a
// WriteIntentError. This is not generally allowed for other errors, but
// a WriteIntentError is special.
func TestTxnContinueAfterWriteIntentError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s := createTestDB(t)
	defer s.Stop()

	otherTxn := s.DB.NewTxn(ctx, "lock holder txn")
	require.NoError(t, otherTxn.Put(ctx, "a", "b"))

	txn := s.DB.NewTxn(ctx, "test txn")

	b := txn.NewBatch()
	b.Header.WaitPolicy = lock.WaitPolicy_Error
	b.Put("a", "c")
	err := txn.Run(ctx, b)
	require.IsType(t, &kvpb.WriteIntentError{}, err)

	require.NoError(t, txn.Put(ctx, "a'", "c"))
	require.NoError(t, txn.Commit(ctx))
}

func TestTxnWaitPolicies(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s := createTestDB(t)
	defer s.Stop()

	testutils.RunTrueAndFalse(t, "highPriority", func(t *testing.T, highPriority bool) {
		key := []byte("b")
		require.NoError(t, s.DB.Put(ctx, key, "old value"))

		txn := s.DB.NewTxn(ctx, "test txn")
		require.NoError(t, txn.Put(ctx, key, "new value"))

		pri := roachpb.NormalUserPriority
		if highPriority {
			pri = roachpb.MaxUserPriority
		}

		// Block wait policy.
		blockC := make(chan error)
		go func() {
			var b kv.Batch
			b.Header.UserPriority = pri
			b.Header.WaitPolicy = lock.WaitPolicy_Block
			b.Get(key)
			blockC <- s.DB.Run(ctx, &b)
		}()

		if highPriority {
			// Should push txn and not block.
			require.NoError(t, <-blockC)
		} else {
			// Should block.
			select {
			case err := <-blockC:
				t.Fatalf("blocking wait policy unexpected returned with err=%v", err)
			case <-time.After(10 * time.Millisecond):
			}
		}

		// Error wait policy.
		errorC := make(chan error)
		go func() {
			var b kv.Batch
			b.Header.UserPriority = pri
			b.Header.WaitPolicy = lock.WaitPolicy_Error
			b.Get(key)
			errorC <- s.DB.Run(ctx, &b)
		}()

		if highPriority {
			// Should push txn and not block.
			require.NoError(t, <-errorC)
		} else {
			// Should return error immediately, without blocking.
			err := <-errorC
			require.NotNil(t, err)
			lcErr := new(kvpb.WriteIntentError)
			require.True(t, errors.As(err, &lcErr))
			require.Equal(t, kvpb.WriteIntentError_REASON_WAIT_POLICY, lcErr.Reason)
		}

		// SkipLocked wait policy.
		type skipRes struct {
			res []kv.Result
			err error
		}
		skipC := make(chan skipRes)
		go func() {
			var b kv.Batch
			b.Header.UserPriority = pri
			b.Header.WaitPolicy = lock.WaitPolicy_SkipLocked
			b.Get(key)
			err := s.DB.Run(ctx, &b)
			skipC <- skipRes{res: b.Results, err: err}
		}()

		// Should return successful but empty result immediately, without blocking.
		// Priority does not matter.
		res := <-skipC
		require.Nil(t, res.err)
		require.Len(t, res.res, 1)
		getRes := res.res[0]
		require.Len(t, getRes.Rows, 1)
		require.False(t, getRes.Rows[0].Exists())

		// Let blocked requests proceed.
		require.NoError(t, txn.Commit(ctx))
		if !highPriority {
			require.NoError(t, <-blockC)
		}
	})
}

func TestTxnLockTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s := createTestDB(t)
	defer s.Stop()

	key := []byte("b")
	txn := s.DB.NewTxn(ctx, "test txn")
	require.NoError(t, txn.Put(ctx, key, "new value"))

	var b kv.Batch
	b.Header.LockTimeout = 25 * time.Millisecond
	b.Get(key)
	err := s.DB.Run(ctx, &b)
	require.NotNil(t, err)
	lcErr := new(kvpb.WriteIntentError)
	require.True(t, errors.As(err, &lcErr))
	require.Equal(t, kvpb.WriteIntentError_REASON_LOCK_TIMEOUT, lcErr.Reason)
}

// TestTxnReturnsWriteTooOldErrorOnConflictingDeleteRange tests that if two
// transactions issue delete range operations over the same keys, the later
// transaction eagerly returns a WriteTooOld error instead of deferring the
// error and temporarily leaking a non-serializable state through its ReturnKeys
// field.
func TestTxnReturnsWriteTooOldErrorOnConflictingDeleteRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	s := createTestDB(t)
	defer s.Stop()

	// Write a value at key "a".
	require.NoError(t, s.DB.Put(ctx, "a", "val"))

	// Create two transactions.
	txn1 := s.DB.NewTxn(ctx, "test txn 1")
	txn2 := s.DB.NewTxn(ctx, "test txn 2")

	// Both txns read the value.
	kvs, err := txn1.Scan(ctx, "a", "b", 0)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, roachpb.Key("a"), kvs[0].Key)

	kvs, err = txn2.Scan(ctx, "a", "b", 0)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, roachpb.Key("a"), kvs[0].Key)

	// The first transaction deletes the value using a delete range operation.
	b := txn1.NewBatch()
	b.DelRange("a", "b", true /* returnKeys */)
	require.NoError(t, txn1.Run(ctx, b))
	require.Len(t, b.Results[0].Keys, 1)
	require.Equal(t, roachpb.Key("a"), b.Results[0].Keys[0])

	// The first transaction commits.
	require.NoError(t, txn1.Commit(ctx))

	// The second transaction attempts to delete the value using a delete range
	// operation. This should immediately fail with a WriteTooOld error. It
	// would be incorrect for this to be delayed and for 0 keys to be returned.
	b = txn2.NewBatch()
	b.DelRange("a", "b", true /* returnKeys */)
	err = txn2.Run(ctx, b)
	require.NotNil(t, err)
	require.Regexp(t, "TransactionRetryWithProtoRefreshError: WriteTooOldError", err)
	require.Len(t, b.Results[0].Keys, 0)
}

// TestRetrySerializableBumpsToNow verifies that transaction read time is forwarded to the
// current HLC time rather than closed timestamp to give it enough time to retry.
// To achieve that, test fixes transaction time to prevent refresh from succeeding first
// then waits for closed timestamp to advance sufficiently and commits. Since write
// can't succeed below closed timestamp and commit timestamp has leaked and can't be bumped
// txn is restarted. Test then verifies that it can proceed even if closed timestamp
// is again updated.
func TestRetrySerializableBumpsToNow(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s := createTestDB(t)
	defer s.Stop()
	ctx := context.Background()

	bumpClosedTimestamp := func(delay time.Duration) {
		s.Manual.Advance(delay)
		// We need to bump closed timestamp for clock increment to have effect
		// on further kv writes. Putting anything into proposal buffer will
		// trigger achieve this.
		// We write a value to a non-overlapping key to ensure we are not
		// bumping test transaction. We can't use reads because they could
		// only bump tscache directly which we try not to do in this test case.
		require.NoError(t, s.DB.Put(ctx, roachpb.Key("z"), []byte{0}))
	}

	attempt := 0
	require.NoError(t, s.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		require.Less(t, attempt, 2, "Transaction can't recover after initial retry error, too many retries")
		closedTsTargetDuration := closedts.TargetDuration.Get(&s.Cfg.Settings.SV)
		delay := closedTsTargetDuration / 2
		if attempt == 0 {
			delay = closedTsTargetDuration * 2
		}
		bumpClosedTimestamp(delay)
		attempt++
		// Fixing transaction commit timestamp to disallow read refresh.
		_, err := txn.CommitTimestamp()
		require.NoError(t, err)
		// Perform a scan to populate the transaction's read spans and mandate a refresh
		// if the transaction's write timestamp is ever bumped. Because we fixed the
		// transaction's commit timestamp, it will be forced to retry.
		_, err = txn.Scan(ctx, roachpb.Key("a"), roachpb.Key("p"), 1000)
		require.NoError(t, err, "Failed Scan request")
		// Perform a write, which will run into the closed timestamp and get pushed.
		require.NoError(t, txn.Put(ctx, roachpb.Key("b"), []byte(fmt.Sprintf("value-%d", attempt))))
		return nil
	}))
	require.Greater(t, attempt, 1, "Transaction is expected to retry once")
}

// TestTxnRetryWithLatchesDroppedEarly serves as a regression test for
// https://github.com/cockroachdb/cockroach/issues/92189. It constructs a batch
// like:
// b.Scan(a, e)
// b.Put(b, "value2")
// which is forced to retry at a higher timestamp. It ensures that the scan
// request does not see the intent at key b, even when the retry happens.
func TestTxnRetryWithLatchesDroppedEarly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := createTestDB(t)
	defer s.Stop()

	keyA := "a"
	keyB := "b"
	keyE := "e"
	keyF := "f"

	err := s.DB.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		s.Manual.Advance(1 * time.Second)

		{
			// Attempt to write to keyF in another txn.
			conflictTxn := kv.NewTxn(ctx, s.DB, 0 /* gatewayNodeID */)
			conflictTxn.TestingSetPriority(enginepb.MaxTxnPriority)
			if err := conflictTxn.Put(ctx, keyF, "valueF"); err != nil {
				return err
			}
			if err := conflictTxn.Commit(ctx); err != nil {
				return err
			}
		}

		b := txn.NewBatch()
		b.Scan(keyA, keyE)
		b.Put(keyB, "value2")
		b.Put(keyF, "value3") // bumps the transaction and causes a server side retry.

		err := txn.Run(ctx, b)
		if err != nil {
			return err
		}

		// Ensure no rows were returned as part of the scan.
		require.Equal(t, 0, len(b.RawResponse().Responses[0].GetInner().(*kvpb.ScanResponse).Rows))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestTxnUpdateFromTxnRecordDoesNotOverwriteFields tests that any field in
// the Transaction proto, that is not present in TransactionRecord, is not
// accidentally overwritten by Update().
// OmitInRangefeeds and AdmissionPriority are two such fields.
func TestTxnUpdateFromTxnRecordDoesNotOverwriteFields(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	keyA := roachpb.Key("a")
	var storeKnobs kvserver.StoreTestingKnobs
	storeKnobs.TestingProposalFilter = func(args kvserverbase.ProposalFilterArgs) *kvpb.Error {
		if args.Req.Txn != nil && args.Req.Txn.Name == "txn" {
			// Ensure all requests by the transaction use the right admission
			// priority.
			require.Equal(t, int32(admissionpb.UserHighPri), args.Req.Txn.AdmissionPriority)
		}
		return nil
	}

	s, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{Store: &storeKnobs},
	})
	defer s.Stopper().Stop(context.Background())
	ctx := context.Background()

	// Start a transaction with high admission priority.
	txn := kv.NewTxnWithAdmissionControl(
		ctx, kvDB, 0, kvpb.AdmissionHeader_ROOT_KV, admissionpb.UserHighPri)
	// Set OmitInRangefeeds to true.
	txn.SetOmitInRangefeeds()
	txn.SetDebugName("txn")
	require.NoError(t, txn.Put(ctx, keyA, "a"))

	hbRequest := &kvpb.HeartbeatTxnRequest{
		RequestHeader: kvpb.RequestHeader{Key: keyA},
		Now:           txn.ReadTimestamp(),
	}

	// The first txn heartbeat writes the TransactionRecord to disk.
	// OmitInRangefeeds and AdmissionPriority are not present on the
	// TransactionRecord proto, so they are not written to disk.
	b := txn.NewBatch()
	b.AddRawRequest(hbRequest)
	require.NoError(t, txn.Run(ctx, b))

	// The second txn heartbeat reads the TransactionRecord from disk, writes an
	// updated one, and returns it.
	// As part of command evaluation, the Transaction proto is updated with the
	// new TransactionRecord. OmitInRangefeeds and AdmissionPriority are not
	// dropped in the process because they can be updated only if they were not
	// set previously.
	b = txn.NewBatch()
	b.AddRawRequest(hbRequest)
	require.NoError(t, txn.Run(ctx, b))

	require.NoError(t, txn.Commit(ctx))

	// OmitInRangefeeds is still true.
	require.True(t, txn.GetOmitInRangefeeds())
}
