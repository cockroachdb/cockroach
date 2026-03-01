// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/stretchr/testify/require"
)

// BenchmarkLeaderTickWithLeaderLeases benchmarks the performance of the replica
// tick when the replica is the leader and running leader leases.
func BenchmarkLeaderTickWithLeaderLeases(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	kvserver.OverrideDefaultLeaseType(ctx, &st.SV, roachpb.LeaseLeader)

	// Create a cluster with one node to make sure that this is the leader.
	cluster := testcluster.StartTestCluster(b, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings: st,
		},
	})
	defer cluster.Stopper().Stop(ctx)

	// Set up a replica to be ticked, and wait for the lease to get upgraded.
	keyA := cluster.ScratchRange(b)
	desc := cluster.LookupRangeOrFatal(b, keyA)
	cluster.MaybeWaitForLeaseUpgrade(ctx, b, desc)
	store := cluster.GetFirstStoreFromServer(b, 0)
	repl := store.LookupReplica(desc.StartKey)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.ProcessTick(ctx, repl.RangeID)
	}
}

// BenchmarkOptimisticEvalForLocks benchmarks optimistic evaluation when the
// potentially conflicting lock is explicitly held for a duration of time.
func BenchmarkOptimisticEvalForLocks(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	args := base.TestServerArgs{}
	s, _, db := serverutils.StartServer(b, args)
	defer s.Stopper().Stop(ctx)

	require.NoError(b, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		defer func() {
			b.Log(err)
		}()
		if err := txn.Put(ctx, "a", "a"); err != nil {
			return err
		}
		if err := txn.Put(ctx, "b", "b"); err != nil {
			return err
		}
		return txn.Commit(ctx)
	}))
	tup, err := db.Get(ctx, "a")
	require.NoError(b, err)
	require.NotNil(b, tup.Value)
	tup, err = db.Get(ctx, "b")
	require.NoError(b, err)
	require.NotNil(b, tup.Value)

	for _, realContention := range []bool{false, true} {
		b.Run(fmt.Sprintf("real-contention=%t", realContention),
			func(b *testing.B) {
				lockStart := "b"
				if realContention {
					lockStart = "a"
				}
				finishWrites := make(chan struct{})
				var writers sync.WaitGroup
				for i := 0; i < 1; i++ {
					writers.Add(1)
					go func() {
						for {
							txn := db.NewTxn(ctx, "locking txn")
							_, err = txn.ScanForUpdate(ctx, lockStart, "c", 0, kvpb.BestEffort)
							require.NoError(b, err)
							time.Sleep(5 * time.Millisecond)
							// Normally, it would do a write here, but we don't bother.
							require.NoError(b, txn.Commit(ctx))
							select {
							case _, recv := <-finishWrites:
								if !recv {
									writers.Done()
									return
								}
							default:
							}
						}
					}()
				}
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					_ = db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
						_, err = txn.Scan(ctx, "a", "c", 1)
						if err != nil {
							panic(err)
						}
						err = txn.Commit(ctx)
						if err != nil {
							panic(err)
						}
						return err
					})
				}
				b.StopTimer()
				close(finishWrites)
				writers.Wait()
			})
	}
}

// BenchmarkOptimisticEval benchmarks optimistic evaluation with
//   - potentially conflicting latches held by 1PC transactions doing writes.
//   - potentially conflicting latches or locks held by transactions doing
//     writes.
func BenchmarkOptimisticEval(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	args := base.TestServerArgs{}

	for _, latches := range []bool{false, true} {
		conflictWith := "latches-and-locks"
		if latches {
			conflictWith = "latches"
		}
		b.Run(conflictWith, func(b *testing.B) {
			for _, realContention := range []bool{false, true} {
				b.Run(fmt.Sprintf("real-contention=%t", realContention), func(b *testing.B) {
					for _, numWriters := range []int{1, 4} {
						b.Run(fmt.Sprintf("num-writers=%d", numWriters), func(b *testing.B) {
							// Since we are doing writes in the benchmark, start with a
							// fresh server each time so that we start with a fresh engine
							// without many versions for a key.
							s, _, db := serverutils.StartServer(b, args)
							defer s.Stopper().Stop(ctx)

							require.NoError(b, db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
								if err := txn.Put(ctx, "a", "a"); err != nil {
									return err
								}
								if err := txn.Put(ctx, "b", "b"); err != nil {
									return err
								}
								return txn.Commit(ctx)
							}))
							tup, err := db.Get(ctx, "a")
							require.NoError(b, err)
							require.NotNil(b, tup.Value)
							tup, err = db.Get(ctx, "b")
							require.NoError(b, err)
							require.NotNil(b, tup.Value)

							writeKey := "b"
							if realContention {
								writeKey = "a"
							}
							finishWrites := make(chan struct{})
							var writers sync.WaitGroup
							for i := 0; i < numWriters; i++ {
								writers.Add(1)
								go func(shouldLatch bool) {
									for {
										if shouldLatch {
											require.NoError(b, db.Put(ctx, writeKey, "foo"))
										} else {
											require.NoError(b, db.Txn(ctx,
												func(ctx context.Context, txn *kv.Txn) (err error) {
													if err := txn.Put(ctx, writeKey, "foo"); err != nil {
														return err
													}
													return txn.Commit(ctx)
												}))
										}
										select {
										case _, recv := <-finishWrites:
											if !recv {
												writers.Done()
												return
											}
										default:
										}
									}
								}(latches)
							}
							b.ResetTimer()
							for i := 0; i < b.N; i++ {
								_ = db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
									_, err = txn.Scan(ctx, "a", "c", 1)
									if err != nil {
										panic(err)
									}
									err = txn.Commit(ctx)
									if err != nil {
										panic(err)
									}
									return err
								})
							}
							b.StopTimer()
							close(finishWrites)
							writers.Wait()
						})
					}
				})
			}
		})
	}
}

func BenchmarkIntentResolution(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	const batchSize = 1000
	const batchCount = 1

	pendingWriter := func(t testing.TB, ctx context.Context, txn *kv.Txn, makeKey func(i int) roachpb.Key) {
		for i := range batchCount {
			batch := txn.NewBatch()
			for j := range batchSize {
				batch.Put(makeKey(j+batchSize*i), "heyo")
			}
			require.NoError(t, txn.Run(ctx, batch))
		}
	}
	rolledBackWriter := func(t testing.TB, ctx context.Context, txn *kv.Txn, makeKey func(i int) roachpb.Key) {
		pendingWriter(t, ctx, txn, makeKey)
		require.NoError(t, txn.Rollback(ctx))
	}
	abortedWriter := func(t testing.TB, ctx context.Context, txn *kv.Txn, makeKey func(i int) roachpb.Key) {
		pendingWriter(t, ctx, txn, makeKey)

		txn2 := txn.DB().NewTxn(ctx, "aborter")
		require.NoError(t, txn2.SetUserPriority(roachpb.MaxUserPriority))
		pushee := txn.Sender().TestingCloneTxn().TxnMeta
		ba := &kvpb.BatchRequest{}
		ba.Add(&kvpb.PushTxnRequest{
			RequestHeader: kvpb.RequestHeader{
				Key: pushee.Key,
			},
			PushType:  kvpb.PUSH_ABORT,
			PusheeTxn: pushee,
			PusherTxn: *txn2.Sender().TestingCloneTxn(),
			Force:     true,
		})
		_, pErr := txn.DB().NonTransactionalSender().Send(ctx, ba)
		require.NoError(t, pErr.GoError())
	}

	highPriorityScan := func(t testing.TB, ctx context.Context, kvDB *kv.DB, span roachpb.Span) {
		txn2 := kvDB.NewTxn(ctx, "high priority reader")
		require.NoError(t, txn2.SetUserPriority(roachpb.MaxUserPriority))
		_, err := txn2.Scan(ctx, span.Key, span.EndKey, 0 /* maxKeys */)
		require.NoError(t, err)
	}

	highPriorityExportRequest := func(t testing.TB, ctx context.Context, kvDB *kv.DB, span roachpb.Span) {
		readTimestamp := kvDB.Clock().Now()

		req := &kvpb.ExportRequest{
			RequestHeader: kvpb.RequestHeaderFromSpan(span),
		}

		header := kvpb.Header{
			TargetBytes:                 1,
			Timestamp:                   readTimestamp,
			ReturnElasticCPUResumeSpans: true,
			UserPriority:                roachpb.MaxUserPriority,
		}
		_, pErr := kv.SendWrappedWith(ctx, kvDB.NonTransactionalSender(), header, req)
		require.NoError(t, pErr.GoError())
	}

	type testCase struct {
		name   string
		writer func(testing.TB, context.Context, *kv.Txn, func(i int) roachpb.Key)
		reader func(testing.TB, context.Context, *kv.DB, roachpb.Span)
	}

	testCases := []testCase{
		{
			name:   "writer=PENDING/reader=Scan",
			writer: pendingWriter,
			reader: highPriorityScan,
		},
		{
			name:   "writer=PENDING/reader=Export",
			writer: pendingWriter,
			reader: highPriorityExportRequest,
		},
		// NB: The writer=ABORTED cases create a rolled back transaction. Since half
		// of the intents will be on the range with the transaction record, the
		// reader in this case will only have half as many intents to resolve. As a
		// result, one cannot compare these benchmarks to the cases that have the
		// full set of intents to resolve.
		//
		// Async intent resolution is disabled via a testing hook.
		{
			name:   "writer=ABORTED/reader=Scan",
			writer: rolledBackWriter,
			reader: highPriorityScan,
		},
		{
			name:   "writer=ABORTED/reader=Export",
			writer: rolledBackWriter,
			reader: highPriorityExportRequest,
		},
		// Here we create an aborted transaction via a PUSH_ABORT from a different
		// transaction.
		{
			name:   "writer=force-ABORTED/reader=Scan",
			writer: abortedWriter,
			reader: highPriorityScan,
		},
		{
			name:   "writer=force-ABORTED/reader=Export",
			writer: abortedWriter,
			reader: highPriorityExportRequest,
		},
	}

	for _, virtualIntentResolution := range []bool{false, true} {
		b.Run(fmt.Sprintf("virtual-resolution=%t", virtualIntentResolution), func(b *testing.B) {
			for _, tc := range testCases {
				b.Run(tc.name, func(b *testing.B) {
					// doDebug is rather expensive because it also installs tracing spans
					// which is very expensive
					const doDebug = false
					var (
						txnID                     atomic.Value
						pushCounter               atomic.Int32
						resolveIntentCounter      atomic.Int32
						resolveIntentRangeCounter atomic.Int32

						requestFilter         kvserverbase.ReplicaRequestFilter
						printAndResetCounters = func() {}
					)
					if doDebug {
						testutils.SetVModule(b, "lock_table_waiter=4,intent_resolver=4")
						printAndResetCounters = func() {
							// These counters are useful for making sure the test is actually doing
							// what we expect.
							b.Logf("Counters for %s", txnID.Load().(string))
							b.Logf("\t%d pushes", pushCounter.Load())
							b.Logf("\t%d resolve intents", resolveIntentCounter.Load())
							b.Logf("\t%d resolve intent ranges", resolveIntentRangeCounter.Load())
							txnID.Store("")
							pushCounter.Store(0)
							resolveIntentCounter.Store(0)
							resolveIntentRangeCounter.Store(0)
						}
						requestFilter = func(ctx context.Context, ba *kvpb.BatchRequest) *kvpb.Error {
							tID := txnID.Load()
							if tID == nil {
								return nil
							}
							tIDstr := tID.(string)
							if tIDstr == "" {
								return nil
							}
							for _, ru := range ba.Requests {
								switch r := ru.GetInner().(type) {
								case *kvpb.PushTxnRequest:
									if r.PusheeTxn.ID.String() == tIDstr {
										pushCounter.Add(1)
									}
								case *kvpb.ResolveIntentRequest:
									if r.IntentTxn.ID.String() == tIDstr {
										resolveIntentCounter.Add(1)
									}
								case *kvpb.ResolveIntentRangeRequest:
									if r.IntentTxn.ID.String() == tIDstr {
										resolveIntentRangeCounter.Add(1)
									}
								}
							}
							return nil
						}
					}

					s, _, kvDB := serverutils.StartServer(b, base.TestServerArgs{
						Knobs: base.TestingKnobs{
							Store: &kvserver.StoreTestingKnobs{
								TestingRequestFilter: requestFilter,
								IntentResolverKnobs: kvserverbase.IntentResolverTestingKnobs{
									// Disable async intent resolution, so that the reader must do all
									// required intent resolution.
									DisableAsyncIntentResolution: true,
								},
							},
						},
					})
					defer s.Stopper().Stop(ctx)
					concurrency.VirtualIntentResolution.Override(ctx, &s.ClusterSettings().SV, virtualIntentResolution)
					scratchRangeKey, err := s.ScratchRange()
					require.NoError(b, err)

					testRange := roachpb.Span{
						Key:    scratchRangeKey,
						EndKey: scratchRangeKey.PrefixEnd(),
					}
					makeKey := func(i int) roachpb.Key {
						return append(testRange.Key.Clone(), []byte(fmt.Sprintf("-%05d", i))...)
					}
					_, _, err = s.SplitRange(makeKey(batchCount * batchSize / 2))
					require.NoError(b, err)

					for b.Loop() {
						func() {
							b.StopTimer()

							collectAndFinishReader := func() tracingpb.Recording { return tracingpb.Recording{} }
							collectAndFinishWriter := func() tracingpb.Recording { return tracingpb.Recording{} }

							readerCtx := ctx
							writerCtx := ctx
							if doDebug {
								tracer := s.TracerI().(*tracing.Tracer)
								readerCtx, collectAndFinishReader = tracing.ContextWithRecordingSpan(ctx, tracer, "reader")
								writerCtx, collectAndFinishWriter = tracing.ContextWithRecordingSpan(ctx, tracer, "writer")

								defer collectAndFinishReader()
								defer collectAndFinishWriter()
							}

							txn1 := kvDB.NewTxn(ctx, "writer")
							txnID.Store(txn1.ID().String())

							tc.writer(b, writerCtx, txn1, makeKey)
							if doDebug {
								b.Logf("WRITER TRACE: %s", collectAndFinishWriter())
							}

							b.StartTimer()
							tc.reader(b, readerCtx, kvDB, testRange)
							b.StopTimer()
							if doDebug {
								b.Logf("READER TRACE: %s", collectAndFinishReader())
							}
							printAndResetCounters()
							// If txn1 is still alive, roll it back.
							_ = txn1.Rollback(ctx)

							// Clear out the keys so that the next iteration doesn't have to iterate
							// over them.
							_, pErr := kv.SendWrappedWith(ctx, kvDB.NonTransactionalSender(), kvpb.Header{}, &kvpb.ClearRangeRequest{
								RequestHeader: kvpb.RequestHeader{
									Key:    testRange.Key,
									EndKey: testRange.EndKey,
								},
							})
							require.Nil(b, pErr, "error: %s", pErr)
							b.StartTimer()
						}()
					}
				})
			}
		})
	}
}
