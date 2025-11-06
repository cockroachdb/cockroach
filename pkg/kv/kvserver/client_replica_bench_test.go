// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

// BenchmarkEmptyRebalance benchmarks the time it takes to add/remove replicas
// of an empty range.
func BenchmarkEmptyRebalance(b *testing.B) {
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	tc := testcluster.StartTestCluster(
		b, 2, base.TestClusterArgs{ReplicationMode: base.ReplicationManual},
	)
	defer tc.Stopper().Stop(ctx)

	scratchRange := tc.ScratchRange(b)

	// Before actually starting the benchmark, we need to make sure that the raft
	// group is able to add/remove voters. This is important because in leader
	// leases, it takes a few seconds for store liveness heartbeats to start.
	// We need store liveness heartbeats for two reasons: (1) By default,
	// followers won't campaign unless they are supported by a quorum of peers,
	// and (2) The leader won't be able to propose config changes unless the new
	// config doesn't cause a regression in the LeadSupportUntil.
	tc.AddVotersOrFatal(b, scratchRange, tc.Target(1))
	tc.RemoveVotersOrFatal(b, scratchRange, tc.Target(1))

	b.Run("add-remove", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			tc.AddVotersOrFatal(b, scratchRange, tc.Target(1))
			tc.RemoveVotersOrFatal(b, scratchRange, tc.Target(1))
		}
		b.StopTimer()
	})
}
