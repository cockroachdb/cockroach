// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

type replicaCircuitBreakerBench struct {
	*testcluster.TestCluster
	pool *sync.Pool // *BatchRequest
}

func (tc *replicaCircuitBreakerBench) repl(b *testing.B) *kvserver.Replica {
	return tc.GetFirstStoreFromServer(b, 0).LookupReplica(keys.MustAddr(tc.ScratchRange(b)))
}

func setupCircuitBreakerReplicaBench(
	b *testing.B, breakerEnabled bool, cs string,
) (*replicaCircuitBreakerBench, *stop.Stopper) {
	b.Helper()

	var numShards int
	{
		_, err := fmt.Sscanf(cs, "mutexmap-%d", &numShards)
		require.NoError(b, err)
	}
	sFn := func() kvserver.CancelStorage { return &kvserver.MapCancelStorage{NumShards: numShards} }

	var knobs kvserver.StoreTestingKnobs
	knobs.CancelStorageFactory = sFn

	var args base.TestClusterArgs
	args.ServerArgs.Knobs.Store = &knobs
	tc := testcluster.StartTestCluster(b, 1, args)

	stmt := `SET CLUSTER SETTING kv.replica_circuit_breaker.slow_replication_threshold = '1000s'`
	if !breakerEnabled {
		stmt = `SET CLUSTER SETTING kv.replica_circuit_breaker.slow_replication_threshold = '0s'`
	}
	_, err := tc.ServerConn(0).Exec(stmt)
	require.NoError(b, err)
	wtc := &replicaCircuitBreakerBench{
		TestCluster: tc,
	}
	wtc.pool = &sync.Pool{
		New: func() interface{} {
			repl := wtc.repl(b)
			var ba roachpb.BatchRequest
			ba.RangeID = repl.RangeID
			ba.Timestamp = repl.Clock().NowAsClockTimestamp().ToTimestamp()
			var k roachpb.Key
			k = append(k, repl.Desc().StartKey.AsRawKey()...)
			k = encoding.EncodeUint64Ascending(k, uint64(rand.Intn(1000)))
			ba.Add(roachpb.NewGet(k, false))
			return &ba
		},
	}
	return wtc, tc.Stopper()
}

func BenchmarkReplicaCircuitBreakerSendOverhead(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	for _, enabled := range []bool{false, true} {
		b.Run("enabled="+strconv.FormatBool(enabled), func(b *testing.B) {
			dss := []string{
				"mutexmap-1", "mutexmap-2", "mutexmap-4", "mutexmap-8", "mutexmap-12", "mutexmap-16",
				"mutexmap-20", "mutexmap-24", "mutexmap-32", "mutexmap-64",
			}
			if !enabled {
				dss = dss[:1]
			}

			for _, ds := range dss {
				b.Run(ds, func(b *testing.B) {
					b.ReportAllocs()
					tc, stopper := setupCircuitBreakerReplicaBench(b, enabled, ds)
					defer stopper.Stop(ctx)

					repl := tc.repl(b)

					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							ba := tc.pool.Get().(*roachpb.BatchRequest)
							_, err := repl.Send(ctx, *ba)
							tc.pool.Put(ba)
							if err != nil {
								b.Fatal(err)
							}
						}
					})
				})
			}
		})
	}
}
