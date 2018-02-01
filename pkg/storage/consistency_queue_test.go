// Copyright 2016 The Cockroach Authors.
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

package storage_test

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestConsistencyQueueRequiresLive verifies the queue will not
// process ranges whose replicas are not all live.
func TestConsistencyQueueRequiresLive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := storage.TestStoreConfig(nil)
	mtc := &multiTestContext{storeConfig: &sc}
	defer mtc.Stop()
	mtc.Start(t, 3)

	// Replicate the range to three nodes.
	repl := mtc.stores[0].LookupReplica(roachpb.RKeyMin, nil)
	rangeID := repl.RangeID
	mtc.replicateRange(rangeID, 1, 2)

	// Verify that queueing is immediately possible.
	if shouldQ, priority := mtc.stores[0].ConsistencyQueueShouldQueue(
		context.TODO(), mtc.clock.Now(), repl, config.SystemConfig{}); !shouldQ {
		t.Fatalf("expected shouldQ true; got %t, %f", shouldQ, priority)
	}

	// Stop a node and expire leases.
	mtc.stopStore(2)
	mtc.advanceClock(context.TODO())

	if shouldQ, priority := mtc.stores[0].ConsistencyQueueShouldQueue(
		context.TODO(), mtc.clock.Now(), repl, config.SystemConfig{}); shouldQ {
		t.Fatalf("expected shouldQ false; got %t, %f", shouldQ, priority)
	}
}

// TestCheckConsistencyMultiStore creates a node with three stores
// with three way replication. A value is added to the node, and a
// consistency check is run.
func TestCheckConsistencyMultiStore(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const numStores = 3
	mtc := &multiTestContext{}
	defer mtc.Stop()
	mtc.Start(t, numStores)
	// Setup replication of range 1 on store 0 to stores 1 and 2.
	mtc.replicateRange(1, 1, 2)

	// Write something to the DB.
	putArgs := putArgs([]byte("a"), []byte("b"))
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), putArgs); err != nil {
		t.Fatal(err)
	}

	// Run consistency check.
	checkArgs := roachpb.CheckConsistencyRequest{
		Span: roachpb.Span{
			// span of keys that include "a".
			Key:    []byte("a"),
			EndKey: []byte("aa"),
		},
	}
	if _, err := client.SendWrappedWith(context.Background(), rg1(mtc.stores[0]), roachpb.Header{
		Timestamp: mtc.stores[0].Clock().Now(),
	}, &checkArgs); err != nil {
		t.Fatal(err)
	}

}

func TestCheckConsistencyInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sc := storage.TestStoreConfig(nil)
	mtc := &multiTestContext{storeConfig: &sc}
	// Store 0 will report a diff with inconsistent key "e".
	diffKey := []byte("e")
	var diffTimestamp hlc.Timestamp
	notifyReportDiff := make(chan struct{}, 1)
	sc.TestingKnobs.BadChecksumReportDiff =
		func(s roachpb.StoreIdent, diff []storage.ReplicaSnapshotDiff) {
			if s != mtc.Store(0).Ident {
				t.Errorf("BadChecksumReportDiff called from follower (StoreIdent = %s)", s)
				return
			}
			if len(diff) != 1 {
				t.Errorf("diff length = %d, diff = %v", len(diff), diff)
			}
			d := diff[0]
			if d.LeaseHolder || !bytes.Equal(diffKey, d.Key) || diffTimestamp != d.Timestamp {
				t.Errorf("diff = %v", d)
			}
			notifyReportDiff <- struct{}{}
		}
	// Store 0 will panic.
	notifyPanic := make(chan struct{}, 1)
	sc.TestingKnobs.BadChecksumPanic = func(s roachpb.StoreIdent) {
		if s != mtc.Store(0).Ident {
			t.Errorf("BadChecksumPanic called from follower (StoreIdent = %s)", s)
			return
		}
		notifyPanic <- struct{}{}
	}

	const numStores = 3
	defer mtc.Stop()
	mtc.Start(t, numStores)
	// Setup replication of range 1 on store 0 to stores 1 and 2.
	mtc.replicateRange(1, 1, 2)

	// Write something to the DB.
	pArgs := putArgs([]byte("a"), []byte("b"))
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), pArgs); err != nil {
		t.Fatal(err)
	}
	pArgs = putArgs([]byte("c"), []byte("d"))
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), pArgs); err != nil {
		t.Fatal(err)
	}

	// Write some arbitrary data only to store 1. Inconsistent key "e"!
	var val roachpb.Value
	val.SetInt(42)
	diffTimestamp = mtc.stores[1].Clock().Now()
	if err := engine.MVCCPut(
		context.Background(), mtc.stores[1].Engine(), nil, diffKey, diffTimestamp, val, nil,
	); err != nil {
		t.Fatal(err)
	}

	// Run consistency check.
	checkArgs := roachpb.CheckConsistencyRequest{
		Span: roachpb.Span{
			// span of keys that include "a" & "c".
			Key:    []byte("a"),
			EndKey: []byte("z"),
		},
	}
	if _, err := client.SendWrapped(context.Background(), rg1(mtc.stores[0]), &checkArgs); err != nil {
		t.Fatal(err)
	}
	select {
	case <-notifyReportDiff:
	case <-time.After(5 * time.Second):
		t.Fatal("CheckConsistency() failed to report a diff as expected")
	}
	select {
	case <-notifyPanic:
	case <-time.After(5 * time.Second):
		t.Fatal("CheckConsistency() failed to panic as expected")
	}
}

// TestConsistencyQueueRecomputeStats is an end-to-end test of the mechanism CockroachDB
// employs to adjust incorrect MVCCStats ("incorrect" meaning not an inconsistency of
// these stats between replicas, but a delta between persisted stats and those one
// would obtain via a recomputation from the on-disk state), namely a call to
// RecomputeStats triggered from the consistency checker (which also recomputes the stats).
//
// The test splits off a range on a single node cluster backed by an on-disk RocksDB
// instance, and takes that node offline to perturb its stats. Next, it restarts the
// node as part of a cluster, upreplicates the range, and waits for the stats
// divergence to disappear.
//
// The upreplication here is immaterial and serves only to add realism to the test.
func TestConsistencyQueueRecomputeStats(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	path, cleanup := testutils.TempDir(t)
	defer cleanup()

	// Set scanner timings that minimize waiting in this test.
	tsArgs := base.TestServerArgs{
		ScanInterval:    time.Second,
		ScanMaxIdleTime: 100 * time.Millisecond,
	}
	nodeZeroArgs := tsArgs
	nodeZeroArgs.StoreSpecs = []base.StoreSpec{{
		Path: path,
	}}

	clusterArgs := base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs:      tsArgs,
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: nodeZeroArgs,
		},
	}

	key := []byte("a")

	computeDelta := func(db *client.DB) enginepb.MVCCStats {
		var b client.Batch
		b.AddRawRequest(&roachpb.RecomputeStatsRequest{
			Span:   roachpb.Span{Key: key},
			DryRun: true,
		})
		if err := db.Run(ctx, &b); err != nil {
			t.Fatal(err)
		}
		resp := b.RawResponse().Responses[0].GetInner().(*roachpb.RecomputeStatsResponse)
		delta := enginepb.MVCCStats(resp.AddedDelta)
		delta.AgeTo(0)
		return delta
	}

	rangeID := func() roachpb.RangeID {
		tc := testcluster.StartTestCluster(t, 1, clusterArgs)
		defer tc.Stopper().Stop(context.TODO())

		db0 := tc.Servers[0].DB()

		// Split off a range so that we get away from the timeseries writes, which
		// pollute the stats with ContainsEstimates=true. Note that the split clears
		// the right hand side (which is what we operate on) from that flag.
		if err := db0.AdminSplit(ctx, key, key); err != nil {
			t.Fatal(err)
		}

		delta := computeDelta(db0)

		if delta != (enginepb.MVCCStats{}) {
			t.Fatalf("unexpected initial stats adjustment of %+v", delta)
		}

		rangeDesc, err := tc.LookupRange(key)
		if err != nil {
			t.Fatal(err)
		}

		return rangeDesc.RangeID
	}()

	const sysCountGarbage = 123000

	func() {
		cache := engine.NewRocksDBCache(1 << 20)
		defer cache.Release()
		eng, err := engine.NewRocksDB(engine.RocksDBConfig{
			Dir:       path,
			MustExist: true,
		}, cache)
		if err != nil {
			t.Fatal(err)
		}
		defer eng.Close()

		rsl := stateloader.Make(nil /* st */, rangeID)
		ms, err := rsl.LoadMVCCStats(ctx, eng)
		if err != nil {
			t.Fatal(err)
		}

		// Put some garbage in the stats that we're hoping the consistency queue will
		// trigger a removal of via RecomputeStats. SysCount was chosen because it is
		// not affected by the workload we run below and also does not influence the
		// GC queue score.
		ms.SysCount += sysCountGarbage

		// Overwrite with the new stats; remember that this range hasn't upreplicated,
		// so the consistency checker won't see any replica divergence when it runs,
		// but it should definitely see that its recomputed stats mismatch.
		if err := rsl.SetMVCCStats(ctx, eng, &ms); err != nil {
			t.Fatal(err)
		}
	}()

	// Now that we've tampered with the stats, restart the cluster and extend it
	// to three nodes.
	const numNodes = 3
	tc := testcluster.StartTestCluster(t, numNodes, clusterArgs)
	defer tc.Stopper().Stop(ctx)

	db0 := tc.Servers[0].DB()

	// Run a goroutine that writes to the range in a tight loop. This tests that
	// RecomputeStats does not see any skew in its MVCC stats when they are
	// modified concurrently. Note that these writes don't interfere with the
	// field we modified (SysCount).
	tc.Stopper().RunWorker(ctx, func(ctx context.Context) {
		// This channel terminates the loop early if the test takes more than five
		// seconds. This is useful for stress race runs in CI where the tight loop
		// can starve the actual work to be done.
		done := time.After(5 * time.Second)
		for {
			if err := db0.Put(ctx, fmt.Sprintf("%s%d", key, rand.Int63()), "ballast"); err != nil {
				t.Error(err)
			}

			select {
			case <-ctx.Done():
				return
			case <-tc.Stopper().ShouldQuiesce():
				return
			case <-done:
				return
			default:
			}
		}
	})

	var targets []roachpb.ReplicationTarget
	for i := 1; i < numNodes; i++ {
		targets = append(targets, tc.Target(i))
	}
	if _, err := tc.AddReplicas(key, targets...); err != nil {
		t.Fatal(err)
	}

	pg0 := tc.ServerConn(0)

	// Make the consistency checker run aggressively.
	if _, err := pg0.Exec("SET CLUSTER SETTING server.consistency_check.interval = '50ms'"); err != nil {
		t.Fatal(err)
	}

	// The stats should magically repair themselves. Run a cheap check to see when
	// it happened.
	testutils.SucceedsSoon(t, func() error {
		// Run a cheap check first.
		repl, err := tc.Servers[0].GetStores().(*storage.Stores).GetReplicaForRangeID(rangeID)
		if err != nil {
			t.Fatal(err)
		}
		ms := repl.GetMVCCStats()
		if ms.SysCount >= sysCountGarbage {
			err := errors.Errorf("still have a SysCount of %d", ms.SysCount)
			log.Info(ctx, err)
			return err
		}
		return nil
	})

	// Only run the expensive recomputation check now that we're positive that
	// it should succeed.
	if delta := computeDelta(db0); delta != (enginepb.MVCCStats{}) {
		t.Fatalf("stats still in need of adjustment: %+v", delta)
	}
}
