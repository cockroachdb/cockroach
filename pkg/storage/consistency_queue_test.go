// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage_test

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/stateloader"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	repl := mtc.stores[0].LookupReplica(roachpb.RKeyMin)
	rangeID := repl.RangeID
	mtc.replicateRange(rangeID, 1, 2)

	// Verify that queueing is immediately possible.
	if shouldQ, priority := mtc.stores[0].ConsistencyQueueShouldQueue(
		context.TODO(), mtc.clock.Now(), repl, config.NewSystemConfig(sc.DefaultZoneConfig)); !shouldQ {
		t.Fatalf("expected shouldQ true; got %t, %f", shouldQ, priority)
	}

	// Stop a node and expire leases.
	mtc.stopStore(2)
	mtc.advanceClock(context.TODO())

	if shouldQ, priority := mtc.stores[0].ConsistencyQueueShouldQueue(
		context.TODO(), mtc.clock.Now(), repl, config.NewSystemConfig(sc.DefaultZoneConfig)); shouldQ {
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
	if _, err := client.SendWrapped(context.Background(), mtc.stores[0].TestSender(), putArgs); err != nil {
		t.Fatal(err)
	}

	// Run consistency check.
	checkArgs := roachpb.CheckConsistencyRequest{
		RequestHeader: roachpb.RequestHeader{
			// span of keys that include "a".
			Key:    []byte("a"),
			EndKey: []byte("aa"),
		},
	}
	if _, err := client.SendWrappedWith(context.Background(), mtc.stores[0].TestSender(), roachpb.Header{
		Timestamp: mtc.stores[0].Clock().Now(),
	}, &checkArgs); err != nil {
		t.Fatal(err)
	}
}

// TestCheckConsistencyReplay verifies that two ComputeChecksum requests with
// the same checksum ID are not committed to the Raft log, even if DistSender
// retries the request.
func TestCheckConsistencyReplay(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type applyKey struct {
		checksumID uuid.UUID
		storeID    roachpb.StoreID
	}
	var state struct {
		syncutil.Mutex
		forcedRetry bool
		applies     map[applyKey]int
	}
	state.applies = map[applyKey]int{}

	var mtc *multiTestContext
	ctx := context.Background()
	storeCfg := storage.TestStoreConfig(nil /* clock */)

	// Arrange to count the number of times each checksum command applies to each
	// store.
	storeCfg.TestingKnobs.TestingApplyFilter = func(args storagebase.ApplyFilterArgs) (int, *roachpb.Error) {
		state.Lock()
		defer state.Unlock()
		if ccr := args.ComputeChecksum; ccr != nil {
			state.applies[applyKey{ccr.ChecksumID, args.StoreID}]++
		}
		return 0, nil
	}

	// Arrange to trigger a retry when a ComputeChecksum request arrives.
	storeCfg.TestingKnobs.TestingResponseFilter = func(ba roachpb.BatchRequest, br *roachpb.BatchResponse) *roachpb.Error {
		state.Lock()
		defer state.Unlock()
		if ba.IsSingleComputeChecksumRequest() && !state.forcedRetry {
			state.forcedRetry = true
			return roachpb.NewError(roachpb.NewSendError("injected failure"))
		}
		return nil
	}

	mtc = &multiTestContext{storeConfig: &storeCfg}
	defer mtc.Stop()
	mtc.Start(t, 2)

	mtc.replicateRange(roachpb.RangeID(1), 1)

	checkArgs := roachpb.CheckConsistencyRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    []byte("a"),
			EndKey: []byte("b"),
		},
	}
	if _, err := client.SendWrapped(ctx, mtc.Store(0).TestSender(), &checkArgs); err != nil {
		t.Fatal(err)
	}

	state.Lock()
	defer state.Unlock()
	for applyKey, count := range state.applies {
		if count != 1 {
			t.Errorf("checksum %s was applied %d times to s%d (expected once)",
				applyKey.checksumID, count, applyKey.storeID)
		}
	}
}

func TestCheckConsistencyInconsistent(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sc := storage.TestStoreConfig(nil)
	mtc := &multiTestContext{
		storeConfig: &sc,
		// This test was written before the multiTestContext started creating many
		// system ranges at startup, and hasn't been update to take that into
		// account.
		startWithSingleRange: true,
	}

	const numStores = 3

	dir, cleanup := testutils.TempDir(t)
	defer cleanup()
	cache := engine.NewRocksDBCache(1 << 20)
	defer cache.Release()

	// Use on-disk stores because we want to take a RocksDB checkpoint and be
	// able to find it.
	for i := 0; i < numStores; i++ {
		eng, err := engine.NewRocksDB(engine.RocksDBConfig{
			StorageConfig: base.StorageConfig{
				Dir: filepath.Join(dir, fmt.Sprintf("%d", i)),
			},
		}, cache)
		if err != nil {
			t.Fatal(err)
		}
		defer eng.Close()
		mtc.engines = append(mtc.engines, eng)
	}

	// s1 will report a diff with inconsistent key "e", and only s2 has that
	// write (s3 agrees with s1).
	diffKey := []byte("e")
	var diffTimestamp hlc.Timestamp
	notifyReportDiff := make(chan struct{}, 1)
	sc.TestingKnobs.ConsistencyTestingKnobs.BadChecksumReportDiff =
		func(s roachpb.StoreIdent, diff storage.ReplicaSnapshotDiffSlice) {
			if s != *mtc.Store(0).Ident {
				t.Errorf("BadChecksumReportDiff called from follower (StoreIdent = %v)", s)
				return
			}
			if len(diff) != 1 {
				t.Errorf("diff length = %d, diff = %v", len(diff), diff)
				return
			}
			d := diff[0]
			if d.LeaseHolder || !bytes.Equal(diffKey, d.Key) || diffTimestamp != d.Timestamp {
				t.Errorf("diff = %v", d)
			}

			diff[0].Timestamp.Logical = 987 // mock this out for a consistent string below

			act := diff.String()

			exp := `--- leaseholder
+++ follower
+0.000000123,987 "e"
+    ts:1970-01-01 00:00:00.000000123 +0000 UTC
+    value:"\x00\x00\x00\x00\x01T"
+    raw mvcc_key/value: 6500000000000000007b000003db0d 000000000154
`
			if act != exp {
				// We already logged the actual one above.
				t.Errorf("expected:\n%s\ngot:\n%s", exp, act)
			}

			notifyReportDiff <- struct{}{}
		}
	// s2 (index 1) will panic.
	notifyFatal := make(chan struct{}, 1)
	sc.TestingKnobs.ConsistencyTestingKnobs.OnBadChecksumFatal = func(s roachpb.StoreIdent) {
		if s != *mtc.Store(1).Ident {
			t.Errorf("OnBadChecksumFatal called from %v", s)
			return
		}
		notifyFatal <- struct{}{}
	}

	defer mtc.Stop()
	mtc.Start(t, numStores)
	// Setup replication of range 1 on store 0 to stores 1 and 2.
	mtc.replicateRange(1, 1, 2)

	// Write something to the DB.
	pArgs := putArgs([]byte("a"), []byte("b"))
	if _, err := client.SendWrapped(context.Background(), mtc.stores[0].TestSender(), pArgs); err != nil {
		t.Fatal(err)
	}
	pArgs = putArgs([]byte("c"), []byte("d"))
	if _, err := client.SendWrapped(context.Background(), mtc.stores[0].TestSender(), pArgs); err != nil {
		t.Fatal(err)
	}

	runCheck := func() *roachpb.CheckConsistencyResponse {
		checkArgs := roachpb.CheckConsistencyRequest{
			RequestHeader: roachpb.RequestHeader{
				// span of keys that include "a" & "c".
				Key:    []byte("a"),
				EndKey: []byte("z"),
			},
			Mode: roachpb.ChecksumMode_CHECK_VIA_QUEUE,
		}
		resp, err := client.SendWrapped(context.Background(), mtc.stores[0].TestSender(), &checkArgs)
		if err != nil {
			t.Fatal(err)
		}
		return resp.(*roachpb.CheckConsistencyResponse)
	}

	checkpoints := func(nodeIdx int) []string {
		pat := filepath.Join(mtc.engines[nodeIdx].GetAuxiliaryDir(), "checkpoints") + "/*"
		m, err := filepath.Glob(pat)
		assert.NoError(t, err)
		return m
	}

	// Run the check the first time, it shouldn't find anything.
	respOK := runCheck()
	assert.Len(t, respOK.Result, 1)
	assert.Equal(t, roachpb.CheckConsistencyResponse_RANGE_CONSISTENT, respOK.Result[0].Status)
	select {
	case <-notifyReportDiff:
		t.Fatal("unexpected diff")
	case <-notifyFatal:
		t.Fatal("unexpected panic")
	default:
	}

	// No checkpoints should have been created.
	for i := 0; i < numStores; i++ {
		assert.Empty(t, checkpoints(i))
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

	// Run consistency check again, this time it should find something.
	resp := runCheck()

	select {
	case <-notifyReportDiff:
	case <-time.After(5 * time.Second):
		t.Fatal("CheckConsistency() failed to report a diff as expected")
	}
	select {
	case <-notifyFatal:
	case <-time.After(5 * time.Second):
		t.Fatal("CheckConsistency() failed to panic as expected")
	}

	// Checkpoints should have been created on all stores and they're not empty.
	for i := 0; i < numStores; i++ {
		cps := checkpoints(i)
		assert.Len(t, cps, 1)
		cpEng, err := engine.NewRocksDB(engine.RocksDBConfig{
			StorageConfig: base.StorageConfig{
				Dir: cps[0],
			},
		}, cache)
		assert.NoError(t, err)
		defer cpEng.Close()

		iter := cpEng.NewIterator(engine.IterOptions{UpperBound: []byte("\xff")})
		defer iter.Close()

		ms, err := engine.ComputeStatsGo(iter, roachpb.KeyMin, roachpb.KeyMax, 0 /* nowNanos */)
		assert.NoError(t, err)

		assert.NotZero(t, ms.KeyBytes)
	}

	assert.Len(t, resp.Result, 1)
	assert.Equal(t, roachpb.CheckConsistencyResponse_RANGE_INCONSISTENT, resp.Result[0].Status)
	assert.Contains(t, resp.Result[0].Detail, `[minority]`)
	assert.Contains(t, resp.Result[0].Detail, `stats`)

	// A death rattle should have been written on s2 (store index 1).
	b, err := ioutil.ReadFile(base.PreventedStartupFile(mtc.stores[1].Engine().GetAuxiliaryDir()))
	require.NoError(t, err)
	require.NotEmpty(t, b)
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
	testutils.RunTrueAndFalse(t, "hadEstimates", testConsistencyQueueRecomputeStatsImpl)
}

func testConsistencyQueueRecomputeStatsImpl(t *testing.T, hadEstimates bool) {
	ctx := context.Background()

	path, cleanup := testutils.TempDir(t)
	defer cleanup()

	// Set scanner timings that minimize waiting in this test.
	tsArgs := base.TestServerArgs{
		ScanInterval:    time.Second,
		ScanMinIdleTime: 0,
		ScanMaxIdleTime: 100 * time.Millisecond,
	}

	ccCh := make(chan roachpb.CheckConsistencyResponse, 1)
	knobs := &storage.StoreTestingKnobs{}
	knobs.ConsistencyTestingKnobs.ConsistencyQueueResultHook = func(resp roachpb.CheckConsistencyResponse) {
		if len(resp.Result) == 0 || resp.Result[0].Status != roachpb.CheckConsistencyResponse_RANGE_CONSISTENT_STATS_INCORRECT {
			// Ignore recomputations triggered by the time series ranges.
			return
		}
		select {
		case ccCh <- resp:
		default:
		}
	}
	tsArgs.Knobs.Store = knobs
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
			RequestHeader: roachpb.RequestHeader{Key: key},
			DryRun:        true,
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
		if err := db0.AdminSplit(ctx, key, key, hlc.MaxTimestamp /* expirationTime */); err != nil {
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
			StorageConfig: base.StorageConfig{
				Dir:       path,
				MustExist: true,
			},
		}, cache)
		if err != nil {
			t.Fatal(err)
		}
		defer eng.Close()

		rsl := stateloader.Make(rangeID)
		ms, err := rsl.LoadMVCCStats(ctx, eng)
		if err != nil {
			t.Fatal(err)
		}

		// Put some garbage in the stats that we're hoping the consistency queue will
		// trigger a removal of via RecomputeStats. SysCount was chosen because it is
		// not affected by the workload we run below and also does not influence the
		// GC queue score.
		ms.SysCount += sysCountGarbage
		ms.ContainsEstimates = 0
		if hadEstimates {
			ms.ContainsEstimates = 123
		}

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

	// Force a run of the consistency queue, otherwise it might take a while.
	ts := tc.Servers[0]
	store, pErr := ts.Stores().GetStore(ts.GetFirstStoreID())
	if pErr != nil {
		t.Fatal(pErr)
	}
	if err := store.ForceConsistencyQueueProcess(); err != nil {
		t.Fatal(err)
	}

	// The stats should magically repair themselves. We'll first do a quick check
	// and then a full recomputation.
	repl, err := ts.Stores().GetReplicaForRangeID(rangeID)
	if err != nil {
		t.Fatal(err)
	}
	ms := repl.GetMVCCStats()
	if ms.SysCount >= sysCountGarbage {
		t.Fatalf("still have a SysCount of %d", ms.SysCount)
	}

	if delta := computeDelta(db0); delta != (enginepb.MVCCStats{}) {
		t.Fatalf("stats still in need of adjustment: %+v", delta)
	}

	select {
	case resp := <-ccCh:
		assert.Contains(t, resp.Result[0].Detail, `KeyBytes`) // contains printed stats
		assert.Equal(t, roachpb.CheckConsistencyResponse_RANGE_CONSISTENT_STATS_INCORRECT, resp.Result[0].Status)
		assert.False(t, hadEstimates)
	default:
		assert.True(t, hadEstimates)
	}
}
