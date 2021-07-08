// Copyright 2016 The Cockroach Authors.
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
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConsistencyQueueRequiresLive verifies the queue will not
// process ranges whose replicas are not all live.
func TestConsistencyQueueRequiresLive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	manualClock := hlc.NewManualClock(timeutil.Now().UnixNano())
	clock := hlc.NewClock(manualClock.UnixNano, 10)
	interval := time.Second * 5
	live := true
	testStart := clock.Now()

	// Move time by the interval, so we run the job again.
	manualClock.Increment(interval.Nanoseconds())

	desc := &roachpb.RangeDescriptor{
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 1, ReplicaID: 1},
			{NodeID: 2, StoreID: 1, ReplicaID: 2},
			{NodeID: 3, StoreID: 1, ReplicaID: 3},
		},
	}

	getQueueLastProcessed := func(ctx context.Context) (hlc.Timestamp, error) {
		return testStart, nil
	}

	isNodeAvailable := func(nodeID roachpb.NodeID) bool {
		return live
	}

	if shouldQ, priority := kvserver.ConsistencyQueueShouldQueue(
		context.Background(), clock.NowAsClockTimestamp(), desc, getQueueLastProcessed, isNodeAvailable,
		false, interval); !shouldQ {
		t.Fatalf("expected shouldQ true; got %t, %f", shouldQ, priority)
	}

	live = false

	if shouldQ, priority := kvserver.ConsistencyQueueShouldQueue(
		context.Background(), clock.NowAsClockTimestamp(), desc, getQueueLastProcessed, isNodeAvailable,
		false, interval); shouldQ {
		t.Fatalf("expected shouldQ false; got %t, %f", shouldQ, priority)
	}
}

// TestCheckConsistencyMultiStore creates a node with three stores
// with three way replication. A value is added to the node, and a
// consistency check is run.
func TestCheckConsistencyMultiStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &kvserver.StoreTestingKnobs{
						DisableConsistencyQueue: true,
					},
				},
			},
		},
	)

	defer tc.Stopper().Stop(context.Background())
	ts := tc.Servers[0]
	store, pErr := ts.Stores().GetStore(ts.GetFirstStoreID())
	if pErr != nil {
		t.Fatal(pErr)
	}

	// Write something to the DB.
	putArgs := putArgs([]byte("a"), []byte("b"))
	if _, err := kv.SendWrapped(context.Background(), store.DB().NonTransactionalSender(), putArgs); err != nil {
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
	if _, err := kv.SendWrappedWith(context.Background(), store.DB().NonTransactionalSender(), roachpb.Header{
		Timestamp: store.Clock().Now(),
	}, &checkArgs); err != nil {
		t.Fatal(err)
	}
}

// TestCheckConsistencyReplay verifies that two ComputeChecksum requests with
// the same checksum ID are not committed to the Raft log, even if DistSender
// retries the request.
func TestCheckConsistencyReplay(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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

	testKnobs := kvserver.StoreTestingKnobs{
		DisableConsistencyQueue: true,
	}
	// Arrange to count the number of times each checksum command applies to each
	// store.
	testKnobs.TestingApplyFilter = func(args kvserverbase.ApplyFilterArgs) (int, *roachpb.Error) {
		state.Lock()
		defer state.Unlock()
		if ccr := args.ComputeChecksum; ccr != nil {
			state.applies[applyKey{ccr.ChecksumID, args.StoreID}]++
		}
		return 0, nil
	}

	// Arrange to trigger a retry when a ComputeChecksum request arrives.
	testKnobs.TestingResponseFilter = func(
		ctx context.Context, ba roachpb.BatchRequest, br *roachpb.BatchResponse,
	) *roachpb.Error {
		state.Lock()
		defer state.Unlock()
		if ba.IsSingleComputeChecksumRequest() && !state.forcedRetry {
			state.forcedRetry = true
			// We need to return a retryable error from the perspective of the sender
			return roachpb.NewError(&roachpb.NotLeaseHolderError{})
		}
		return nil
	}

	tc := testcluster.StartTestCluster(t, 2,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationAuto,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Store: &testKnobs,
				},
			},
		},
	)

	defer tc.Stopper().Stop(context.Background())

	ts := tc.Servers[0]
	store, pErr := ts.Stores().GetStore(ts.GetFirstStoreID())
	if pErr != nil {
		t.Fatal(pErr)
	}
	checkArgs := roachpb.CheckConsistencyRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    []byte("a"),
			EndKey: []byte("b"),
		},
	}

	if _, err := kv.SendWrapped(context.Background(), store.DB().NonTransactionalSender(), &checkArgs); err != nil {
		t.Fatal(err)
	}
	// Check that the request was evaluated twice (first time when forcedRetry was
	// set, and a 2nd time such that kv.SendWrapped() returned success).
	require.True(t, state.forcedRetry)

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
	defer log.Scope(t).Close(t)

	// This test prints a consistency checker diff, so it's
	// good to make sure we're overly redacting said diff.
	defer log.TestingSetRedactable(true)()

	// Test uses sticky registry to have persistent pebble state that could
	// be analyzed for existence of snapshots and to verify snapshot content
	// after failures.
	stickyEngineRegistry := server.NewStickyInMemEnginesRegistry()
	defer stickyEngineRegistry.CloseAllStickyInMemEngines()

	const numStores = 3
	testKnobs := kvserver.StoreTestingKnobs{
		DisableConsistencyQueue: true,
	}

	var tc *testcluster.TestCluster

	// s1 will report a diff with inconsistent key "e", and only s2 has that
	// write (s3 agrees with s1).
	diffKey := []byte("e")
	var diffTimestamp hlc.Timestamp
	notifyReportDiff := make(chan struct{}, 1)
	testKnobs.ConsistencyTestingKnobs.BadChecksumReportDiff =
		func(s roachpb.StoreIdent, diff kvserver.ReplicaSnapshotDiffSlice) {
			rangeDesc := tc.LookupRangeOrFatal(t, diffKey)
			repl, pErr := tc.FindRangeLeaseHolder(rangeDesc, nil)
			if pErr != nil {
				t.Fatal(pErr)
			}
			// Servers start at 0, but NodeID starts at 1.
			store, pErr := tc.Servers[repl.NodeID-1].Stores().GetStore(repl.StoreID)
			if pErr != nil {
				t.Fatal(pErr)
			}
			if s != *store.Ident {
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

			// mock this out for a consistent string below.
			diff[0].Timestamp = hlc.Timestamp{Logical: 987, WallTime: 123}

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
	testKnobs.ConsistencyTestingKnobs.OnBadChecksumFatal = func(s roachpb.StoreIdent) {
		ts := tc.Servers[1]
		store, pErr := ts.Stores().GetStore(ts.GetFirstStoreID())
		if pErr != nil {
			t.Fatal(pErr)
		}
		if s != *store.Ident {
			t.Errorf("OnBadChecksumFatal called from %v", s)
			return
		}
		notifyFatal <- struct{}{}
	}

	serverArgsPerNode := make(map[int]base.TestServerArgs)
	for i := 0; i < numStores; i++ {
		testServerArgs := base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &testKnobs,
				Server: &server.TestingKnobs{
					StickyEngineRegistry: stickyEngineRegistry,
				},
			},
			StoreSpecs: []base.StoreSpec{
				{
					InMemory:               true,
					StickyInMemoryEngineID: strconv.FormatInt(int64(i), 10),
				},
			},
		}
		serverArgsPerNode[i] = testServerArgs
	}

	tc = testcluster.StartTestCluster(t, numStores,
		base.TestClusterArgs{
			ReplicationMode:   base.ReplicationAuto,
			ServerArgsPerNode: serverArgsPerNode,
		},
	)
	defer tc.Stopper().Stop(context.Background())

	ts := tc.Servers[0]
	store, pErr := ts.Stores().GetStore(ts.GetFirstStoreID())
	if pErr != nil {
		t.Fatal(pErr)
	}
	// Write something to the DB.
	pArgs := putArgs([]byte("a"), []byte("b"))
	if _, err := kv.SendWrapped(context.Background(), store.DB().NonTransactionalSender(), pArgs); err != nil {
		t.Fatal(err)
	}
	pArgs = putArgs([]byte("c"), []byte("d"))
	if _, err := kv.SendWrapped(context.Background(), store.DB().NonTransactionalSender(), pArgs); err != nil {
		t.Fatal(err)
	}

	runConsistencyCheck := func() *roachpb.CheckConsistencyResponse {
		checkArgs := roachpb.CheckConsistencyRequest{
			RequestHeader: roachpb.RequestHeader{
				// span of keys that include "a" & "c".
				Key:    []byte("a"),
				EndKey: []byte("z"),
			},
			Mode: roachpb.ChecksumMode_CHECK_VIA_QUEUE,
		}
		resp, err := kv.SendWrapped(context.Background(), store.DB().NonTransactionalSender(), &checkArgs)
		if err != nil {
			t.Fatal(err)
		}
		return resp.(*roachpb.CheckConsistencyResponse)
	}

	onDiskCheckpointPaths := func(nodeIdx int) []string {
		testServer := tc.Servers[nodeIdx]
		fs, pErr := stickyEngineRegistry.GetUnderlyingFS(
			base.StoreSpec{StickyInMemoryEngineID: strconv.FormatInt(int64(nodeIdx), 10)})
		if pErr != nil {
			t.Fatal(pErr)
		}
		testStore, pErr := testServer.Stores().GetStore(testServer.GetFirstStoreID())
		if pErr != nil {
			t.Fatal(pErr)
		}
		checkpointPath := filepath.Join(testStore.Engine().GetAuxiliaryDir(), "checkpoints")
		checkpoints, _ := fs.List(checkpointPath)
		var checkpointPaths []string
		for _, cpDirName := range checkpoints {
			checkpointPaths = append(checkpointPaths, filepath.Join(checkpointPath, cpDirName))
		}
		return checkpointPaths
	}

	// Run the check the first time, it shouldn't find anything.
	respOK := runConsistencyCheck()
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
		assert.Empty(t, onDiskCheckpointPaths(i))
	}

	// Write some arbitrary data only to store 1. Inconsistent key "e"!
	ts1 := tc.Servers[1]
	store1, pErr := ts1.Stores().GetStore(ts1.GetFirstStoreID())
	if pErr != nil {
		t.Fatal(pErr)
	}
	var val roachpb.Value
	val.SetInt(42)
	diffTimestamp = ts.Clock().Now()
	if err := storage.MVCCPut(
		context.Background(), store1.Engine(), nil, diffKey, diffTimestamp, val, nil,
	); err != nil {
		t.Fatal(err)
	}

	// Run consistency check again, this time it should find something.
	resp := runConsistencyCheck()

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
		cps := onDiskCheckpointPaths(i)
		assert.Len(t, cps, 1)

		// Create a new store on top of checkpoint location inside existing in mem
		// VFS to verify its contents.
		fs, err := stickyEngineRegistry.GetUnderlyingFS(base.StoreSpec{StickyInMemoryEngineID: strconv.FormatInt(int64(i), 10)})
		assert.NoError(t, err)
		cpEng := storage.InMemFromFS(context.Background(), roachpb.Attributes{}, 1<<20, 0, fs, cps[0], nil)
		defer cpEng.Close()

		iter := cpEng.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{UpperBound: []byte("\xff")})
		defer iter.Close()

		// The range is specified using only global keys, since the implementation
		// may use an intentInterleavingIter.
		ms, err := storage.ComputeStatsForRange(iter, keys.LocalMax, roachpb.KeyMax, 0 /* nowNanos */)
		assert.NoError(t, err)

		assert.NotZero(t, ms.KeyBytes)
	}

	assert.Len(t, resp.Result, 1)
	assert.Equal(t, roachpb.CheckConsistencyResponse_RANGE_INCONSISTENT, resp.Result[0].Status)
	assert.Contains(t, resp.Result[0].Detail, `[minority]`)
	assert.Contains(t, resp.Result[0].Detail, `stats`)

	// A death rattle should have been written on s2 (store index 1).
	eng := store1.Engine()
	f, err := eng.Open(base.PreventedStartupFile(eng.GetAuxiliaryDir()))
	require.NoError(t, err)
	b, err := ioutil.ReadAll(f)
	require.NoError(t, err)
	require.NoError(t, f.Close())
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
	defer log.Scope(t).Close(t)
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
	knobs := &kvserver.StoreTestingKnobs{}
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

	computeDelta := func(db *kv.DB) enginepb.MVCCStats {
		var b kv.Batch
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
		defer tc.Stopper().Stop(context.Background())

		db0 := tc.Servers[0].DB()

		// Split off a range so that we get away from the timeseries writes, which
		// pollute the stats with ContainsEstimates=true. Note that the split clears
		// the right hand side (which is what we operate on) from that flag.
		if err := db0.AdminSplit(ctx, key, hlc.MaxTimestamp /* expirationTime */); err != nil {
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
		cache := pebble.NewCache(1 << 20)
		defer cache.Unref()
		opts := storage.DefaultPebbleOptions()
		opts.Cache = cache
		eng, err := storage.NewPebble(ctx, storage.PebbleConfig{
			StorageConfig: base.StorageConfig{
				Dir:       path,
				MustExist: true,
			},
			Opts: opts,
		})
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
	_ = tc.Stopper().RunAsyncTask(ctx, "recompute-loop", func(ctx context.Context) {
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
	if _, err := tc.AddVoters(key, targets...); err != nil {
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
	repl, _, err := ts.Stores().GetReplicaForRangeID(ctx, rangeID)
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
