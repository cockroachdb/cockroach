// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"fmt"
	io "io"
	"math/rand"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConsistencyQueueRequiresLive verifies the queue will not
// process ranges whose replicas are not all live.
func TestConsistencyQueueRequiresLive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	manualClock := timeutil.NewManualTime(timeutil.Unix(0, 123))
	clock := hlc.NewClockForTesting(manualClock)
	interval := time.Second * 5
	live := true
	testStart := clock.Now()

	// Move time by the interval, so we run the job again.
	manualClock.Advance(interval)

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
// with three way replication. A consistency check is run on r1,
// which always has some data, and on a newly split off range
// as well.
func TestCheckConsistencyMultiStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
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
	store := tc.GetFirstStoreFromServer(t, 0)

	// Split off a range and write to it; we'll use it below but we test r1 first.
	k2 := tc.ScratchRange(t)
	{
		// Write something to k2.
		putArgs := putArgs(k2, []byte("b"))
		_, pErr := kv.SendWrapped(context.Background(), store.DB().NonTransactionalSender(), putArgs)
		require.NoError(t, pErr.GoError())
	}

	// 3x replicate our ranges.
	//
	// Depending on what we're doing, we need to use LocalMax or [R]KeyMin
	// for r1 due to the issue below.
	//
	// See: https://github.com/cockroachdb/cockroach/issues/95055
	tc.AddVotersOrFatal(t, roachpb.KeyMin, tc.Targets(1, 2)...)
	tc.AddVotersOrFatal(t, k2, tc.Targets(1, 2)...)

	// Run consistency check on r1 and ScratchRange.
	for _, k := range []roachpb.Key{keys.LocalMax, k2} {
		t.Run(k.String(), func(t *testing.T) {
			checkArgs := kvpb.CheckConsistencyRequest{
				RequestHeader: kvpb.RequestHeader{
					Key:    keys.LocalMax,
					EndKey: keys.LocalMax.Next(),
				},
			}
			_, pErr := kv.SendWrappedWith(context.Background(), store.DB().NonTransactionalSender(), kvpb.Header{
				Timestamp: store.Clock().Now(),
			}, &checkArgs)
			require.NoError(t, pErr.GoError())
		})
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
	testKnobs.TestingPostApplyFilter = func(args kvserverbase.ApplyFilterArgs) (int, *kvpb.Error) {
		state.Lock()
		defer state.Unlock()
		if ccr := args.ComputeChecksum; ccr != nil {
			state.applies[applyKey{ccr.ChecksumID, args.StoreID}]++
		}
		return 0, nil
	}

	// Arrange to trigger a retry when a ComputeChecksum request arrives.
	testKnobs.TestingResponseFilter = func(
		ctx context.Context, ba *kvpb.BatchRequest, br *kvpb.BatchResponse,
	) *kvpb.Error {
		state.Lock()
		defer state.Unlock()
		if ba.IsSingleComputeChecksumRequest() && !state.forcedRetry {
			state.forcedRetry = true
			// We need to return a retryable error from the perspective of the sender
			return kvpb.NewError(&kvpb.NotLeaseHolderError{})
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

	store := tc.GetFirstStoreFromServer(t, 0)
	checkArgs := kvpb.CheckConsistencyRequest{
		RequestHeader: kvpb.RequestHeader{
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

	// Test expects simple MVCC value encoding.
	storage.DisableMetamorphicSimpleValueEncoding(t)

	// Test uses sticky registry to have persistent pebble state that could
	// be analyzed for existence of snapshots and to verify snapshot content
	// after failures.
	stickyVFSRegistry := fs.NewStickyRegistry()

	// The cluster has 3 nodes, one store per node. The test writes a few KVs to a
	// range, which gets replicated to all 3 stores. Then it manually replaces an
	// entry in s2. The consistency check must detect this and terminate n2/s2.
	const numStores = 3
	testKnobs := kvserver.StoreTestingKnobs{DisableConsistencyQueue: true}
	var tc *testcluster.TestCluster

	notifyFatal := make(chan struct{})
	testKnobs.ConsistencyTestingKnobs.OnBadChecksumFatal = func(s roachpb.StoreIdent) {
		store := tc.GetFirstStoreFromServer(t, 1) // only s2 must terminate
		require.Equal(t, *store.Ident, s)
		close(notifyFatal)
	}

	serverArgsPerNode := make(map[int]base.TestServerArgs, numStores)
	for i := 0; i < numStores; i++ {
		serverArgsPerNode[i] = base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store:  &testKnobs,
				Server: &server.TestingKnobs{StickyVFSRegistry: stickyVFSRegistry},
			},
			StoreSpecs: []base.StoreSpec{{
				InMemory:    true,
				StickyVFSID: strconv.FormatInt(int64(i), 10),
			}},
		}
	}

	tc = testcluster.StartTestCluster(t, numStores, base.TestClusterArgs{
		ReplicationMode:   base.ReplicationAuto,
		ServerArgsPerNode: serverArgsPerNode,
	})
	defer tc.Stopper().Stop(context.Background())

	// Write something to the DB.
	store := tc.GetFirstStoreFromServer(t, 0)
	for k, v := range map[string]string{"a": "b", "c": "d"} {
		_, err := kv.SendWrapped(context.Background(), store.DB().NonTransactionalSender(),
			putArgs([]byte(k), []byte(v)))
		require.NoError(t, err.GoError())
	}

	runConsistencyCheck := func() *kvpb.CheckConsistencyResponse {
		req := kvpb.CheckConsistencyRequest{
			RequestHeader: kvpb.RequestHeader{ // keys span that includes "a" & "c"
				Key:    []byte("a"),
				EndKey: []byte("z"),
			},
			Mode: kvpb.ChecksumMode_CHECK_VIA_QUEUE,
		}
		resp, err := kv.SendWrapped(context.Background(), store.DB().NonTransactionalSender(), &req)
		require.NoError(t, err.GoError())
		return resp.(*kvpb.CheckConsistencyResponse)
	}

	onDiskCheckpointPaths := func(nodeIdx int) []string {
		fs := stickyVFSRegistry.Get(strconv.FormatInt(int64(nodeIdx), 10))
		store := tc.GetFirstStoreFromServer(t, nodeIdx)
		checkpointPath := filepath.Join(store.TODOEngine().GetAuxiliaryDir(), "checkpoints")
		checkpoints, _ := fs.List(checkpointPath)
		var checkpointPaths []string
		for _, cpDirName := range checkpoints {
			checkpointPaths = append(checkpointPaths, filepath.Join(checkpointPath, cpDirName))
		}
		return checkpointPaths
	}

	// Run the check the first time, it shouldn't find anything.
	resp := runConsistencyCheck()
	require.Len(t, resp.Result, 1)
	assert.Equal(t, kvpb.CheckConsistencyResponse_RANGE_CONSISTENT, resp.Result[0].Status)
	select {
	case <-notifyFatal:
		t.Fatal("unexpected panic")
	case <-time.After(time.Second): // the panic may come asynchronously
		// TODO(pavelkalinnikov): track the full lifecycle of the check in testKnobs
		// on each replica, to make checks more reliable. E.g., instead of waiting
		// for panic, ensure the task has started, wait until it exits, and only
		// then check whether it panicked.
	}
	// No checkpoints should have been created.
	for i := 0; i < numStores; i++ {
		assert.Empty(t, onDiskCheckpointPaths(i))
	}

	// Write some arbitrary data only to store on n2. Inconsistent key "e"!
	s2 := tc.GetFirstStoreFromServer(t, 1)
	s2AuxDir := s2.TODOEngine().GetAuxiliaryDir()
	var val roachpb.Value
	val.SetInt(42)
	// Put an inconsistent key "e" to s2, and have s1 and s3 still agree.
	_, err := storage.MVCCPut(context.Background(), s2.TODOEngine(),
		roachpb.Key("e"), tc.Server(0).Clock().Now(), val, storage.MVCCWriteOptions{})
	require.NoError(t, err)

	// Run consistency check again, this time it should find something.
	resp = runConsistencyCheck()
	select {
	case <-notifyFatal:
	case <-time.After(5 * time.Second):
		t.Fatal("CheckConsistency() failed to panic as expected")
	}

	require.Len(t, resp.Result, 1)
	assert.Equal(t, kvpb.CheckConsistencyResponse_RANGE_INCONSISTENT, resp.Result[0].Status)
	assert.Contains(t, resp.Result[0].Detail, `[minority]`)
	assert.Contains(t, resp.Result[0].Detail, `stats`)

	// Make sure that all the stores started creating a checkpoint. The metric
	// measures the number of checkpoint directories, but a directory can
	// represent an incomplete checkpoint that is still being populated.
	for i := 0; i < numStores; i++ {
		metric := tc.GetFirstStoreFromServer(t, i).Metrics().RdbCheckpoints
		testutils.SucceedsSoon(t, func() error {
			if got, want := metric.Value(), int64(1); got != want {
				return errors.Errorf("%s is %d, want %d", metric.Name, got, want)
			}
			return nil
		})
	}
	// As discussed in https://github.com/cockroachdb/cockroach/issues/81819, it
	// is possible that the check completes while there are still checkpoints in
	// flight. Waiting for the server termination makes sure that checkpoints are
	// fully created.
	tc.Stopper().Stop(context.Background())

	// Checkpoints should have been created on all stores.
	hashes := make([][]byte, numStores)
	for i := 0; i < numStores; i++ {
		cps := onDiskCheckpointPaths(i)
		require.Len(t, cps, 1)
		t.Logf("found a checkpoint at %s", cps[0])
		// The checkpoint must have been finalized.
		require.False(t, strings.HasSuffix(cps[0], "_pending"))

		// Create a new store on top of checkpoint location inside existing in-mem
		// VFS to verify its contents.
		ctx := context.Background()
		memFS := stickyVFSRegistry.Get(strconv.FormatInt(int64(i), 10))
		env, err := fs.InitEnv(ctx, memFS, cps[0], fs.EnvConfig{RW: fs.ReadOnly}, nil /* statsCollector */)
		require.NoError(t, err)
		cpEng, err := storage.Open(ctx, env, cluster.MakeClusterSettings(),
			storage.ForTesting, storage.MustExist, storage.CacheSize(1<<20))
		if err != nil {
			require.NoError(t, err)
		}
		defer cpEng.Close()

		// Find the problematic range in the storage.
		var desc *roachpb.RangeDescriptor
		require.NoError(t, kvstorage.IterateRangeDescriptorsFromDisk(context.Background(), cpEng,
			func(rd roachpb.RangeDescriptor) error {
				if rd.RangeID == resp.Result[0].RangeID {
					desc = &rd
				}
				return nil
			}))
		require.NotNil(t, desc)

		// Compute a checksum over the content of the problematic range.
		rd, err := kvserver.CalcReplicaDigest(context.Background(), *desc, cpEng,
			kvpb.ChecksumMode_CHECK_FULL, quotapool.NewRateLimiter("test", quotapool.Inf(), 0), nil /* settings */)
		require.NoError(t, err)
		hashes[i] = rd.SHA512[:]
	}

	assert.Equal(t, hashes[0], hashes[2])    // s1 and s3 agree
	assert.NotEqual(t, hashes[0], hashes[1]) // s2 diverged

	// A death rattle should have been written on s2. Note that the VFSes are
	// zero-indexed whereas store IDs are one-indexed.
	fs := stickyVFSRegistry.Get("1")
	f, err := fs.Open(base.PreventedStartupFile(s2AuxDir))
	require.NoError(t, err)
	b, err := io.ReadAll(f)
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

	ccCh := make(chan kvpb.CheckConsistencyResponse, 1)
	knobs := &kvserver.StoreTestingKnobs{}
	knobs.ConsistencyTestingKnobs.ConsistencyQueueResultHook = func(resp kvpb.CheckConsistencyResponse) {
		if len(resp.Result) == 0 || resp.Result[0].Status != kvpb.CheckConsistencyResponse_RANGE_CONSISTENT_STATS_INCORRECT {
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
		b.AddRawRequest(&kvpb.RecomputeStatsRequest{
			RequestHeader: kvpb.RequestHeader{Key: key},
			DryRun:        true,
		})
		require.NoError(t, db.Run(ctx, &b))
		resp := b.RawResponse().Responses[0].GetInner().(*kvpb.RecomputeStatsResponse)
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
		require.NoError(t, db0.AdminSplit(
			ctx,
			key,              /* splitKey */
			hlc.MaxTimestamp, /* expirationTime */
		))

		delta := computeDelta(db0)

		if delta != (enginepb.MVCCStats{}) {
			t.Fatalf("unexpected initial stats adjustment of %+v", delta)
		}

		rangeDesc, err := tc.LookupRange(key)
		require.NoError(t, err)

		return rangeDesc.RangeID
	}()

	const sysCountGarbage = 123000

	func() {
		eng, err := storage.Open(ctx,
			fs.MustInitPhysicalTestingEnv(path),
			cluster.MakeClusterSettings(),
			storage.CacheSize(1<<20 /* 1 MiB */),
			storage.MustExist)
		require.NoError(t, err)
		defer eng.Close()

		rsl := stateloader.Make(rangeID)
		ms, err := rsl.LoadMVCCStats(ctx, eng)
		require.NoError(t, err)

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
		require.NoError(t, rsl.SetMVCCStats(ctx, eng, &ms))
	}()

	// Now that we've tampered with the stats, restart the cluster and extend it
	// to three nodes.
	const numNodes = 3
	tc := testcluster.StartTestCluster(t, numNodes, clusterArgs)
	defer tc.Stopper().Stop(ctx)

	srv0 := tc.Servers[0]
	db0 := srv0.DB()

	// Run a goroutine that writes to the range in a tight loop. This tests that
	// RecomputeStats does not see any skew in its MVCC stats when they are
	// modified concurrently. Note that these writes don't interfere with the
	// field we modified (SysCount).
	//
	// We want to run this task under the cluster's stopper, as opposed to the
	// first node's stopper, so that the quiesce signal is delivered below before
	// individual nodes start shutting down.
	_ = tc.Stopper().RunAsyncTaskEx(ctx,
		stop.TaskOpts{
			TaskName: "recompute-loop",
		}, func(_ context.Context) {
			// This channel terminates the loop early if the test takes more than five
			// seconds. This is useful for stress race runs in CI where the tight loop
			// can starve the actual work to be done.
			done := time.After(5 * time.Second)
			for {
				// We're using context.Background for the KV call. As explained above,
				// this task runs on the cluster's stopper, and so the ctx that was
				// passed to this function has a span created with a Tracer that's
				// different from the first node's Tracer. If we used the ctx, we'd be
				// combining Tracers in the trace, which is illegal.
				require.NoError(t, db0.Put(context.Background(), fmt.Sprintf("%s%d", key, rand.Int63()), "ballast"))
				select {
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

	// When running with leader leases, it might take an extra election interval
	// for a lease to be established after adding the voters above because the
	// leader needs to get store liveness support from the followers. The stats
	// re-computation runs on the leaseholder and will fail if there isn't one.
	testutils.SucceedsSoon(t, func() error {
		// Force a run of the consistency queue, otherwise it might take a while.
		store := tc.GetFirstStoreFromServer(t, 0)
		require.NoError(t, store.ForceConsistencyQueueProcess())

		// The stats should magically repair themselves. We'll first do a quick check
		// and then a full recomputation.
		repl, _, err := tc.Servers[0].GetStores().(*kvserver.Stores).GetReplicaForRangeID(ctx, rangeID)
		require.NoError(t, err)
		ms := repl.GetMVCCStats()
		if ms.SysCount >= sysCountGarbage {
			return errors.Newf("still have a SysCount of %d", ms.SysCount)
		}
		return nil
	})

	if delta := computeDelta(db0); delta != (enginepb.MVCCStats{}) {
		t.Fatalf("stats still in need of adjustment: %+v", delta)
	}

	select {
	case resp := <-ccCh:
		assert.Contains(t, resp.Result[0].Detail, `KeyBytes`) // contains printed stats
		assert.Equal(t, kvpb.CheckConsistencyResponse_RANGE_CONSISTENT_STATS_INCORRECT, resp.Result[0].Status)
		assert.False(t, hadEstimates)
	default:
		assert.True(t, hadEstimates)
	}
}
