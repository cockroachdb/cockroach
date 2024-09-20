// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rafttrace"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/listenerutil"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

// TestRaftLogQueue verifies that the raft log queue correctly truncates the
// raft log.
func TestRaftLogQueue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Set maxBytes to something small so we can trigger the raft log truncation
	// without adding 64MB of logs.
	const maxBytes = 1 << 16

	zoneConfig := zonepb.DefaultZoneConfig()
	zoneConfig.RangeMaxBytes = proto.Int64(maxBytes)

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{
			ReplicationMode: base.ReplicationManual,
			ServerArgs: base.TestServerArgs{
				Knobs: base.TestingKnobs{
					Server: &server.TestingKnobs{
						DefaultZoneConfigOverride: &zoneConfig,
					},
				},
				RaftConfig: base.RaftConfig{
					// Turn off raft elections so the raft leader won't change out from under
					// us in this test.
					RaftTickInterval:         math.MaxInt32,
					RaftElectionTimeoutTicks: 1000000,
				},
			},
		})
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	key := tc.ScratchRange(t)
	tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)
	// Write a single value to ensure we have a leader.
	pArgs := putArgs(key, []byte("value"))
	if _, err := kv.SendWrapped(ctx, store.TestSender(), pArgs); err != nil {
		t.Fatal(err)
	}

	// Get the raft leader (and ensure one exists).
	raftLeaderRepl := tc.GetRaftLeader(t, roachpb.RKey(key))
	require.NotNil(t, raftLeaderRepl)
	originalIndex := raftLeaderRepl.GetFirstIndex()

	// Write a collection of values to increase the raft log.
	value := bytes.Repeat(key, 1000) // 1KB
	for size := int64(0); size < 2*maxBytes; size += int64(len(value)) {
		key = key.Next()
		pArgs = putArgs(key, value)
		if _, err := kv.SendWrapped(ctx, store.TestSender(), pArgs); err != nil {
			t.Fatal(err)
		}
	}

	var afterTruncationIndex kvpb.RaftIndex
	testutils.SucceedsSoon(t, func() error {
		// Force a truncation check.
		for i := range tc.Servers {
			tc.GetFirstStoreFromServer(t, i).MustForceRaftLogScanAndProcess()
		}
		// Flush the engine to advance durability, which triggers truncation.
		require.NoError(t, raftLeaderRepl.Store().TODOEngine().Flush())
		// Ensure that firstIndex has increased indicating that the log
		// truncation has occurred.
		afterTruncationIndex = raftLeaderRepl.GetFirstIndex()
		if afterTruncationIndex <= originalIndex {
			return errors.Errorf("raft log has not been truncated yet, afterTruncationIndex:%d originalIndex:%d",
				afterTruncationIndex, originalIndex)
		}
		return nil
	})

	// Force a truncation check again to ensure that attempting to truncate an
	// already truncated log has no effect. This check, unlike in the last
	// iteration, cannot use a succeedsSoon. This check is fragile in that the
	// truncation triggered here may lose the race against the call to
	// GetFirstIndex, giving a false negative. Fixing this requires additional
	// instrumentation of the queues, which was deemed to require too much work
	// at the time of this writing.
	for i := range tc.Servers {
		tc.GetFirstStoreFromServer(t, i).MustForceRaftLogScanAndProcess()
	}

	after2ndTruncationIndex := raftLeaderRepl.GetFirstIndex()
	if afterTruncationIndex > after2ndTruncationIndex {
		t.Fatalf("second truncation destroyed state: afterTruncationIndex:%d after2ndTruncationIndex:%d",
			afterTruncationIndex, after2ndTruncationIndex)
	}
}

func TestRaftTracing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// TODO(baptist): Remove this once we change the default to be enabled.
	st := cluster.MakeTestingClusterSettings()
	rafttrace.MaxConcurrentRaftTraces.Override(context.Background(), &st.SV, 10)

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Settings: st,
			RaftConfig: base.RaftConfig{
				RangeLeaseDuration:       24 * time.Hour, // disable lease moves
				RaftElectionTimeoutTicks: 1 << 30,        // disable elections
			},
		},
	})
	defer tc.Stopper().Stop(context.Background())
	store := tc.GetFirstStoreFromServer(t, 0)

	// Write a single value to ensure we have a leader on n1.
	key := tc.ScratchRange(t)
	_, pErr := kv.SendWrapped(context.Background(), store.TestSender(), putArgs(key, []byte("value")))
	require.NoError(t, pErr.GoError())
	require.NoError(t, tc.WaitForSplitAndInitialization(key))
	// Set to have 3 voters.
	tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)
	tc.WaitForVotersOrFatal(t, key, tc.Targets(1, 2)...)

	for i := 0; i < 100; i++ {
		var finish func() tracingpb.Recording
		ctx := context.Background()
		if i == 50 {
			// Trace a random request on a "client" tracer.
			ctx, finish = tracing.ContextWithRecordingSpan(ctx, store.GetStoreConfig().Tracer(), "test")
		}
		_, pErr := kv.SendWrapped(ctx, store.TestSender(), putArgs(key, []byte(fmt.Sprintf("value-%d", i))))
		require.NoError(t, pErr.GoError())
		// Note that this is the clients span, there may be additional logs created after the span is returned.
		if finish != nil {
			output := finish().String()
			// NB: It is hard to get all the messages in an expected order. We
			// simply ensure some of the key messages are returned. Also note
			// that we want to make sure that the logs are not reported against
			// the tracing library, but the line that called into it.
			expectedMessages := []string{
				`replica_proposal_buf.* flushing proposal to Raft`,
				`replica_proposal_buf.* registering local trace`,
				`replica_raft.* 1->2 MsgApp`,
				`replica_raft.* 1->3 MsgApp`,
				`replica_raft.* AppendThread->1 MsgStorageAppendResp`,
				`ack-ing replication success to the client`,
			}
			require.NoError(t, testutils.MatchInOrder(output, expectedMessages...))
		}
	}
}

// TestCrashWhileTruncatingSideloadedEntries emulates a process crash in the
// middle of applying a raft log truncation command that removes some entries
// from the sideloaded storage. The test expects that storage remains in a
// correct state, and the replica recovers and catches up after restart.
//
// This is a regression test for issues #38566 and #113135. Previously such a
// crash could invalidate storage, and lead to restart crash loops.
//
// The scenario is as follows:
//
//  1. Commit a few AddSST commands to raft (written to the sideloaded log
//     storage as individual files).
//  2. Commit a log truncation command.
//  3. Wait for the application of this command on a follower (it will remove
//     some files from the sideloaded storage).
//  4. Emulate the follower process crash (which discards all the storage engine
//     state that was not flushed).
//  5. However, make sure that the files removal is synced (because the
//     filesystem is still running after the crash).
//  6. Restart the follower process.
//  7. The follower recovers and catches up to the leader.
//
// Previously, steps 6-7 would crash loop because the application of AddSST and
// truncation commands to Pebble in (step 3) was not fully flushed/synced before
// the truncation command would remove files. After a restart, some suffix of
// AddSST commands would need to be replayed, but the files would be missing.
//
// This is now fixed: the application of the truncation command is synced before
// deleting the sideloaded files.
func TestCrashWhileTruncatingSideloadedEntries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Use sticky engine registry to "survive" a node restart. Use the strict
	// in-memory engine to be able to stop flushes and emulate data loss.
	vfsReg := fs.NewStickyRegistry(fs.UseStrictMemFS)
	// Use the sticky listener registry so that server port assignments survive
	// node restarts, and don't get erroneously used by other clusters.
	netReg := listenerutil.NewListenerRegistry()
	defer netReg.Close()
	// TODO(pavelkalinnikov): make sticky VFS and listeners the default.

	// Boilerplate to make the hooks dynamically changeable.
	propFilter := newAtomicFunc(func(kvserverbase.ProposalFilterArgs) *kvpb.Error {
		return nil
	})
	applyThrottle := newAtomicFunc(func(storage.FullReplicaID) {})
	postSideEffects := newAtomicFunc(func(args kvserverbase.ApplyFilterArgs) (int, *kvpb.Error) {
		return 0, nil
	})

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode:     base.ReplicationManual,
		ReusableListenerReg: netReg,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					DisableRaftLogQueue:     true, // we send a log truncation manually
					DisableSyncLogWriteToss: true, // always use async log writes
					TestingAfterRaftLogSync: func(id storage.FullReplicaID) { applyThrottle.get()(id) },
					TestingProposalFilter: func(args kvserverbase.ProposalFilterArgs) *kvpb.Error {
						return propFilter.get()(args)
					},
					TestingPostApplySideEffectsFilter: func(
						args kvserverbase.ApplyFilterArgs,
					) (int, *kvpb.Error) {
						return postSideEffects.get()(args)
					},
				},
				Server: &server.TestingKnobs{StickyVFSRegistry: vfsReg},
			},
			RaftConfig: base.RaftConfig{
				RangeLeaseDuration:       24 * time.Hour, // disable lease moves
				RaftElectionTimeoutTicks: 1 << 30,        // disable elections
				RaftProposalQuota:        1 << 30,        // unlimited proposals
			},
		},
	})
	ctx := context.Background()
	defer tc.Stopper().Stop(ctx)
	store := tc.GetFirstStoreFromServer(t, 0)

	// Write a single value to ensure we have a leader on n1.
	key := tc.ScratchRange(t)
	_, pErr := kv.SendWrapped(ctx, store.TestSender(), putArgs(key, []byte("value")))
	require.NoError(t, pErr.GoError())
	require.NoError(t, tc.WaitForSplitAndInitialization(key))
	// We need 3 voters so that stalling one follower does not block committing writes.
	tc.AddVotersOrFatal(t, key, tc.Targets(1, 2)...)
	tc.WaitForVotersOrFatal(t, key, tc.Targets(1, 2)...)

	// Get the raft leader (and ensure one exists).
	leader := tc.GetRaftLeader(t, roachpb.RKey(key))
	require.NotNil(t, leader)
	require.Equal(t, store.NodeID(), leader.NodeID())
	t.Logf("leader replica: %v", leader)
	// Get the follower on n2.
	follower, err := tc.GetFirstStoreFromServer(t, 1).GetReplica(leader.RangeID)
	require.NoError(t, err)
	require.NotNil(t, follower)
	t.Logf("follower replica: %v", follower)
	// Pin the leaseholder to the leader node (most likely it's already there).
	require.NoError(t, tc.TransferRangeLease(*leader.Desc(), tc.Target(0)))

	info := func(r *kvserver.Replica, name string) (kvpb.RaftIndex, kvpb.RaftIndex) {
		first, last := r.GetFirstIndex(), r.GetLastIndex()
		t.Logf("%s: log indices: [%d..%d]", name, first, last)
		t.Logf("%s: applied to: %d", name, r.State(ctx).ReplicaState.RaftAppliedIndex)
		return first, last
	}
	info(leader, "leader")
	info(follower, "follower")

	// Get the follower's file system.
	memFS := vfsReg.Get("auto-node2-store1")

	// Before writing more commands, block the raft commands application flow on
	// the follower replica.
	unblockApply := make(chan struct{})
	applyThrottle.set(func(id storage.FullReplicaID) {
		if id == follower.ID() {
			applyThrottle.reset()
			<-unblockApply
		}
	})

	// Write a few AddSST requests to increase the raft log.
	for i := 0; i < 20; i++ {
		_, pErr = kv.SendWrapped(ctx, store.TestSender(), makeAddSST(t, store.ClusterSettings(), key, 10))
		require.NoError(t, pErr.GoError())
	}
	t.Log("committed AddSSTs")
	_, lastIndex := info(leader, "leader")
	info(follower, "follower")

	// Trigger raft log truncation. When the corresponding command is proposed,
	// record its ID. Use this ID to watch other events related to this command,
	// in particular we're interested to catch the moment when the command has
	// been applied at the follower replica.
	//
	// TODO(#115759): group proposal lifecycle callbacks into a convenient
	// watcher, so that each test doesn't have to write ad-hoc things like this.
	var cmdID atomicValue[kvserverbase.CmdIDKey]
	propFilter.set(func(args kvserverbase.ProposalFilterArgs) *kvpb.Error {
		if _, ok := args.Req.GetArg(kvpb.TruncateLog); !ok {
			return nil
		}
		propFilter.reset()
		cmdID.set(args.CmdID)
		return nil
	})
	_, pErr = kv.SendWrapped(ctx, store.TestSender(), &kvpb.TruncateLogRequest{
		RequestHeader:      kvpb.RequestHeader{Key: key},
		Index:              lastIndex - 1, // truncate all but the last AddSST
		RangeID:            leader.RangeID,
		ExpectedFirstIndex: 0,
	})
	require.NoError(t, pErr.GoError())
	require.NotEmpty(t, cmdID.get(), "truncation command ID not captured")
	t.Logf("committed truncation for indices < %d", lastIndex-1)
	info(leader, "leader")
	info(follower, "follower")

	// Catch when the truncation command and its side effects have been applied.
	truncateApplied := make(chan struct{})
	postSideEffects.set(func(args kvserverbase.ApplyFilterArgs) (int, *kvpb.Error) {
		if args.StoreID != follower.StoreID() || args.CmdID != cmdID.get() {
			return 0, nil
		}
		postSideEffects.reset()
		// Assume that the filesystem will live longer than the process, and will
		// eventually sync the sideloaded storage.
		require.NoError(t, follower.SideloadedRaftMuLocked().Sync())
		close(truncateApplied)
		return 0, nil
	})
	// Unblock the command application flow on the follower replica, and wait
	// until the truncation command has applied.
	close(unblockApply)
	t.Log("unblocked follower application flow")
	<-truncateApplied
	t.Log("follower applied the truncation")

	// Emulate process crash at this point.
	//
	//	1. First, block the outgoing RPC traffic.
	//	2. Then capture the storage state and start ignoring all the syncs.
	//	3. Turn down the follower node.
	//
	// Without step 1, a flake is possible and has been observed while writing
	// this test. Between steps 2 and 3, the follower may persist a log entry and
	// send an ack to leader thinking that it's durable. The leader now, too,
	// thinks that it's durable, and may a) commit this entry, and b) send a
	// commit index advancement to the follower. If (b) happens after the follower
	// restarted and lost the last entry, it will panic because commit index the
	// leader sent is now above the last index in the log.
	for _, peer := range []int{0, 2} { // the leader and the other follower
		dialer := tc.Servers[1].NodeDialer().(*nodedialer.Dialer)
		for c := 0; c < rpc.NumConnectionClasses; c++ {
			brk, found := dialer.GetCircuitBreaker(tc.Servers[peer].NodeID(), rpc.ConnectionClass(c))
			if found {
				brk.Report(errors.New("connection is terminated by the test"))
			}
		}
	}
	crashFS := memFS.CrashClone(vfs.CrashCloneCfg{})
	info(follower, "follower")
	t.Log("CRASH!")
	// TODO(pavelkalinnikov): add "crash" helpers to the TestCluster.
	tc.StopServer(1)

	t.Log("restarting follower")
	vfsReg.Set("auto-node2-store1", crashFS)
	t.Logf("FS after restart:\n%s", crashFS.String())
	require.NoError(t, tc.RestartServer(1))

	// Update the follower variable to point at a newly restarted replica.
	follower, err = tc.GetFirstStoreFromServer(t, 1).GetReplica(leader.RangeID)
	require.NoError(t, err)
	require.NotNil(t, follower)
	info(follower, "follower")

	// We still should be able to write.
	_, pErr = kv.SendWrapped(ctx, store.TestSender(), putArgs(key, []byte("another value")))
	require.NoError(t, pErr.GoError())
	// The follower replica should catch up to leader.
	leaderLAI := leader.State(ctx).ReplicaState.LeaseAppliedIndex
	t.Logf("leader LAI %d", leaderLAI)
	testutils.SucceedsSoon(t, func() error {
		if lai := follower.State(ctx).ReplicaState.LeaseAppliedIndex; lai < leaderLAI {
			return fmt.Errorf("follower still catching up from LAI %d to %d", lai, leaderLAI)
		}
		return nil
	})
}

func makeAddSST(
	t *testing.T, st *cluster.Settings, key roachpb.Key, values int,
) *kvpb.AddSSTableRequest {
	kvs := make(storageutils.KVs, values)
	for i := range kvs {
		k := testutils.MakeKey(key, []byte(fmt.Sprintf("%d", i)))
		kvs[i] = storageutils.PointKV(string(k), 1, "value")
	}
	sst, start, end := storageutils.MakeSST(t, st, kvs)
	return &kvpb.AddSSTableRequest{
		RequestHeader: kvpb.RequestHeader{Key: start, EndKey: end},
		Data:          sst,
		MVCCStats:     storageutils.SSTStats(t, sst, 0),
	}
}

type atomicValue[T any] atomic.Value

func newAtomicValue[T any](v T) atomicValue[T] {
	var ret atomicValue[T]
	ret.set(v)
	return ret
}

func (a *atomicValue[T]) get() T {
	return (*atomic.Value)(a).Load().(T)
}

func (a *atomicValue[T]) set(v T) {
	(*atomic.Value)(a).Store(v)
}

type atomicFunc[T any] struct {
	noop T
	atomicValue[T]
}

func newAtomicFunc[T any](fn T) atomicFunc[T] {
	return atomicFunc[T]{
		noop:        fn,
		atomicValue: newAtomicValue(fn),
	}
}

func (a *atomicFunc[T]) reset() {
	a.set(a.noop)
}
