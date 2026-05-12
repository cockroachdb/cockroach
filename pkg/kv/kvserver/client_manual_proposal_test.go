// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/uncertainty"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
)

// TestCreateManyUnappliedProbes is a (by default skipped) test that writes
// a very large unapplied raft log consisting entirely of probes.
//
// It's a toy example for #75729 but has been useful to validate improvements
// in the raft pipeline, so it is checked in to allow for future re-use for
// similar purposes.
//
// See also: https://github.com/cockroachdb/cockroach/issues/105177
func TestCreateManyUnappliedProbes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	// NB: the actual logic takes only milliseconds, but we need to start/stop a
	// test cluster twice and that is slow.
	skip.UnderShort(t, "takes ~4s")

	ctx := context.Background()

	// NB: To set up a "real" cockroach-data dir with a large raft log in
	// system.settings, and then restart the node to watch it apply the long raft
	// log, you can use the below:
	//
	//    p := os.ExpandEnv("$HOME/go/src/github.com/cockroachdb/cockroach/cockroach-data")
	//    const entsPerBatch = 100000
	//    const batches = 1000
	//
	//   ./dev build && rm -rf cockroach-data && timeout 10 ./cockroach start-single-node --logtostderr --insecure ; \
	//   go test ./pkg/kv/kvserver/ -v --run TestCreateManyUnappliedProbes && sleep 3 && \
	//   (./cockroach start-single-node --logtostderr=INFO --insecure | grep -F r10/)
	//
	// Then wait and watch the `raft.commandsapplied` metric to see r10 apply the entries.
	stateEnginePath := filepath.Join(t.TempDir(), "cockroach-data")
	// logEnginePath is the on-disk path of the store's log engine. In
	// single-engine mode it equals p; in separated mode it points at the
	// log-engine directory.
	var logEnginePath string
	const entsPerBatch = 10
	const batches = 3
	rangeID := roachpb.RangeID(10) // system.settings

	if _, err := os.Stat(stateEnginePath); err != nil {
		args := base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{StoreSpecs: []base.StoreSpec{
				{Path: stateEnginePath},
			}},
			ReplicationMode: base.ReplicationManual,
		}
		tc := testcluster.StartTestCluster(t, 1, args)
		// Reload system.settings' rangeID just in case it changes.
		require.NoError(t, tc.ServerConn(0).QueryRow(`SELECT
	range_id
FROM
	[SHOW RANGES FROM TABLE system.settings]
ORDER BY
	range_id ASC
LIMIT
	1;`).Scan(&rangeID))
		logEnginePath = tc.GetFirstStoreFromServer(t, 0).LogEngine().Env().Dir
		tc.Stopper().Stop(ctx)

		defer func() {
			if t.Failed() {
				return
			}
			restartTC := testcluster.StartTestCluster(t, 1, args)
			defer restartTC.Stopper().Stop(ctx)
			require.NoError(t, restartTC.ServerConn(0).QueryRow(`SELECT count(1) FROM system.settings`).Err())
			t.Log("read system.settings")
		}()
	}

	st := cluster.MakeTestingClusterSettings()
	stateEng, err := storage.Open(ctx, fs.MustInitPhysicalTestingEnv(stateEnginePath), st)
	require.NoError(t, err)
	defer stateEng.Close()

	// If engines are separated, open the logEng. If engines are not separated,
	// logEng is the same as stateEng.
	logEng := stateEng
	if logEnginePath != "" && logEnginePath != stateEnginePath {
		logEng, err = storage.Open(ctx, fs.MustInitPhysicalTestingEnv(logEnginePath), st)
		require.NoError(t, err)
		defer logEng.Close()
	}

	// Determine LastIndex, LastTerm, and next MaxLeaseIndex by scanning
	// existing log.
	it, err := raftlog.NewIterator(ctx, rangeID, logEng, raftlog.IterOptions{})
	require.NoError(t, err)
	defer it.Close()
	rsl := logstore.NewStateLoader(rangeID)
	ts, err := rsl.LoadRaftTruncatedState(ctx, logEng)
	require.NoError(t, err)
	lastEntryID, err := rsl.LoadLastEntryID(ctx, logEng, ts)
	require.NoError(t, err)
	t.Logf("loaded LastEntryID: %+v", lastEntryID)
	lastIndex := lastEntryID.Index
	ok, err := it.SeekGE(lastIndex)
	require.NoError(t, err)
	require.True(t, ok)

	var lai kvpb.LeaseAppliedIndex
	var lastTerm uint64
	require.NoError(t, raftlog.Visit(
		ctx, logEng, rangeID, lastIndex, math.MaxUint64, func(entry raftpb.Entry) error {
			ent, err := raftlog.NewEntry(it.Entry())
			require.NoError(t, err)
			if lai < ent.Cmd.MaxLeaseIndex {
				lai = ent.Cmd.MaxLeaseIndex
			}
			lastTerm = ent.Term
			return nil
		}))

	sl := kvstorage.MakeStateLoader(rangeID)
	lease, err := sl.LoadLease(ctx, stateEng)
	require.NoError(t, err)

	for batchIdx := 0; batchIdx < batches; batchIdx++ {
		t.Logf("batch %d", batchIdx+1)
		b := storage.NewOpLoggerBatch(stateEng.NewBatch())
		defer b.Batch.Close()

		var ents []raftpb.Entry
		for i := 0; i < entsPerBatch; i++ {
			lai++
			c := &kvpb.ProbeRequest{}
			resp := &kvpb.ProbeResponse{}
			c.Key = keys.LocalMax

			cmd, ok := batcheval.LookupCommand(c.Method())
			require.True(t, ok)
			// NB: this should really operate on a BatchRequest. We need to librarize
			// evaluateBatch or its various callers.

			evalCtx := &batcheval.MockEvalCtx{}
			args := batcheval.CommandArgs{
				EvalCtx:     evalCtx.EvalContext(),
				Header:      kvpb.Header{},
				Args:        c,
				Stats:       &enginepb.MVCCStats{},
				Uncertainty: uncertainty.Interval{},
			}
			res, err := cmd.EvalRW(ctx, b, args, resp)
			require.NoError(t, err)
			// NB: ideally evaluateProposal could be used directly here;
			// we just cobble a `result.Result` together manually in this test.
			res.WriteBatch = &kvserverpb.WriteBatch{Data: b.Repr()}
			res.LogicalOpLog = &kvserverpb.LogicalOpLog{Ops: b.LogicalOps()}

			// End of evaluation. Start of "proposing". This too is all cobbled
			// together here and ideally we'd be able to call a method that is
			// also invoked by the production code paths. For example, there is
			// no real lease handling here so you can't use this code to propose
			// a {Request,Transfer}LeaseRequest; we'd need [^1].
			//
			// [^1]: https://github.com/cockroachdb/cockroach/blob/9a7b735b1282bbb3fb7472cc26a47d516a446958/pkg/kv/kvserver/replica_raft.go#L192-L219
			raftCmd := kvserverpb.RaftCommand{
				ProposerLeaseSequence: lease.Sequence,
				MaxLeaseIndex:         lai,
				// Rest was determined by evaluation.
				ReplicatedEvalResult: res.Replicated,
				WriteBatch:           res.WriteBatch,
				LogicalOpLog:         res.LogicalOpLog,
			}

			idKey := raftlog.MakeCmdIDKey()
			payload, err := raftlog.EncodeCommand(ctx, &raftCmd, idKey, raftlog.EncodeOptions{})
			require.NoError(t, err)
			ents = append(ents, raftpb.Entry{
				Term:  lastTerm,
				Index: uint64(lastIndex) + uint64(len(ents)) + 1,
				Type:  raftpb.EntryNormal,
				Data:  payload,
			})
		}

		stats := &logstore.AppendStats{}

		app := raft.StorageAppend{
			HardState: raftpb.HardState{
				Term:   lastTerm,
				Commit: uint64(lastIndex) + uint64(len(ents)),
			},
			Entries:   ents,
			LeadTerm:  lastTerm,
			Responses: []raftpb.Message{{}}, // need >0 responses so StoreEntries will sync
		}

		swl := logstore.NewSyncWaiterLoop()
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		swl.Start(ctx, stopper)
		ls := logstore.LogStore{
			RangeID:     rangeID,
			Engine:      logEng,
			Sideload:    nil,
			StateLoader: rsl,
			SyncWaiter:  swl,
			Settings:    st,
		}

		wg := &sync.WaitGroup{}
		wg.Add(1)
		_, err = ls.StoreEntries(ctx, logstore.RaftState{
			LastIndex: lastIndex,
			LastTerm:  kvpb.RaftTerm(lastTerm),
		}, app, (*wgSyncCallback)(wg), stats)
		require.NoError(t, err)
		wg.Wait()

		lastIndex = kvpb.RaftIndex(ents[len(ents)-1].Index)
	}

	t.Logf("LastIndex is now: %d", lastIndex)
}

type wgSyncCallback sync.WaitGroup

func (w *wgSyncCallback) OnLogSync(context.Context, raft.StorageAppendAck, logstore.WriteStats) {
	(*sync.WaitGroup)(w).Done()
}
