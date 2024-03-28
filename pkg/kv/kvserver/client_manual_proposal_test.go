// Copyright 2023 The Cockroach Authors.
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
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftentry"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/uncertainty"
	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/pebble/vfs"
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
	p := filepath.Join(t.TempDir(), "cockroach-data")
	const entsPerBatch = 10
	const batches = 3
	rangeID := roachpb.RangeID(10) // system.settings

	if _, err := os.Stat(p); err != nil {
		args := base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{StoreSpecs: []base.StoreSpec{
				{Path: p},
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
		tc.Stopper().Stop(ctx)

		defer func() {
			if t.Failed() {
				return
			}
			tc := testcluster.StartTestCluster(t, 1, args)
			defer tc.Stopper().Stop(ctx)
			require.NoError(t, tc.ServerConn(0).QueryRow(`SELECT count(1) FROM system.settings`).Err())
			t.Log("read system.settings")
		}()

	}

	st := cluster.MakeTestingClusterSettings()
	eng, err := storage.Open(ctx, fs.MustInitPhysicalTestingEnv(p), st)
	require.NoError(t, err)
	defer eng.Close()

	appendSomeStuff(ctx, t, st, eng, rangeID, batches, entsPerBatch)
}

// TestCreateManyUnappliedEntriesAcrossRanges is a (by default skipped) test
// that writes a very large unapplied raft log across many ranges. When a node
// is back up, it tries to apply these entries, and may pull a lot of entries
// into memory. This test helps to exercise the memory budgeting strategy.
//
// See: https://github.com/cockroachdb/cockroach/issues/102840
func TestCreateManyUnappliedEntriesAcrossRanges(t *testing.T) {
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
	//   ./workload init kv --splits=50 'postgresql://root@localhost:26257?sslmode=disable' ; \
	//   go test ./pkg/kv/kvserver/ -v --run TestCreateManyUnappliedProbes && sleep 3 && \
	//   (./cockroach start-single-node --logtostderr=INFO --insecure | grep -F r10/)
	//
	// Then wait and watch the `raft.commandsapplied` metric to see r10 apply the entries.
	// p := filepath.Join(t.TempDir(), "cockroach-data")
	// const entsPerBatch = 10
	// const batches = 3

	p := os.ExpandEnv("$HOME/go/src/github.com/cockroachdb/cockroach/cockroach-data")
	// p := filepath.Join(t.TempDir(), "cockroach-data")
	const ranges = 20
	ids := maybeCreateClusterDir(ctx, t, p, ranges)
	fmt.Println(ids)

	st := cluster.MakeTestingClusterSettings()

	env, err := fs.InitEnv(ctx, vfs.Default, p, fs.EnvConfig{})
	require.NoError(t, err)
	eng, err := storage.Open(ctx, env, st)
	require.NoError(t, err)
	defer eng.Close()
	for _, id := range ids {
		const entsPerBatch = 100000
		const batches = 100
		appendSomeStuff(ctx, t, st, eng, id, batches, entsPerBatch)
	}
}

func maybeCreateClusterDir(
	ctx context.Context, t *testing.T, path string, ranges int,
) []roachpb.RangeID {
	existing := false
	if _, err := os.Stat(path); err == nil {
		existing = true
	}
	tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs:      base.TestServerArgs{StoreSpecs: []base.StoreSpec{{Path: path}}},
		ReplicationMode: base.ReplicationManual,
	})
	defer tc.Stopper().Stop(ctx)

	// If the directory does not exist, create it and split the scratch space to
	// have the requested number of ranges.
	if !existing || !time.Now().IsZero() {
		_, desc, err := tc.SplitRange(keys.ScratchRangeMin)
		require.NoError(t, err)
		ids := append(make([]roachpb.RangeID, 0, ranges), desc.RangeID)
		for i := 1; i < ranges; i++ {
			_, rhs, err := tc.SplitRange(testutils.MakeKey(keys.ScratchRangeMin, []byte(strconv.Itoa(i))))
			require.NoError(t, err)
			ids = append(ids, rhs.RangeID)
		}
		return ids
	}

	return nil
}

func appendSomeStuff(
	ctx context.Context,
	t *testing.T,
	st *cluster.Settings,
	eng storage.Engine,
	rangeID roachpb.RangeID,
	batches, entsPerBatch int,
) {
	t.Helper()

	// Load the last index in the log.
	rsl := logstore.NewStateLoader(rangeID)
	lastIndex, err := rsl.LoadLastIndex(ctx, eng)
	require.NoError(t, err)
	t.Logf("loaded LastIndex: %d", lastIndex)
	// Determine the term and MaxLeaseIndex of the last entry.
	it, err := raftlog.NewIterator(ctx, rangeID, eng, raftlog.IterOptions{})
	require.NoError(t, err)
	defer it.Close()
	ok, err := it.SeekGE(lastIndex)
	require.NoError(t, err)
	require.True(t, ok)
	ent, err := raftlog.NewEntry(it.Entry())
	require.NoError(t, err)
	lastTerm, lai := ent.Term, ent.Cmd.MaxLeaseIndex
	// Load the lease state.
	sl := stateloader.Make(rangeID)
	lease, err := sl.LoadLease(ctx, eng)
	require.NoError(t, err)

	for batchIdx := 0; batchIdx < batches; batchIdx++ {
		t.Logf("batch %d", batchIdx+1)
		b := storage.NewOpLoggerBatch(eng.NewBatch())
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
			payload, err := raftlog.EncodeCommand(ctx, &raftCmd, idKey, nil)
			require.NoError(t, err)
			ents = append(ents, raftpb.Entry{
				Term:  lastTerm,
				Index: uint64(lastIndex) + uint64(len(ents)) + 1,
				Type:  raftpb.EntryNormal,
				Data:  payload,
			})
		}

		stats := &logstore.AppendStats{}

		msgApp := raftpb.Message{
			Type:      raftpb.MsgStorageAppend,
			To:        raft.LocalAppendThread,
			Term:      lastTerm,
			LogTerm:   lastTerm,
			Index:     uint64(lastIndex),
			Entries:   ents,
			Commit:    uint64(lastIndex) + uint64(len(ents)),
			Responses: []raftpb.Message{{}}, // need >0 responses so StoreEntries will sync
		}

		fakeMeta := metric.Metadata{
			Name: "fake.meta",
		}
		swl := logstore.NewSyncWaiterLoop()
		stopper := stop.NewStopper()
		defer stopper.Stop(ctx)
		swl.Start(ctx, stopper)
		ls := logstore.LogStore{
			RangeID:     rangeID,
			Engine:      eng,
			Sideload:    nil,
			StateLoader: rsl,
			SyncWaiter:  swl,
			EntryCache:  raftentry.NewCache(1024),
			Settings:    st,
			Metrics: logstore.Metrics{
				RaftLogCommitLatency: metric.NewHistogram(metric.HistogramOptions{
					Mode:         metric.HistogramModePrometheus,
					Metadata:     fakeMeta,
					Duration:     time.Millisecond,
					BucketConfig: metric.IOLatencyBuckets,
				}),
			},
		}

		wg := &sync.WaitGroup{}
		wg.Add(1)
		_, err = ls.StoreEntries(ctx, logstore.RaftState{
			LastIndex: lastIndex,
			LastTerm:  kvpb.RaftTerm(lastTerm),
		}, logstore.MakeMsgStorageAppend(msgApp), (*wgSyncCallback)(wg), stats)
		require.NoError(t, err)
		wg.Wait()

		lastIndex = kvpb.RaftIndex(ents[len(ents)-1].Index)
	}

	t.Logf("LastIndex is now: %d", lastIndex)
}

type wgSyncCallback sync.WaitGroup

func (w *wgSyncCallback) OnLogSync(
	ctx context.Context, messages []raftpb.Message, stats storage.BatchCommitStats,
) {
	(*sync.WaitGroup)(w).Done()
}
