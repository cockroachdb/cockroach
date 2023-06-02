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
	"math"
	"os"
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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// TestCreateManualProposal tracks progress on #75729. Ultimately we would like
// to be able to programmatically build up a raft log from a sequence of
// BatchRequests and apply it to an initial state, all without instantiating
// a Replica.
func TestCreateManualProposal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	p := "cockroach-data"
	if _, err := os.Stat(p); err != nil {
		tc := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{StoreSpecs: []base.StoreSpec{
				{
					Path: p,
				},
			}},
		})
		tc.Stopper().Stop(ctx)
	}

	st := cluster.MakeTestingClusterSettings()
	eng, err := storage.Open(ctx, storage.Filesystem("cockroach-data"), st)
	require.NoError(t, err)
	defer eng.Close()

	b := storage.NewOpLoggerBatch(eng.NewBatch())
	defer b.Batch.Close()

	const rangeID = 54

	// Determine LastIndex, LastTerm, and next MaxLeaseIndex by scanning
	// existing log.
	it := raftlog.NewIterator(54, eng, raftlog.IterOptions{})
	defer it.Close()
	rsl := logstore.NewStateLoader(rangeID)
	lastIndex, err := rsl.LoadLastIndex(ctx, eng)
	require.NoError(t, err)
	ok, err := it.SeekGE(lastIndex)
	require.NoError(t, err)
	require.True(t, ok)

	var lai kvpb.LeaseAppliedIndex
	var lastTerm uint64
	require.NoError(t, raftlog.Visit(eng, rangeID, lastIndex, math.MaxUint64, func(entry raftpb.Entry) error {
		ent, err := raftlog.NewEntry(it.Entry())
		require.NoError(t, err)
		if lai < ent.Cmd.MaxLeaseIndex {
			lai = ent.Cmd.MaxLeaseIndex
		}
		lastTerm = ent.Term
		return nil
	}))

	sl := stateloader.Make(rangeID)
	lease, err := sl.LoadLease(ctx, eng)
	require.NoError(t, err)

	lai++

	var ents []raftpb.Entry
	evalCtx := batcheval.MockEvalCtx{}

	for i := 0; i < 10; i++ {
		c := &kvpb.ProbeRequest{}
		resp := &kvpb.ProbeResponse{}
		c.Key = keys.LocalMax

		cmd, ok := batcheval.LookupCommand(c.Method())
		require.True(t, ok)
		// NB: this should really operate on a BatchRequest. We need to librarize
		// evaluateBatch:
		// https://github.com/cockroachdb/cockroach/blob/9c09473ec9da9d458869abb3fe08a9db251c9291/pkg/kv/kvserver/replica_evaluate.go#L141-L153
		// The good news is, this is already in good shape! Just needs to be moved
		// to a leaf package, like `batcheval`.
		// To use the "true" logic we want this for sure, and probably even the
		// caller of evaluateBatch and a few more levels. Pulling it out until
		// there's nothing left basically.

		args := batcheval.CommandArgs{
			EvalCtx:     evalCtx.EvalContext(),
			Header:      kvpb.Header{},
			Args:        c,
			Stats:       &enginepb.MVCCStats{},
			Uncertainty: uncertainty.Interval{},
		}
		res, err := cmd.EvalRW(ctx, b, args, resp)
		require.NoError(t, err)
		// TODO: there's more stuff in evaluateProposal that would need to be lifted
		// here:
		// https://github.com/cockroachdb/cockroach/blob/f048ab082c58ec0357b2ecad763606ef64faa3b7/pkg/kv/kvserver/replica_proposal.go#L842-L869
		res.WriteBatch = &kvserverpb.WriteBatch{Data: b.Repr()}
		res.LogicalOpLog = &kvserverpb.LogicalOpLog{Ops: b.LogicalOps()}

		// End of evaluation. Start of "proposing".

		// TODO: the "requires consensus" logic is not reusable, make it so:
		// https://github.com/cockroachdb/cockroach/blob/f048ab082c58ec0357b2ecad763606ef64faa3b7/pkg/kv/kvserver/replica_proposal.go#L827-L840

		raftCmd := kvserverpb.RaftCommand{
			// Propose under latest lease, this isn't necessarily what you want (in a
			// test) but it reflects the steady state when proposing under the leader.
			// To also support proposing leases itself, we need this whole chunk of code
			// to be reusable:
			// https://github.com/cockroachdb/cockroach/blob/9a7b735b1282bbb3fb7472cc26a47d516a446958/pkg/kv/kvserver/replica_raft.go#L192-L219
			// Really we probably just want to librarize the relevant parts of
			// evalAndPropose and requestToProposal.
			ProposerLeaseSequence: lease.Sequence,
			MaxLeaseIndex:         lai,
			// Rest was determined by evaluation.
			ReplicatedEvalResult: res.Replicated,
			WriteBatch:           res.WriteBatch,
			LogicalOpLog:         res.LogicalOpLog,
		}

		idKey := raftlog.MakeCmdIDKey()
		payload, err := raftlog.RaftCmdToPayload(ctx, &raftCmd, idKey)
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
				Mode:     metric.HistogramModePrometheus,
				Metadata: fakeMeta,
				Duration: time.Millisecond,
				Buckets:  metric.NetworkLatencyBuckets,
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

	require.NoError(t, raftlog.Visit(eng, rangeID, 0, math.MaxUint64, func(entry raftpb.Entry) error {
		t.Log(entry.Index)
		return nil
	}))
}

type wgSyncCallback sync.WaitGroup

func (w *wgSyncCallback) OnLogSync(
	ctx context.Context, messages []raftpb.Message, stats storage.BatchCommitStats,
) {
	log.Infof(ctx, "XXX DONE")
	(*sync.WaitGroup)(w).Done()
}
