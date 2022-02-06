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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/uncertainty"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestCreateManualProposal tracks progress on #75729. Ultimately we would like
// to be able to programmatically build up a raft log from a sequence of
// BatchRequests and apply it to an initial state, all without instantiating
// a Replica.
func TestCreateManualProposal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()
	b := storage.NewOpLoggerBatch(eng.NewBatch())
	defer b.Close()

	const (
		rangeID   = roachpb.RangeID(1)
		replicaID = roachpb.ReplicaID(1)
	)
	replicas := roachpb.MakeReplicaSet(nil)
	replicas.AddReplica(roachpb.ReplicaDescriptor{
		NodeID:    1,
		StoreID:   1,
		ReplicaID: replicaID,
		Type:      roachpb.ReplicaTypeVoterFull(),
	})
	desc := roachpb.NewRangeDescriptor(rangeID, roachpb.RKeyMin, roachpb.RKeyMax, replicas)
	require.NoError(t, stateloader.WriteInitialRangeState(
		ctx, eng, *desc, replicaID, clusterversion.TestingBinaryVersion,
	))

	put := roachpb.NewPut(roachpb.Key("foo"), roachpb.MakeValueFromString("bar"))

	cmd, ok := batcheval.LookupCommand(put.Method())
	require.True(t, ok)

	evalCtx := batcheval.MockEvalCtx{}

	// NB: this should really operate on a BatchRequest. We need to librarize
	// evaluateBatch:
	// https://github.com/cockroachdb/cockroach/blob/9c09473ec9da9d458869abb3fe08a9db251c9291/pkg/kv/kvserver/replica_evaluate.go#L141-L153
	// The good news is, this is already in good shape! Just needs to be moved
	// to a leaf package, like `batcheval`.
	// To use the "true" logic we want this for sure, and probably even the
	// caller of evaluateBatch and a few more levels. Pulling it out until
	// there's nothing left basically.

	resp := &roachpb.PutResponse{}
	args := batcheval.CommandArgs{
		EvalCtx:     evalCtx.EvalContext(),
		Header:      roachpb.Header{},
		Args:        put,
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

	sl := stateloader.Make(rangeID)
	st, err := sl.LoadLease(ctx, eng)
	require.NoError(t, err)
	raftCmd := kvserverpb.RaftCommand{
		// Propose under latest lease, this isn't necessarily what you want (in a
		// test) but it reflects the steady state when proposing under the leader.
		// To also support proposing leases itself, we need this whole chunk of code
		// to be reusable:
		// https://github.com/cockroachdb/cockroach/blob/9a7b735b1282bbb3fb7472cc26a47d516a446958/pkg/kv/kvserver/replica_raft.go#L192-L219
		// Really we probably just want to librarize the relevant parts of
		// evalAndPropose and requestToProposal.
		ProposerLeaseSequence: st.Sequence,
		// TODO: this would need to be assigned usefully as well if this log has
		// any chance of applying. So when we build a log, we need a state keeper
		// that uses sane defaults but can be overridden if test so desires. The
		// prod code assigns this when flushing from the proposal buffer.
		MaxLeaseIndex: 0,
		// Ditto.
		ClosedTimestamp: nil,

		TraceData: nil, // can be injected at will

		// Rest was determined by evaluation.
		ReplicatedEvalResult: res.Replicated,
		WriteBatch:           res.WriteBatch,
		LogicalOpLog:         res.LogicalOpLog,
	}

	// TODO need library code to make an entry. Also something to write it
	// to disk (these have to be separate things since we hand the entry to
	// raft and then raft hands it back to us via the raft.Storage).
	// In effect this means librarizing `(*Replica).propose`.
	idKey := kvserver.MakeIDKey()
	payload, err := kvserver.RaftCmdToPayload(ctx, idKey, &raftCmd, replicaID)
	require.NoError(t, err)
	// This is something suitable for raft.Propose{,ConfChange}...
	_ = payload
	// ... which will result in a log entry being written out in (*Replica).append:
	// https://github.com/cockroachdb/cockroach/blob/9a7b735b1282bbb3fb7472cc26a47d516a446958/pkg/kv/kvserver/replica_raftstorage.go#L642-L649
	// which should definitely also lean on a library-style method.
}
