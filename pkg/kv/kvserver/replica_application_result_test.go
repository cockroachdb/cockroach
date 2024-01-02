// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/stretchr/testify/require"
)

func makeProposalData() *ProposalData {
	raftCommand := &kvserverpb.RaftCommand{
		ProposerLeaseSequence: 1,
		MaxLeaseIndex:         1,
		ClosedTimestamp:       &hlc.Timestamp{},
		ReplicatedEvalResult:  kvserverpb.ReplicatedEvalResult{IsProbe: true},
		WriteBatch:            &kvserverpb.WriteBatch{},
		LogicalOpLog:          &kvserverpb.LogicalOpLog{},
		TraceData:             map[string]string{},
		AdmissionPriority:     1,
		AdmissionCreateTime:   1,
		AdmissionOriginNode:   1,
	}

	return &ProposalData{
		ctx:                     context.WithValue(context.Background(), struct{}{}, "nonempty-ctx"),
		sp:                      &tracing.Span{},
		idKey:                   "deadbeef",
		proposedAtTicks:         1,
		createdAtTicks:          2,
		command:                 raftCommand,
		encodedCommand:          []byte("x"),
		quotaAlloc:              &quotapool.IntAlloc{},
		ec:                      endCmds{repl: &Replica{}},
		applied:                 true,
		doneCh:                  make(chan proposalResult),
		Local:                   &result.LocalResult{},
		Request:                 &kvpb.BatchRequest{},
		leaseStatus:             kvserverpb.LeaseStatus{Lease: roachpb.Lease{Sequence: 1}},
		tok:                     TrackedRequestToken{done: true},
		raftAdmissionMeta:       &kvflowcontrolpb.RaftAdmissionMeta{},
		v2SeenDuringApplication: true,
		seedProposal:            nil,
		lastReproposal:          nil,
	}
}

func TestProposalDataAndRaftCommandAreConsideredWhenAddingFields(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	prop := makeProposalData()
	// If you are adding a field to ProposalData or RaftCommand, please consider the
	// desired semantics of that field in `tryReproposeWithNewLeaseIndex{,v2}`. Once
	// this has been done, adjust the expected number of fields below, and populate
	// the field above, to let this test pass.
	//
	// NB: we can't use zerofields for two reasons: First, we have unexported fields
	// here, and second, we don't want to check for recursively populated structs (but
	// only for the top level fields).
	require.Equal(t, 10, reflect.TypeOf(*prop.command).NumField())
	require.Equal(t, 19, reflect.TypeOf(*prop).NumField())
}

func TestReplicaMakeReproposalChaininig(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var r Replica
	proposals := make([]*ProposalData, 1, 4)
	proposals[0] = makeProposalData()
	sharedCtx := proposals[0].ctx

	verify := func() {
		seed := proposals[0]
		require.Nil(t, seed.seedProposal)
		// The seed proposal must know the latest reproposal.
		if len(proposals) > 1 {
			require.Equal(t, proposals[len(proposals)-1], seed.lastReproposal)
		} else {
			require.Nil(t, seed.lastReproposal)
		}
		// All reproposals must point at the seed proposal.
		for _, reproposal := range proposals[1:] {
			require.Equal(t, seed, reproposal.seedProposal)
			require.Nil(t, reproposal.lastReproposal)
		}
		// Only the latest reproposal must use the seed context.
		for _, prop := range proposals[:len(proposals)-1] {
			require.NotEqual(t, sharedCtx, prop.ctx)
		}
		require.Equal(t, sharedCtx, proposals[len(proposals)-1].ctx)
	}

	verify()
	for i := 1; i < cap(proposals); i++ {
		reproposal, onSuccess := r.makeReproposal(proposals[i-1])
		proposals = append(proposals, reproposal)
		onSuccess()
		verify()
	}

	reproposal, onSuccess := r.makeReproposal(proposals[len(proposals)-1])
	_, _ = reproposal, onSuccess // No onSuccess call, assume the proposal failed.
	verify()
}
