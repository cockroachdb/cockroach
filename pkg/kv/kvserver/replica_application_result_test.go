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

func TestProposalDataAndRaftCommandAreConsideredWhenAddingFields(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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

	prop := &ProposalData{
		ctx:                     context.Background(),
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
	}

	// If you are adding a field to ProposalData or RaftCommand, please consider the
	// desired semantics of that field in `tryReproposeWithNewLeaseIndex{,v2}`. Once
	// this has been done, adjust the expected number of fields below, and populate
	// the field above, to let this test pass.
	//
	// NB: we can't use zerofields for two reasons: First, we have unexported fields
	// here, and second, we don't want to check for recursively populated structs (but
	// only for the top level fields).
	require.Equal(t, 10, reflect.TypeOf(*raftCommand).NumField())
	require.Equal(t, 18, reflect.TypeOf(*prop).NumField())
}
