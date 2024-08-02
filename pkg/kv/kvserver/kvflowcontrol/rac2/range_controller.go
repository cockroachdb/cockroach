// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rac2

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
)

// RangeController provides flow control for replication traffic in KV, for a
// range.
type RangeController interface {
	// WaitForEval seeks admission to evaluate a request at the given priority.
	// This blocks until there are enough tokens available for the request to be
	// admitted for evaluation.
	WaitForEval(ctx context.Context, pri admissionpb.WorkPriority) error
	// HandleRaftEvent handles the provided raft event for the range. This should
	// be called while holding raftMu.
	HandleRaftEvent(ctx context.Context, e RaftEvent) error
	// HandleControllerSchedulerEvent processes an event scheduled by the
	// controller.
	HandleControllerSchedulerEvent(ctx context.Context) error
	// SetReplicas sets the replicas of the range.
	SetReplicas(ctx context.Context, replicas roachpb.ReplicaSet) error
	// SetLeaseholder sets the leaseholder of the range.
	SetLeaseholder(ctx context.Context, replica roachpb.ReplicaID)
	// Close closes the range controller.
	Close(ctx context.Context)
}

// TODO(pav-kv): This struct is a placeholder for the interface or struct
// containing raft entries. Replace this as part of #128019.
type RaftEvent struct{}
