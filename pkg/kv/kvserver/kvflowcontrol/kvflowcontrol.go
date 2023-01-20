// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package kvflowcontrol provides flow control for replication traffic in KV.
// It's part of the integration layer between KV and admission control.
package kvflowcontrol

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
)

// Stream models the stream over which we replicate data traffic, the
// transmission for which we regulate using flow control. It's segmented by the
// specific store the traffic is bound for and the tenant driving it. Despite
// the underlying link/transport being shared across tenants, modeling streams
// on a per-tenant basis helps provide inter-tenant isolation.
type Stream struct {
	TenantID roachpb.TenantID
	StoreID  roachpb.StoreID
}

// Tokens represent the finite capacity of a given stream, expressed in bytes
// for data we're looking to replicate. Use of replication streams are
// predicated on tokens being available.
type Tokens uint64

// Controller provides flow control for replication traffic in KV, held at the
// node-level.
type Controller interface {
	// Admit seeks admission to replicate data, regardless of size, for work
	// with the given priority, create-time, and over the given stream. This
	// blocks until there are flow tokens available.
	Admit(context.Context, admissionpb.WorkPriority, time.Time, Stream) error
	// DeductTokens deducts (without blocking) flow tokens for replicating work
	// with given priority over the given stream. Requests are expected to
	// have been Admit()-ed first.
	DeductTokens(context.Context, admissionpb.WorkPriority, Tokens, Stream) (deducted bool)
	// ReturnTokens returns flow tokens for the given stream. These tokens are
	// expected to have been deducted earlier with the same priority provided
	// here.
	ReturnTokens(context.Context, admissionpb.WorkPriority, Tokens, Stream)

	// TODO(irfansharif): We might need the ability to "disable" specific
	// streams/corresponding token buckets when there are failures or
	// replication to a specific store is paused due to follower-pausing.
	// That'll have to show up between the Handler and the Controller somehow.
	// See I2, I3a and [^7] in kvflowcontrol/doc.go.
}

// Handle is used to interface with replication flow control; it's typically
// backed by a node-level kvflowcontrol.Controller. Handles are held on replicas
// initiating replication traffic, i.e. are both the leaseholder and raft
// leader, and manage multiple Streams (one per active replica) underneath.
//
// When replicating log entries, these replicas choose the log position
// (term+index) the data is to end up at, and use this handle to track the token
// deductions on a per log position basis. When informed of admitted log entries
// on the receiving end of the stream, we free up tokens by specifying the
// highest log position up to which we've admitted (below-raft admission, for a
// given priority, takes log position into account -- see
// kvflowcontrolpb.AdmittedRaftLogEntries for more details).
type Handle interface {
	// Admit seeks admission to replicate data, regardless of size, for work
	// with the given priority and create-time. This blocks until there are
	// flow tokens available.
	Admit(context.Context, admissionpb.WorkPriority, time.Time)
	// DeductTokensFor deducts (without blocking) flow tokens for replicating
	// work with given priority to members of the raft group. The deduction,
	// if successful, is tracked with respect to the specific raft log position
	// it's expecting it to end up in. Requests are assumed to have been
	// Admit()-ed first.
	DeductTokensFor(context.Context, admissionpb.WorkPriority, kvflowcontrolpb.RaftLogPosition, Tokens)
	// DeductedTokensUpto returns the highest log position for which we've
	// deducted flow tokens for, over the given stream.
	DeductedTokensUpto(context.Context, Stream) kvflowcontrolpb.RaftLogPosition
	// ReturnTokensUpto returns all previously deducted tokens of a given
	// priority for all log positions less than or equal to the one specified.
	// It does for the specific stream. Once returned, subsequent attempts to
	// return tokens upto the same position or lower are no-ops.
	ReturnTokensUpto(context.Context, admissionpb.WorkPriority, kvflowcontrolpb.RaftLogPosition, Stream)
	// ReturnAllTokensUpto is like ReturnTokensUpto but does so across all
	// priorities.
	//
	// NB: This is used when a replica on the other end of a stream gets caught
	// up via snapshot (say, after a log truncation), where we then don't expect
	// dispatches for the individual AdmittedRaftLogEntries between what it
	// admitted last and its latest RaftLogPosition. Another use is during
	// successive lease changes (out and back) within the same raft term -- we
	// want to both free up tokens from when we lost the lease, and also ensure
	// that attempts to return them (on hearing about AdmittedRaftLogEntries
	// replicated under the earlier lease), we discard the attempts.
	ReturnAllTokensUpto(context.Context, kvflowcontrolpb.RaftLogPosition, Stream)
	// Close closes the handle and returns all held tokens back to the
	// underlying controller. Typically used when the replica loses its lease
	// and/or raft leadership, or ends up getting GC-ed (if it's being
	// rebalanced, merged away, etc).
	Close(context.Context)
}

// Dispatch is used (i) to dispatch information about admitted raft log entries
// to specific nodes, and (ii) to read pending dispatches.
type Dispatch interface {
	DispatchWriter
	DispatchReader
}

// DispatchWriter is used to dispatch information about admitted raft log
// entries to specific nodes (typically where said entries originated, where
// flow tokens were deducted and waiting to be returned).
type DispatchWriter interface {
	Dispatch(roachpb.NodeID, kvflowcontrolpb.AdmittedRaftLogEntries)
}

// DispatchReader is used to read pending dispatches. It's used in the raft
// transport layer when looking to piggyback information on traffic already
// bound to specific nodes. It's also used when timely dispatching (read:
// piggybacking) has not taken place.
//
// NB: PendingDispatchFor is expected to remove dispatches from the pending
// list. If the GRPC stream we're sending it over happens to break, we drop
// these dispatches. The node waiting these dispatches is expected to react to
// the stream breaking by freeing up all held tokens.
type DispatchReader interface {
	PendingDispatch() []roachpb.NodeID
	PendingDispatchFor(roachpb.NodeID) []kvflowcontrolpb.AdmittedRaftLogEntries
}
