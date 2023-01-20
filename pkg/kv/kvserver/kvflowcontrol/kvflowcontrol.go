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
	// Admit seeks admission to replicate data of a given priority (regardless
	// of size) over the specified streams. This is a blocking operation;
	// requests wait until there are flow tokens available.
	Admit(admissionpb.WorkPriority, ...Stream)
	// DeductTokens deducts (without blocking) flow tokens for transmission over
	// the given streams, for work with a given priority. Requests are expected
	// to have been Admit()-ed first.
	DeductTokens(admissionpb.WorkPriority, Tokens, ...Stream)
	// ReturnTokens returns flow tokens for the given streams. These tokens are
	// expected to have been deducted earlier with a specific priority; that
	// same priority is what's specified here.
	ReturnTokens(admissionpb.WorkPriority, Tokens, ...Stream)

	// TODO(irfansharif): We might need the ability to "disable" specific
	// streams/corresponding token buckets when there are failures or
	// replication to a specific store is paused due to follower-pausing.
	// That'll have to show up between the Handler and the Controller somehow.
}

// Handle is used to interface with replication flow control; it's typically
// backed by a node-level Controller. Handles are held on replicas initiating
// replication traffic, i.e. are both the leaseholder and raft leader. When
// replicating log entries, these replicas choose the log position (term+index)
// the data is to end up at, and use this handle to track the token deductions
// on a per log position basis. Later when freeing up tokens (typically after
// being informed of said log entries being admitted on the receiving end of the
// stream), it's done so by specifying the log position up to which we free up
// all deducted tokens. See kvflowcontrolpb.AdmittedRaftLogEntries for more
// details.
type Handle interface {
	// Admit seeks admission to replicate data of a given priority (regardless
	// of size). This is a blocking operation; requests wait until there are
	// flow tokens available.
	Admit(admissionpb.WorkPriority)
	// DeductTokensFor deducts flow tokens for replicating data of a given
	// priority to members of the raft group, and tracks it with respect to the
	// specific raft log position it's expecting it to end up in. Requests are
	// assumed to have been Admit()-ed first.
	DeductTokensFor(admissionpb.WorkPriority, kvflowcontrolpb.RaftLogPosition, Tokens)
	// ReturnTokensUpto returns all previously deducted tokens of a given
	// priority for all log positions less than or equal to the one specified.
	// Once returned, subsequent attempts to return upto the same position or
	// lower are no-ops.
	ReturnTokensUpto(admissionpb.WorkPriority, kvflowcontrolpb.RaftLogPosition)
	// TrackLowWater is used to set a low-water mark for a given replication
	// stream. Tokens held below this position are returned back to the
	// underlying Controller, regardless of priority. All subsequent returns at
	// that position or lower are ignored.
	//
	// NB: This is used when a replica on the other end of a stream gets caught
	// up via snapshot (say, after a log truncation), where we then don't expect
	// dispatches for the individual AdmittedRaftLogEntries between what it
	// admitted last and its latest RaftLogPosition. Another use is during
	// successive lease changes (out and back) within the same raft term -- we
	// want to both free up tokens from when we lost the lease, and also ensure
	// that attempts to return them (on hearing about AdmittedRaftLogEntries
	// replicated under the earlier lease), we discard the attempts.
	TrackLowWater(Stream, kvflowcontrolpb.RaftLogPosition)
	// Close closes the handle and returns all held tokens back to the
	// underlying controller. Typically used when the replica loses its lease
	// and/or raft leadership, or ends up getting GC-ed (if it's being
	// rebalanced, merged away, etc).
	Close()
}

// Dispatch is used to dispatch information about admitted raft log entries to
// specific nodes and read pending dispatches.
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
type DispatchReader interface {
	PendingDispatch() []roachpb.NodeID
	PendingDispatchFor(roachpb.NodeID) []kvflowcontrolpb.AdmittedRaftLogEntries
}
