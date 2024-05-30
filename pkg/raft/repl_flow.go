// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package raft

import "github.com/cockroachdb/cockroach/pkg/raft/tracker"

// NodeID is a unique ID of a node in a raft group.
type NodeID uint64

// FlowState describes the state of the replication flow from the leader to a
// follower, in StateReplicate.
type FlowState struct {
	// Match is the index up to which the follower's log is known to durably match
	// the leader's.
	Match uint64
	// Sent is the index of the last entry sent to this follower. All entries with
	// indices in (Match, Sent] are in flight.
	//
	// Invariant: 0 <= Match <= Sent.
	Sent uint64
}

// FlowStateFrom constructs the FlowState from the Progress struct.
func FlowStateFrom(pr *tracker.Progress) FlowState {
	if pr.State != tracker.StateReplicate {
		panic("not in StateReplicate")
	}
	return FlowState{Match: pr.Match, Sent: pr.Next - 1}
}

// FlowControl is the interface through which a raft node communicates events
// that impact its outgoing log entry replication flows to peers.
//
// Using this interface, the recipient can track all the leader->follower flows
// in StateReplicate. Specifically:
//
//   - Between Lead() and Stop() calls, this node is the leader. All other
//     methods are called between these two calls.
//   - All peers in StateReplicate report their state as a sequence of Update()
//     calls, uninterrupted by Disconnect(), Remove(), or Stop().
//   - All peers in StateReplicate report their (Match, Sent] in-flight state
//     via Update() whenever it changes.
//   - The leader also reports its log state via Append() calls. For every
//     connected flow, the (Sent, lastIndex] interval of log indices represents
//     the outstanding replication work ("send queue").
type FlowControl interface {
	// Lead is called when the node becomes the leader at the given term. The
	// lastIndex communicates the last entry index in this leader's log. After
	// this call, the node starts replicating its log to other nodes in the group.
	Lead(term, lastIndex uint64)
	// Append is called when this leader extends the log.
	Append(lastIndex uint64)
	// Update is called when this leader updates the state of the replication flow
	// for the given peer. This happens if the leader has:
	//
	//	- connected to the follower, and entered StateReplicate
	//	- sent some entries in a MsgApp
	//	- received a MsgApp ack/nack for some entries that changed the flow state
	//
	// It is guaranteed that a sequence of Update calls (uninterrupted by
	// Disconnect, Remove, or Stop) reports FlowState structs which do not
	// regress, i.e. Match and Sent can only go up.
	Update(to NodeID, state FlowState)
	// Disconnect is called when the leader's flow to the given peer has been
	// disrupted, and it left StateReplicate.
	Disconnect(to NodeID)
	// Remove is called when the given peer is removed from the config. The leader
	// stops replicating the log to this peer.
	Remove(to NodeID)
	// Stop is called when this node stops being the leader at the term from the
	// last Lead() call. After this call, the node stops replicating the log.
	Stop()
}

type FlowControlNoop struct{}

func (f FlowControlNoop) Lead(uint64, uint64)      {}
func (f FlowControlNoop) Append(uint64)            {}
func (f FlowControlNoop) Update(NodeID, FlowState) {}
func (f FlowControlNoop) Disconnect(NodeID)        {}
func (f FlowControlNoop) Remove(NodeID)            {}
func (f FlowControlNoop) Stop()                    {}
