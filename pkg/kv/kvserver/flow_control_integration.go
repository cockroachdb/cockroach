// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// The various flow_control_*.go files in this package contain integration code
// between kvflowcontrol and kvserver. This file specifically houses the
// interfaces that are implemented elsewhere, and documents how they fit
// together. It's helpful to read kvflowcontrol/{doc,kvflowcontrol}.go to
// understand the library components in question, the comment blocks on
// replicaFlowControlIntegration and (*RaftTransport).kvflowControl. The
// integration interfaces here help address I1-I13 from kvflowcontrol/doc.go.
//
// Here's how the various pieces fit together:
//
//                                          ┌───────────────────┐
//                                          │ Receiver (client) │
//                                          ├───────────────────┴─────────────────────┬─┬─┐
//                                       ┌──○ kvflowcontrolpb.AdmittedRaftLogEntries  │ │ │
//                                       │  └─────────────────────────────────────────┴─┴─┘
//                                       │  ┌───────────────────┐
//                                       │  │ Receiver (client) │
//                                       │  ├───────────────────┴─────────────────────┬─┬─┐
//               ┌─────────────────────▶─┼──○ kvflowcontrolpb.AdmittedRaftLogEntries  │ │ │
//               │                       │  └─────────────────────────────────────────┴─┴─┘
//   ['1] gRPC streams                   │
//        connecting/disconnecting  [1] RaftMessageBatch
//               │                       │
//               │  ┌─────────────────┐  │
//               │  │ Sender (server) │  │
//               │  ├─────────────────┴──│────────────────┐        ┌────────────────────────────────────────┐
//               │  │ RaftTransport      │                │        │ StoresForFlowControl                   │
//               │  │                    │                │        │                                        │
//               │  │                    │                │        │  ┌───────────────────────────────────┐ │
//               │  │                    └────[2] Lookup ─┼────────┼─▶│       kvflowcontrol.Handles       ○─┼──┐
//               │  │                                     │        │  └───────────────────────────────────┘ │  │
//               │  │ ┌─────────────────────────────────┐ │        │  ┌───────────────────────────────────┐ │  │
//               └──┼▶│ connectionTrackerForFlowControl │ ├──['2]──┼─▶│ RaftTransportDisconnectedListener │ │  │
//                  │ └─────────────────────────────────┘ │        │  └──────○────────────────────────────┘ │  │
//                  └─────────────────▲───────────────────┘        └─────────┼──────────────────────────────┘  │
//                                    │                                      │                                 │
//                                    │                                  ['3] onRaftTransportDisconnected   [3] ReturnTokensUpto
//                                    │                                      │                                 │
//                                    │                                      │                                 │
//                                    │       ┌──────────────────────────────┼─────────────────────────────────┼─────────┬─┬─┐
//                                    │       │ replicaFlowControlIntegration│          ┌──────────────────────▼───────┐ │ │ │
//                                    │       │                              │          │     kvflowcontrol.Handle     │ │ │ │
//                                    │       │   onBecameLeader()           ▼          └───────────────────▲─▲────────┘ │ │ │
//                                    │       │   onBecameFollower()           ○────['4] DisconnectStream ──┘ │          │ │ │
//                                    │       │   onDescChanged()              ◀─── ["5] tryReconnect ──────┐ │          │ │ │
//                                    │       │   onFollowersPaused()          ○─── ["7] ConnectStream  ────┼─┘          │ │ │
//                                    │       │ = onRaftTransportDisconnected()         ┌───────────────────▼──────────┐ │ │ │
//                                    │       │ = onRaftTicked()                        │ replicaForFlowControl        │ │ │ │
//                                    │       │   onReplicaDestroyed()                  │                              │ │ │ │
//                                    │       │                                         │   getDescriptor()            │ │ │ │
//                     ["6] isConnectedTo     │                                         │   getPausedFollowers()       │ │ │ │
//                                    │       │                                         │   getBehindFollowers()       │ │ │ │
//                                    │       │                                         │   getInactiveFollowers()     │ │ │ │
//                                    └───────┼─────────────────────────────────────────▶ = getDisconnectedFollowers() │ │ │ │
//                                            │                                         └──────────────────────────────┘ │ │ │
//                                            └──────────────────────────────────────────────────────────────────────────┴─┴─┘
//
// The "server" and "client" demarcations refer to the server and client-side of
// the RaftTransport stream. "Sender" and "Receiver" is kvflowcontrol verbiage,
// referring to where proposals originate (and flow tokens deducted) and the
// remote follower nodes where they're received. Below-raft admission happens
// asynchronously on the receiver nodes, of which the sender is informed, which
// in turn lets it release flow tokens and unblock further proposals.
//
// Notation:
// - Stacked boxes (with "  │││" on the right hand side) indicate that there are
//   multiple of a kind. Like multiple replicaFlowControlIntegration
//   implementations (one per locally held replica), multiple
//   kvflowcontrolpb.AdmittedRaftLogEntries, etc.
// - [<digit>], [<digit>'], and [<digit>"] denote independent sequences,
//   explained in text below.
//
// ---
//
// A. How are flow tokens returned after work is admitted below-raft on remote,
//    receiver nodes?
//
// [1]: When work gets admitted below-raft on the receiver, the sender (where
//      work originated, and flow tokens were deducted) is informed of the fact
//      through the RaftMessageBatch gRPC stream. There are two bi-directional
//      raft transport streams between a pair of nodes. We piggyback
//      kvflowcontrolpb.AdmittedRaftLogEntries on raft messages being sent from
//      the RaftMessageBatch client to the RaftMessageBatch server.
// [2]: We lookup the relevant kvflowcontrol.Handle from the set of
//      kvflowcontrol.Handles, to inform it of below-raft admission.
// [3]: We use the relevant kvflowcontrol.Handle (hanging off of some locally
//      held replica) to return relevant previously deducted flow tokens.
//
// The piggy-backing from [1] and the intercepting of piggy-backed messages and
// kvflowcontrol.Handle lookup from [2] both happen in the RaftTransport layer,
// in raft_transport.go. The set of local kvflowcontrol.Handles is exposed
// through the StoresForFlowControl interface, backed by local stores and their
// contained replicas. Each replica exposes the underlying handle through the
// replicaFlowControlIntegration interface.
//
// ---
//
// B. How do we react to raft transport streams breaking? (I1 from
//    kvflowcontrol/doc.go)
//
// ['1]: The server-side of RaftMessageBatch observes every client-initiated
//       stream breaking. The connectionTrackerForFlowControl, used within the
//       RaftTransport layer, also monitors all live gRPC streams to understand
//       exactly the set of clients we're connected to.
// ['2]: Whenever any raft transport gRPC stream breaks, we notify components of
//       this fact through the RaftTransportDisconnectedListener interface.
// ['3]: This in turn informs all locally held replicas, through the
//       replicaFlowControlIntegration interface.
// ['4]: We actively disconnect streams for replicas we just disconnected from
//       as informed by the raft transport.
//
// Note that we actually plumb down information about exactly which raft
// transport streams broke. It's not enough to simply inform the various
// replicaFlowControlIntegrations of some transport stream breaking, and for
// them to then determine which streams to disconnect. This is because it's
// possible for the streams to be re-established in the interim, or for there to
// be another active stream from the same client but using a different RPC
// class. We still want to free up all tokens for that replication stream, lest
// we leak flow tokens in transit on the particular stream that broke.
//
// ---
//
// C. What happens when the raft transport streams reconnect? (I1 from
//    kvflowcontrol/doc.go)
//
// ["5]: The replicaFlowControlIntegration interface is used to periodically
//       reconnect previously disconnected streams. This is driven primarily
//       through the onRaftTicked() API, but also happens opportunistically
//       through onFollowersPaused(), onRaftTransportDisconnected(), etc.
// ["6]: We check whether we're connected to remote replicas via the
//       raftTransportForFlowControl.isConnectedTo(). This is powered by the
//       connectionTrackerForFlowControl embedded in the RaftTransport which
//       monitors all active gRPC streams as seen on the server-side.
// ["7]: If we're now connected to previously disconnected replicas, we inform
//       the underlying kvflowcontrol.Handle in order to deduct flow tokens for
//       subsequent proposals.

// replicaFlowControlIntegration is used to integrate with replication flow
// control. It intercepts various points in a replica's lifecycle, like it
// acquiring raft leadership or losing it, or its raft membership changing, etc.
// Accessing it requires Replica.mu to be held, exclusively (this is asserted on
// in the canonical implementation). The "external" state is mediated by the
// replicaForFlowControl interface. The state transitions look as follows:
//
//	 ─ ─ ─ ─ ─ ─ ─                                      ┌───── onDestroyed ──────────────────▶ ╳╳╳╳╳╳╳╳╳╳╳╳╳
//	─ ─ ─ ─ ─ ─ ┐ │                                     │ ┌─── onDescChanged(removed=self) ──▶ ╳ destroyed ╳
//	                ┌──────── onBecameLeader ─────────┐ │ │                                    ╳╳╳╳╳╳╳╳╳╳╳╳╳
//	            │ │ │                                 │ │ │
//	            ○ ○ ○                                 ▼ ○ ○
//	        ┌ ─ ─ ─ ─ ─ ─ ─ ┐                 ┌──────────────┐
//	 ─ ─ ─ ○     follower                     │    leader    │ ○─────────────────────────────┐
//	        └ ─ ─ ─ ─ ─ ─ ─ ┘                 └──────────────┘                               │
//	              ▲ ▲                                 ○ ▲       onDescChanged                │
//	              │ │                                 │ │       onFollowersPaused            │
//	 ─ ─ ─ ─ ─ ─ ─  └──────── onBecameFollower ───────┘ └────── onRaftTransportDisconnected ─┘
//	                                                            onRaftTicked
//
// We're primarily interested in transitions to/from the leader state -- the
// equivalent transitions from the follower state are no-ops.
//
//   - onBecameLeader is when the replica acquires raft leadership. At this
//     point we initialize the underlying kvflowcontrol.Handle and other
//     internal tracking state to handle subsequent transitions.
//
//   - onBecameFollower is when the replica loses raft leadership. We close the
//     underlying kvflowcontrol.Handle and clear other tracking state.
//
//   - onDescChanged is when the range descriptor changes. We react to changes
//     by disconnecting streams for replicas no longer part of the range,
//     connecting streams for newly members of the range, closing the underlying
//     kvflowcontrol.Handle + clearing tracking state if we ourselves are no
//     longer part of the range.
//
//   - onFollowersPaused is when the set of paused followers have changed. We
//     react to it by disconnecting streams for newly paused followers, or
//     reconnecting to newly unpaused ones.
//
//   - onRaftTransportDisconnected is when we're no longer connected to some
//     replicas via the raft transport. We react to it by disconnecting relevant
//     streams.
//
//   - onRaftTicked is invoked periodically, and refreshes the set of streams
//     we're connected to. It disconnects streams to inactive followers and/or
//     reconnects to now-active followers. It also observes raft progress state
//     for individual replicas, disconnecting from ones we're not actively
//     replicating to (because they're too far behind on their raft log, in need
//     of snapshots, or because we're unaware of their committed log indexes).
//     It also reconnects streams if the raft progress changes.
//
//   - onDestroyed is when the replica is destroyed. Like onBecameFollower, we
//     close the underlying kvflowcontrol.Handle and clear other tracking state.
//
// TODO(irfansharif): Today, whenever a raft transport stream breaks, we
// propagate O(replicas) notifications. We could do something simpler --
// bump a sequence number for stores that have been disconnected and lazily
// release tokens the next time onRaftTicked() is invoked. Internally we'd
// track the last sequence number we observed for each replication stream.
type replicaFlowControlIntegration interface {
	onBecameLeader(context.Context)
	onBecameFollower(context.Context)
	onDescChanged(context.Context)
	onFollowersPaused(context.Context)
	onRaftTransportDisconnected(context.Context, ...roachpb.StoreID)
	onRaftTicked(context.Context)
	onDestroyed(context.Context)

	handle() (kvflowcontrol.Handle, bool)
}

// replicaForFlowControl abstracts the interface of an individual Replica, as
// needed by replicaFlowControlIntegration.
type replicaForFlowControl interface {
	getTenantID() roachpb.TenantID
	getReplicaID() roachpb.ReplicaID
	getRangeID() roachpb.RangeID
	getDescriptor() *roachpb.RangeDescriptor
	getAppliedLogPosition() kvflowcontrolpb.RaftLogPosition
	getPausedFollowers() map[roachpb.ReplicaID]struct{}
	getBehindFollowers() map[roachpb.ReplicaID]struct{}
	getInactiveFollowers() map[roachpb.ReplicaID]struct{}
	getDisconnectedFollowers() map[roachpb.ReplicaID]struct{}

	annotateCtx(context.Context) context.Context
	assertLocked()        // only affects test builds
	isScratchRange() bool // only used in tests
}

// raftTransportForFlowControl abstracts the node-level raft transport, and is
// used by the canonical replicaForFlowControl implementation. It exposes the
// set of (remote) stores the raft transport is connected to. If the underlying
// gRPC streams break and don't reconnect, this indicates as much. Ditto if
// they're reconnected to. Also see RaftTransportDisconnectListener, which is
// used to observe every instance of gRPC streams breaking in order to free up
// tokens.
type raftTransportForFlowControl interface {
	isConnectedTo(storeID roachpb.StoreID) bool
}

// StoresForFlowControl is used to integrate with replication flow control. It
// exposes the underlying kvflowcontrol.Handles and is informed of (remote)
// stores we're no longer connected via the raft transport.
type StoresForFlowControl interface {
	kvflowcontrol.Handles
	RaftTransportDisconnectListener
}

// RaftTransportDisconnectListener observes every instance of the raft
// transport disconnecting replication traffic to the given (remote) stores.
type RaftTransportDisconnectListener interface {
	OnRaftTransportDisconnected(context.Context, ...roachpb.StoreID)
}
