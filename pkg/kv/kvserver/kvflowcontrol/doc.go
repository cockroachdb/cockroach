// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowcontrol

// TODO(irfansharif): After implementing these interfaces and integrating it
// into KV, write a "life of a replicated proposal" timeline here for flow-token
// interactions. Talk about how range splits/merges interact and how we ensure
// flow tokens are not leaked or double returned. Talk also about snapshots, log
// truncations, leader/leaseholder changes, leaseholder != leader, follower
// pausing, re-proposals (the token deduction is tracked for the first attempt),
// lossy raft transport send/recv buffers, and raft membership changing.

// This package contains machinery for "replication admission control" --
// end-to-end flow control for replication traffic. It's part of the integration
// layer between KV and admission control. There are a few components, in and
// out of this package.
//
// 1. The central interfaces/types in this package are:
//    - kvflowcontrol.Controller, held at the node-level and holds all available
//      kvflowcontrol.Tokens for each kvflowcontrol.Stream.
//      - kvflowcontrol.Tokens represent the finite capacity of a given stream,
//        expressed in bytes we're looking to replicate over the given stream.
//      - kvflowcontrol.Stream models the stream over which we replicate data
//        traffic, transmission for which we regulate using flow control. It's
//        segmented by the specific store the traffic is bound for, and also the
//        tenant driving it.
//    - kvflowcontrol.Handle is held at the replica-level (only on those who are
//      both leaseholder and raft leader), and is used to interface with
//      the node-level kvflowcontrol.Controller. When replicating log entries,
//      these replicas choose the log position (term+index) the data is to end
//      up at, and use this handle to track the token deductions on a per log
//      position basis. After being informed of these log entries being admitted
//      by the receiving end of the kvflowcontrol.Stream, it frees up the
//      tokens.
//
// 2. kvflowcontrolpb.RaftAdmissionMeta, embedded within each
//    kvserverpb.RaftCommand, includes all necessary information for below-raft
//    IO admission control. Also included is the node where this command
//    originated, who wants to eventually learn of this command's admission.
//    Entries that contain this metadata make use of AC-specific raft log entry
//    encodings described below.
//
// 3. kvflowcontrolpb.AdmittedRaftLogEntries, piggybacked as part of
//    kvserverpb.RaftMessageRequest[1], contains coalesced information about all
//    raft log entries that were admitted below raft. We use the origin node
//    encoded in raft entry (RaftAdmissionMeta.AdmissionOriginNode) to know
//    where to send these to. This information is used on the origin node to
//    release flow tokens that were acquired when replicating the original log
//    entries.
//
// 4. kvflowcontrol.Dispatch is used to dispatch information about
//    admitted raft log entries (AdmittedRaftLogEntries) to the specific nodes
//    where (i) said entries originated, (ii) flow tokens were deducted and
//    (iii) are waiting to be returned. The interface is also used to read
//    pending dispatches, which will be used in the raft transport layer when
//    looking to piggyback information on traffic already bound to specific
//    nodes. Since timely dispatching (read: piggybacking) is not guaranteed, we
//    allow querying for all long-overdue dispatches.
//
// 5. We use specific encodings for raft log entries that contain AC data:
//    EntryEncoding{Standard,Sideloaded}WithAC. Raft log entries have prefix
//    byte that informs decoding routines how to interpret the subsequent bytes.
//    Since we don't want to decode anything if the command is not subject to
//    replication admission control, the encoding type is a convenient place to
//    capture how a specific entry is to be considered.
//    - The decision to use replication admission control happens above raft
//      (using cluster settings, version gates), and AC-specific metadata is
//      plumbed down as part of the marshaled raft command (RaftAdmissionMeta).
//
// 6. AdmitRaftEntry, on the kvadmission.Controller is the integration
//    point for log entries received below raft right as they're being written
//    to storage. This is non-blocking since we're below raft in the
//    raft.Ready() loop. It effectively enqueues a "virtual" work item in
//    underlying StoreWorkQueue mediating store IO. This virtual work item is
//    what later gets dequeued once the store granter informs the work queue of
//    newly available IO tokens. For standard work queue ordering, our work item
//    needs to include the CreateTime and AdmissionPriority. The tenant ID is
//    plumbed to find the right tenant heap to queue it under (for inter-tenant
//    isolation); the store ID to find the right store work queue on multi-store
//    nodes. Since the raftpb.Entry encodes within it its origin node
//    (AdmissionOriginNode), it's used post-admission to dispatch to the right
//    node.
//
// [1]: kvserverpb.RaftMessageRequest is the unit of what's sent
//      back-and-forth between two nodes over their two uni-directional raft
//      transport streams.
