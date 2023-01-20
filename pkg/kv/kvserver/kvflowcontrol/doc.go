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

// This package contains machinery for "replication admission control" --
// end-to-end flow control for replication traffic. It's part of the integration
// layer between KV and admission control. There are a few components, in and
// out of this package. The [<digit>{l,r}] annotations refer to the figure
// below.
//
// A. The central interfaces/types in this package are:
//    - kvflowcontrol.Controller, held at the node-level and holds all available
//      kvflowcontrol.Tokens for each kvflowcontrol.Stream.
//      - kvflowcontrol.Tokens represent the finite capacity of a given stream,
//        expressed in bytes we're looking to replicate over the given stream.
//        The tokens we deduct are determined post-evaluation, after [3].
//      - kvflowcontrol.Stream models the stream over which we replicate data
//        traffic, transmission for which we regulate using flow control. It's
//        segmented by the specific store the traffic is bound for, and also the
//        tenant driving it.
//    - kvflowcontrol.Handle is held at the replica-level (only on those who are
//      both leaseholder and raft leader), and is used to interface with
//      the node-level kvflowcontrol.Controller. When replicating log entries,
//      these replicas choose the log position (term+index) the data is to end
//      up at, and use this handle to track the token deductions on a per log
//      position basis, see [4]. After being informed of these log entries being
//      admitted by the receiving end of the kvflowcontrol.Stream, it frees up
//      the tokens.
//
// B. kvflowcontrolpb.RaftAdmissionMeta, embedded within each
//    kvserverpb.RaftCommand, includes all necessary information for below-raft
//    IO admission control. Also included is the node where this command
//    originated, who wants to eventually learn of this command's admission.
//    Entries that contain this metadata make use of AC-specific raft log entry
//    encodings described below. They're passed through the [5r] and [5l].
//
// C. kvflowcontrolpb.AdmittedRaftLogEntries, piggybacked as part of
//    kvserverpb.RaftMessageRequest[^1] (see [7r] + [9r] below), contains
//    coalesced information about all raft log entries that were admitted below
//    raft. We use the origin node encoded in raft entry
//    (RaftAdmissionMeta.AdmissionOriginNode) to know where to send these to.
//    This information is used on the origin node to release flow tokens that
//    were acquired when replicating the original log entries. This is [10r] and
//    [8l'] below.
//
// D. kvflowcontrol.Dispatch is used to dispatch information about
//    admitted raft log entries (AdmittedRaftLogEntries) to the specific nodes
//    where (i) said entries originated, (ii) flow tokens were deducted and
//    (iii) are waiting to be returned. The interface is also used to read
//    pending dispatches, which will be used in the raft transport layer when
//    looking to piggyback information on traffic already bound to specific
//    nodes; see [8]. Since timely dispatching (read: piggybacking) is not
//    guaranteed, we allow querying for all long-overdue dispatches.
//
// E. We use specific encodings for raft log entries that contain AC data:
//    EntryEncoding{Standard,Sideloaded}WithAC. Raft log entries have prefix
//    byte that informs decoding routines how to interpret the subsequent bytes.
//    Since we don't want to decode anything if the command is not subject to
//    replication admission control, the encoding type is a convenient place to
//    capture how a specific entry is to be considered.
//    - The decision to use replication admission control happens above raft
//      (using cluster settings, version gates), and AC-specific metadata is
//      plumbed down as part of the marshaled raft command (RaftAdmissionMeta).
//
// F. AdmitRaftEntry, on the kvadmission.Controller is the integration
//    point for log entries received below raft right as they're being written
//    to storage. This is [6] in the figure below; it's non-blocking since we're
//    below raft in the raft.Ready() loop. It effectively enqueues a "virtual"
//    work item in underlying StoreWorkQueue mediating store IO. This virtual
//    work item is what later gets dequeued once the store granter informs the
//    work queue of newly available IO tokens. For standard work queue ordering,
//    our work item needs to include the CreateTime and AdmissionPriority. The
//    tenant ID is plumbed to find the right tenant heap to queue it under (for
//    inter-tenant isolation); the store ID to find the right store work queue
//    on multi-store nodes. Since the raftpb.Entry encodes within it its origin
//    node (AdmissionOriginNode), it's used post-admission to dispatch to the
//    right node; see [7'] below.
//
// ---
//
//  Here's how the various pieces fit together:
//
//                    [1] Admit
//                         ○
//                         │
//                         │
//         ┌───────────────▼───────────────┬┬┐                       ┌───────────────────────────────────┐
//         │ Replica (proposer)            │││                       │ kvflowcontrol.Controller          │
//         │ ┌──────────────────────┬┬○────┼┼┼ [2] Admit ────────────▶ ┌───────────────────────────────┐ │
//         │ │                      │││    │││                       │ │ admissionpb.RegularWorkClass  │ │
//         │ │ kvflowcontrol.Handle ││○────┼┼┼ [4] DeductTokens ─────▶ │┌──────────────────────┬┬┐     │ │
//         │ │                      │││    │││                       │ ││ kvflowcontrol.Stream │││     │ │
//         │ └─────────────▲──────○─┴┴○────┼┼┼ [10r] ReturnTokens ───▶ │└──────────────────────┴┴┘     │ │
//         │                ╲    ╱         │││                       │ └───────────────────────────────┘ │
//         │                 ╲  ╱          │││                       │ ┌───────────────────────────────┐ │
//         │             [3] Evaluate      │││                       │ │ admissionpb.ElasticsWorkClass │ │
//         │                               │││                       │ │┌──────────────────────┬┬┐     │ │
//         │                               │││                       │ ││ kvflowcontrol.Stream │││     │ │
//         │                               │││                       │ │└──────────────────────┴┴┘     │ │
//         │                               │││                       │ └───────────────────────────────┘ │
//         └○────▲────────────────○───────▲┴┴┘                       └──────────────────────▲────────────┘
//                                │       │                                                 │
//          │    │                │       │
//                                │       │                                                 │
//  [5l] MsgApp  │      [5r] MsgApp(s)  [9r] MsgAppResp(s)                               [8l'] ReturnTokens
//                                │       │  + kvflowcontrolpb.AdmittedRaftLogEntries       │  (using kvflowcontrol.Handle)
//          │    │                │       │
//                           ┌────┴───────┴────┐                                ┌───────────○────────────┐
//          │    │           │  RaftTransport  ◀──── [8] PendingDispatchFor ────▶ kvflowcontrol.Dispatch │
//                           └────┬───────┬────┘                                └───────────▲────────────┘
//          │    │                │       │                                                 │
//                                │       │                                                 │
//          │    │                │       │                                                 │
//                                │       │                                               [7'] Dispatch
//          │    │                │       │                                                 │
//             [7l] MsgAppResp    │     [7r] MsgAppResp(s)                                  │
//          │    │                │       │                                                 │
//        ┌─▼────○────────────────▼───────○──┬┬┐                                ┌───────────○────────────┐
//        │ Store                            │││                                │ kvadmission.Controller │
//        │                                  ○┼┼───── [6] AdmitRaftEntry ───────▶                        │
//        │                                  │││                                │                        │
//        │                                  │││                                │      ┌─────────────────┴┬┬┐
//        │                                  │││                                │      │ StoreWorkQueue   │││
//        │                                  │││                                │      ├──────────────────┼┼┤
//        └──────────────────────────────────┴┴┘                                └──────┤ Granter          │││
//                                                                                     └──────────────────┴┴┘
//
// Notation:
// - The top-half of the figure is above raft, the bottom half is below-raft.
// - The paths marked as [<digit>l] denote paths taken for the locally held
//   store and [<digit>r] for remotely held stores where raft traffic crosses an
//   RPC boundary (using the RaftTransport). The MsgApp and MsgAppResp shown in
//   [5l] and [7l] don't actually exist -- the raft library short circuits
//   things, but we include it for symmetry. [8l'] is a fast-path we use whereby
//   we return flow tokens for locally admitted raft log entries without going
//   through the RaftTransport.
// - The paths marked with [<digit>'] happen concurrently. In the diagram
//   above, [7'] happens concurrently with [7r]; we're trying to show that a
//   subsequent MsgAppResps may end up carrying AdmittedRaftLogEntries from
//   earlier. [9r] shows this piggybacking.
// - Stacked boxes (with "  │││" on the right hand side) indicate that there are
//   multiple of a kind. Like multiple stores, multiple StoreWorkQueues,
//   kvflowcontrol.Streams, etc.
//
// ---
//
// There are various interactions to consider:
//
// I1. What happens if the RaftTransport gRPC stream[^2] breaks?
// - When reading pending dispatches to piggyback onto outbound raft messages,
//   we're removing it[^3] from the underlying list. So if the stream breaks
//   after having done so, we're possibly leaking flow tokens at the origin
//   node. Doing some sort of handshake/2PC for every return seems fraught, so
//   we do something simpler: return all held tokens[^4] for a given
//   kvflowcontrol.Stream when the underlying transport breaks. We need to
//   ensure that if the stream re-connects we're not doubly returning flow
//   tokens, for which we use the low water mark[^5] on each stream.
//
// I2. What happens if a node crashes?
// - We don't rely on it to return kvflowcontrolpb.AdmittedRaftLogEntries, and
//   don't want the origin node to leak flow tokens. Here too we react
//   similar to the gRPC stream breakage described above. We don't worry about
//   double returns if the node never restarts, and if it does, we'll treat it
//   similar to I3a below.
// - If the node containing store S2 crashed, we don't want to deduct flow
//   tokens when proposing writes to (now under-replicated) ranges with replicas
//   on S2. kvflowcontrol.Controller is made aware of the streams that are to be
//   'ignored' for flow token purposes, and informs callers when they attempt to
//   deduct[^6], who in turn avoid tracking the (non-)deduction.
//
// I3. What happens if a follower store is paused, as configured by
//     admission.kv.pause_replication_io_threshold?
// - Proposing replicas are aware of paused followers and don't deduct flow
//   tokens for those corresponding streams (we're not issuing traffic over
//   those replication streams so deducting flow tokens is meaningless). When
//   the follower store is no longer paused, and the replica on that store
//   is sufficiently caught up, only then do we start deducting flow tokens for
//   it.
//
//   I3a. Why do we need to wait for the previously paused replica to be caught
//        up (via log entries, snapshots) to the raft log position found on a
//        quorum of replicas? What about replicas that have fallen behind by
//        being on recently crashed nodes?
//   - If we didn't, we risk write stalls to the range (and others) due to flow
//     tokens deducted for that store. Take the following example where we start
//     deducting flow tokens as soon as the follower store is unpaused.
//     - If the quorum is at <term=T,index=I>, and the now-no-longer-paused
//       replica at <term=T,index=I-L>, i.e. lagging by L entries.
//     - As we start proposing commands to <term=T,index=I+1>,
//       <term=T,index=I+2>, ..., we're deducting flow tokens for the
//       previously-paused follower at indexes > I.
//     - These flow tokens that will only be released once the
//       now-no-longer-paused follower stores those entries to its raft log and
//       also admits them.
//     - To do so, it has to first catch up to and admit entries from
//       <term=T,index=I-L+1>, <term=T,index=I-L+2>, ..., <term=T,index=I>,
//       which could take a while.
//     - We don't want to stall quorum writes at <term=T,index=I+i> (if going up
//       to I+i depletes the available flow tokens) because the previously
//       paused follower here has not yet caught up.
//   - When the previously-paused replica starts storing log entries from
//     <term=T,index=I-L+1>, <term=T,index=I-L+2>, ..., and admitting them, it's
//     going to try and return flow tokens for them. This is because those
//     entries are encoded to use replication admission control.
//     - We don't want to add these tokens to the stream's bucket since we
//       didn't make corresponding deductions at propose time (the follower was
//       paused then).
//     - In this case, whenever we start tracking this follower's stream, i.e.
//       after it's sufficiently caught up to the quorum raft log position,
//       we'll ensure it's low water mark is at this position to ignore these
//       invalid returns.
//
// I4. What happens when a replica gets caught up via raft snapshot? Or needs to
//     get caught up via snapshot due to log truncation at the leader?
// - If a replica's fallen far enough behind the quorum with respect to its log
//   position, we don't deduct flow tokens for its stream, similar to I3a. The
//   same is true for if we've truncated our log ahead of what it's stored in
//   its raft log. If it's just far behind on a non-truncated raft log, it's
//   still receiving replication traffic through raft generated MsgApps for
//   the older entries, but we don't deduct flow tokens for it.
// - When the replica gets caught up via snapshot, similar to I3a, given it's
//   caught up to the quorum raft log position, we can start deducting flow
//   tokens for its replication stream going forward.
//
// I5. What happens when the leaseholder and/or the raft leader changes? When
//     the raft leader is not the same as the leaseholder?
// - The per-replica kvflowcontrol.Handle is tied to the lifetime of a
//   leaseholder replica having raft leadership. When leadership is lost, or the
//   lease changes hands, we release all held flow tokens.
//   - Avoiding double returns on subsequent AdmittedRaftLogEntries for these
//     already released flow tokens is easier for raft leadership changes since
//     there's a term change, and all earlier/stale AdmittedRaftLogEntries with
//     the lower term can be discarded. We do a similar thing for leases -- when
//     being granted a lease, the low water mark in kvflowcontrol.Handle is at
//     least as high as the command that transferred the lease.
//
// I6. What happens during replica GC?
// - It's unlikely that a replica gets GC-ed without first going through the
//   leaseholder/raft leadership transition described in I5. Regardless, we'll
//   free up all held flow tokens. If a replica were to be re-added (through
//   membership change) and become raft leader + range leaseholder, and hear
//   about stale AdmittedRaftLogEntries from earlier, they'd be ignored due to
//   the low water mark on kvflowcontrol.Handle.
//
// I7. What happens during re-proposals?
// - Flow token deductions are tied to the first raft log position we propose
//   at. We don't deduct flow tokens again for reproposed commands at higher log
//   positions. Ditto for command reproposals at apply-time (if we violated
//   MLAI).
//
// I8. What happens if the proposal is dropped from the raft transport's send
//     queue? What if it gets dropped from some receive queue?
// - As described in I7, we're binding the proposal to the first log position we
//   try to propose at. If the node-level raft transport's send queue is full,
//   and we drop the proposal, we'll end up reproposing it at a higher index
//   without deducting flow tokens again. Free-ing up the originally held tokens
//   will only occur when an entry at a higher log position gets admitted (not
//   necessarily the reproposed command).
// - If messages containing the entry gets dropped from the raft transport
//   receive queue, we rely on raft re-transmitting said entries. Similar to
//   above, we're relying on the logical admission of some entry with log
//   position equal or higher to free up the deducted tokens.
// - Given AdmittedRaftLogEntries travel over the RaftTransport stream, and
//   dropping them could cause flow token leakage, we guarantee delivery (aside
//   from stream breaking, which is covered in I1) by only piggypacking on
//   requests that exit the send queue, and returning flow tokens[^7] before
//   potentially dropping the stream message due to full receive queues.
//
// I9. What happens during range splits and merges?
// - During merges when the RHS raft group is deleted, we'll free up all held
//   flow tokens. Any subsequent AdmittedRaftLogEntries will be dropped since
//   tokens only get returned through the kvflowcontrol.Handle identified by the
//   RangeID, which for the RHS, no longer exists.
// - During splits, the LHS leaseholder+raft leader will construct its own
//   kvflowcontrol.Handle, which will handle flow tokens for subsequent
//   proposals.
//
// I10. What happens when replicas are added/removed from the raft group?
// - The leader+leaseholder replica is aware of replicas being added/removed
//   replicas, and starts and stops deducting flow tokens for the relevant
//   streams accordingly. If a replica is removed, we free up all flow tokens
//   held for its particular stream. The low water mark for that particular
//   stream is set to the raft log position of the command removing the replica,
//   so stale AdmittedRaftLogEntries messages can be discarded.
//
// ---
//
// [^1]: kvserverpb.RaftMessageRequest is the unit of what's sent
//       back-and-forth between two nodes over their two uni-directional raft
//       transport streams.
// [^2]: Over which we're dispatching kvflowcontrolpb.AdmittedRaftLogEntries.
// [^3]: kvflowcontrol.DispatchReader implementations do this as part of
//       PendingDispatchFor.
// [^4]: Using DeductedTokensUpto + TrackLowWater on kvflowcontrol.Handler.
// [^5]: Using TrackLowWater on kvflowcontrol.Handler.
// [^6]: DeductTokens on kvflowcontrol.Controller returns whether the deduction
//       was done.
// [^7]: Using ReturnTokensUpto on kvflowcontrol.Handle.
//
// TODO(irfansharif): These descriptions are too high-level, imprecise and
// possibly wrong. Fix that. After implementing these interfaces and integrating
// it into KV, write tests for each one of them and document the precise
// interactions/integration points. It needs to be distilled to crisper
// invariants. The guiding principle seems to be 'only grab flow tokens when
// actively replicating a proposal along specific streams', which excludes
// dead/paused/lagging/pre-split RHS/non-longer-group-member replicas, and
// explains why we only do it on replicas that are both leaseholder and leader.
// It also explains why we don't re-deduct on reproposals, or try to intercept
// raft-initiated re-transmissions. For each of these scenarios, we know when
// not to deduct flow tokens. If we observe getting into the scenarios, we
// simply free up all held tokens and safeguard against a subsequent double
// returns, relying entirely on low water marks or RangeIDs not being re-used.
