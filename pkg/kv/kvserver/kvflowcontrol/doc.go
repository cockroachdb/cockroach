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
// out of this package.
//
// A. The central interfaces/types in this package are:
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
// B. kvflowcontrolpb.RaftAdmissionMeta, embedded within each
//    kvserverpb.RaftCommand, includes all necessary information for below-raft
//    IO admission control. Also included is the node where this command
//    originated, who wants to eventually learn of this command's admission.
//    Entries that contain this metadata make use of AC-specific raft log entry
//    encodings described below.
//
// C. kvflowcontrolpb.AdmittedRaftLogEntries, piggybacked as part of
//    kvserverpb.RaftMessageRequest[^1], contains coalesced information about
//    all raft log entries that were admitted below raft. We use the origin node
//    encoded in raft entry (RaftAdmissionMeta.AdmissionOriginNode) to know
//    where to send these to. This information is used on the origin node to
//    release flow tokens that were acquired when replicating the original log
//    entries.
//
// D. kvflowcontrol.Dispatch is used to dispatch information about
//    admitted raft log entries (AdmittedRaftLogEntries) to the specific nodes
//    where (i) said entries originated, (ii) flow tokens were deducted and
//    (iii) are waiting to be returned. The interface is also used to read
//    pending dispatches, which will be used in the raft transport layer when
//    looking to piggyback information on traffic already bound to specific
//    nodes. Since timely dispatching (read: piggybacking) is not guaranteed, we
//    allow querying for all long-overdue dispatches.
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
// [^1]: kvserverpb.RaftMessageRequest is the unit of what's sent
//      back-and-forth between two nodes over their two uni-directional raft
//      transport streams.
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
//  [5l] MsgApp  │      [5r] MsgApp(s)  [9r] +kvflowcontrolpb.AdmittedRaftLogEntries     [8l'] ReturnTokens (using kvflowcontrol.Handle)
//                                │       │                                                 │
//          │    │                │       │
//                           ┌────┴───────┴────┐                                ┌───────────○────────────┐
//          │    │           │  RaftTransport  │◀─── [8] PendingDispatchFor ────▶ kvflowcontrol.Dispatch │
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
// Notation (imperfect, but hopefully somewhat helpful):
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
//
// ---
//
// Q1. What happens if the RaftTransport gRPC stream[^2] breaks?
// - When reading pending dispatches to piggyback onto outbound raft messages,
//   we're removing it[^3] from the underlying list. So if the stream breaks after
//   having done so, we're possibly leaking flow tokens at the origin node.
//   Doing some sort of handshake/2PC for every return seems fraught, so we do
//   something simpler: return all held tokens[^4] for a given
//   kvflowcontrol.Stream when the underlying transport breaks. We need to
//   ensure that if the stream re-connects we're not doubly returning flow
//   tokens; we use the low water mark on each stream to prevent it.
//
// Q2. What happens if a node crashes?
// - We're not expecting it to return kvflowcontrolpb.AdmittedRaftLogEntries, so
//   origin nodes will have possibly leaked flow tokens. Here too we can react
//   similar to the gRPC stream breakage described above (without having to
//   worry about double returns).
// - If the node containing store S2 crashed, we don't want to deduct flow
//   tokens when proposing writes to (now under-replicated) ranges with replicas
//   on S2. kvflowcontrol.Controller is made aware of the streams that are to be
//   ignored for flow token purposes and informs callers when attempting to
//   deduct[^5], who in turn avoid tracking the non-deduction.
//
// Q3. What happens if a follower store is paused, as configured by
//     admission.kv.pause_replication_io_threshold?
// - Proposing replicas are aware of paused followers and don't deduct flow
//   tokens for those corresponding streams (we're not issuing traffic over
//   those replication streams so deducting flow tokens is meaningless). When
//   the follower store is no longer paused, and replica on that store
//   sufficiently caught up, only then do we start deducting flow tokens for it.
//
//   Q3a. Why do we need to wait for the previously paused replica to be caught
//   (via log entries, snapshots) up to the raft log position found on a quorum
//   of replicas?
//   - If we didn't, we risk write stalls to the range (and others) due to flow
//     tokens deducted for that store. Take the following example where as soon
//     as the follower store is unpaused, we deduct flow tokens for it.
//     - If the quorum is at <term=T,index=I>, and the now-no-longer-paused
//       replica at <term=T,index=I-L>, as we start proposing commands to
//       <term=T,index=I+1>, <term=T,index=I+2>, ..., we're deducting flow
//       tokens for the now-no-longer-paused follower at indexes > I. Tokens
//       that will only be released once that follower stores those entries to
//       its raft log, and also admits them. But it has to first catch up to +
//       admit entries from <term=T,index=I-L+1>, <term=T,index=I-L+2>, ...,
//       <term=T,index=I>, which might take a while. We don't want to stall
//       quorum writes at <term=T,index=I+i> because the follower here is not
//       yet caught up.
//     - Aside: When the now-no-longer-paused replica starts storing log entries
//       from <term=T,index=I-L+1>, <term=T,index=I-L+2>, ..., and admitting
//       them, it's going to try and return flow tokens for them (the entry is
//       encoded to use replication admission control). We don't want to add
//       these tokens to the stream's bucket since we didn't make corresponding
//       deductions at propose time (the follower was paused then). In this
//       case, whenever we start tracking this follower's stream, i.e. after
//       it's sufficiently caught up to the quorum raft log position, we'll
//       ensure it's low water mark is at this position to ignore these invalid
//       returns.
//
// Q4. What happens when the leaseholder or the raft leader changes?
// - The per-replica kvflowcontrol.Handle is tied to the lifetime of a
//   leaseholder replica having raft leadership. When leadership is lost, or the
//   lease changes hands, we release all held flow tokens.
//   - Avoiding double returns on subsequent AdmittedRaftLogEntries for these
//     already released flow tokens is easier for raft leadership changes since
//     there's a term change, and all earlier/stale AdmittedRaftLogEntries with
//     the lower term can be discarded. We do a similar thing for leases -- when
//     being granted a lease, the low water mark in kvflowcontrol.Handle is at
//     least as high as the command that transfered the lease.
//
// Q5. What happens during replica GC?
// - It's unlikely that a replica gets GC-ed without first going through the
//   leaseholder/raft leadership transition described in Q4. Regardless, we'll
//   free up all held flow tokens. If a replica were to be re-added (through
//   membership change) and become raft leader + range leaseholder, and hear
//   about stale AdmittedRaftLogEntries from earlier, they'd be ignored due to
//   the low water mark on kvflowcontrol.Handle.
//
// Q6. What happens during re-proposals?
// - Flow token deductions are tied to the first raft log position we propose
//   at. We don't deduct flow tokens again for reproposed commands at higher log
//   positions.
//
// TODO(irfansharif): Lots of these descriptions are too high-level, imprecise
// and possibly wrong. Fix that. After implementing these interfaces and
// integrating it into KV, write tests for each one of them and document
// specific interactions/integration points. Talk about how range splits/merges
// interact and how we ensure flow tokens are not leaked or double returned.
// Talk also about snapshots, log truncations, leaseholder != leader, lossy raft
// transport send/recv buffers, and raft membership changing.
//
// [^2]: Over which we're dispatching kvflowcontrolpb.AdmittedRaftLogEntries.
// [^3]: kvflowcontrol.DispatchReader implementations do this as part of
//       PendingDispatchFor.
// [^4]: Using DeductedTokensUpto + TrackLowWater on kvflowcontrol.Handler.
// [^] : DeductTokens on kvflowcontrol.Controller returns whether the deduction
//       was done.
