// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
// - See I3a and [^7] for options on what to do when the node comes back up.
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
//   replica having raft leadership. When leadership is lost we release all held
//   flow tokens. Tokens are only deducted at proposal time when the proposing
//   replica is both the raft leader and leaseholder (the latter is tautological
//   since only leaseholders propose). We're relying on timely acquisition of
//   raft leadership by the leaseholder to not be persistently over admitting.
//   - Even if the lease transfers but raft leadership remains, and there's
//     later admission of writes for which tokens were originally deducted at
//     the raft leader, it's the raft leader that's informed of that admission
//     who then releases those tokens.
//   - Avoiding double returns on subsequent AdmittedRaftLogEntries for these
//     already released flow tokens is easy for raft leadership changes since
//     there's a term change, and all earlier/stale AdmittedRaftLogEntries with
//     the lower term can be discarded.
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
//   will only occur when any entry at a higher log position gets admitted (not
//   necessarily the reproposed command). So we're relying on at least future
//   attempt to make it to the follower's raft log.
//   - If a replica abandons a proposal that its deducted tokens for (i.e. it'll
//     no longer repropose it), we'll need to free up those tokens. That
//     proposal may have never made it to the follower's logs (if they were
//     dropped from the raft transport's send/receive queues for example).
//   - Perhaps another way to think about safety (no token leaks) and liveness
//     (eventual token returns) is that on the sender side, we should only
//     deduct tokens for a proposal once actually transmitting it over the
//     relevant, reliable gRPC stream (and reacting to it breaking as described
//     in I1 above). On the receiver, since this proposal/message can be dropped
//     due to full receive queues within the raft transport, we should either
//     signal back to the sender that it return corresponding tokens (and
//     re-acquire on another attempt), or that it simply free up all tokens for
//     this replication stream. This could also apply to dropped messages on the
//     sender side queue, where we just free up all held tokens.
//   - In addition to relying on higher log positions getting admitted or
//     proposals getting abandoned to return deducted tokens when proposals are
//     dropped from full send queues, we could also intercept every dropped
//     proposal and return whatever tokens were deducted for it specifically.
// - If messages containing the entry gets dropped from the raft transport
//   receive queue, we rely on raft re-transmitting said entries. Similar to
//   above, we're relying on the logical admission of some entry with log
//   position equal or higher to free up the deducted tokens.
// - Given AdmittedRaftLogEntries travel over the RaftTransport stream, and
//   dropping them could cause flow token leakage, we guarantee delivery (aside
//   from stream breaking, which is covered in I1) by only piggybacking on
//   requests that exit the send queue, and returning flow tokens[^8] before
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
// I11. What happens when a node is restarted and is being caught up rapidly
//      through raft log appends? We know of cases where the initial log appends
//      and subsequent state machine application be large enough to invert the
//      LSM[^9]. Imagine large block writes with a uniform key distribution; we
//      may persist log entries rapidly across many replicas (without inverting
//      the LSM, so follower pausing is also of no help) and during state
//      machine application, create lots of overlapping files/sublevels in L0.
// - We want to pace the initial rate of log appends while factoring in the
//   effect of the subsequent state machine application on L0 (modulo [^9]). We
//   can use flow tokens for this too. In I3a we outlined how for quorum writes
//   that includes a replica on some recently re-started node, we need to wait
//   for it to be sufficiently caught before deducting/blocking for flow tokens.
//   Until that point we can use flow tokens on sender nodes that wish to send
//   catchup MsgApps to the newly-restarted node. Similar to the steady state,
//   flow tokens are only be returned once log entries are logically admitted
//   (which takes into account any apply-time write amplification, modulo [^9]).
//   Once the node is sufficiently caught up with respect to all its raft logs,
//   it can transition into the mode described in I3a where we deduct/block for
//   flow tokens for subsequent quorum writes.
//
// I12. How does this interact with epoch-LIFO? Or CreateTime ordering
//      generally?
// - Background: Epoch-LIFO tries to reduce lower percentile admission queueing
//   delay (at the expense of higher percentile delay) by switching between the
//   standard CreateTime-based FIFO ordering to LIFO-within-an-epoch. Work
//   submitted by a transaction with CreateTime T is slotted into the epoch
//   number with time interval [E, E+100ms) where T is contained in this
//   interval. The work will start executing after the epoch is closed, at
//   ~E+100ms. This increases transaction latency by at most ~100ms. When the
//   epoch closes, all nodes start executing the transactions in that epoch in
//   LIFO order, and will implicitly have the same set of competing
//   transactions[^10], a set that stays unchanged until the next epoch closes.
//   And by the time the next epoch closes, and the current epoch's transactions
//   are deprioritized, 100ms will have elapsed, which is selected to be long
//   enough for most of these now-admitted to have finished their work.
//   - We switch to LIFO-within-an-epoch once we start observing that the
//     maximum queuing delay for work within a <tenant,priority> starts
//     exceeding the ~100ms we'd add with epoch-LIFO.
//   - The idea is we have bottleneck resources that cause delays without
//     bound with if work keeps accumulating, and other kinds of bottlenecks
//     where delays aren't increasing without bound. We're also relying on work
//     bottlenecked on epoch-LIFO not being able to issue more work.
// - For below-raft work queue ordering, we effectively ignore the "true"
//   CreateTime when ordering work. Within a given <tenant,priority,range>,
//   we instead want admission takes place in raft log order (i.e. entries with
//   lower terms to get admitted first, or lower indexes within the same term).
//   This lets us simplifies token returns which happen by specifying a prefix
//   up to which we want to release flow tokens for a given priority[^11].
//   - NB: Regarding "admission takes place in raft log order", we could
//     implement this differently. We introduced log-position based ordering to
//     simplify the implementation of token returns where we release tokens by
//     specifying the log position up-to-which we want to release held
//     tokens[^12]. But with additional tracking in the below-raft work queues,
//     if we know that work W2 with log position L2 got admitted, and
//     corresponded to F flow token deductions at the origin, and we also know
//     that work W1 with log position L1 is currently queued, also corresponding
//     to F flow token deductions at the origin, we could inform the origin node
//     to return flow tokens up to L1 and still get what we want -- a return of
//     F flow tokens when each work gets admitted.
//   - We effectively ignore "true" CreateTime since flow token deductions at
//     the sender aren't tied to CreateTime in the way they're tied to the
//     issuing tenant or the work class. So while we still want priority-based
//     ordering to release regular flow tokens before elastic ones, releasing
//     flow tokens for work with lower CreateTimes does not actually promote
//     doing older work first since the physical work below-raft is already done
//     before (asynchronous) admission, and the token returns don't unblock work
//     from some given epoch. Below-raft ordering by "true" CreateTime is moot.
// - Note that for WorkQueue orderings, we have (i) fair sharing through
//   tenantID+weight, (ii) strict prioritization, (iii) and sequencing of work
//   within a <tenant,priority>, using CreateTime. For below-raft work
//   queue ordering where we want to admit in roughly log position order, we
//   then (ab)use the CreateTime sequencing by combining each work's true
//   CreateTime with its log position[^13], to get a monotonic "sequencing
//   timestamp" that tracks observed log positions. This sequencing timestamp is
//   kept close to the maximum observed CreateTime within a replication stream,
//   which also lets us generate cluster-wide FIFO ordering as follows.
//   - We re-assign CreateTime in a way that, with a high probability, matches
//     log position order. We can be imprecise/forgetful about this tracking
//     since at worst we might over-admit slightly.
//    - To operate within cluster-wide FIFO ordering, we want to order by
//     "true" CreateTime when comparing work across different ranges. Writes for
//     a single range, as observed by a given store below-raft (follower or
//     otherwise) travel along a single stream. Consider the case where a single
//     store S3 receives replication traffic for two ranges R1 and R2,
//     originating from two separate nodes N1 and N2. If N1 is issuing writes
//     with strictly older CreateTimes, when returning flow tokens we should
//     prefer N1. By keeping these sequence numbers close to "true" CreateTimes,
//     we'd be favoring N1 without introducing bias for replication streams with
//     shorter/longer raft logs[^12][^14].
// - We could improve cluster-wide FIFO properties by introducing a
//   WorkQueue-like data structure that simply orders by "true" CreateTime when
//   acquiring flow tokens above raft.
//   - Could we then use this above-raft ordering to implement epoch-LIFO?
//     Cluster-wide we want to admit work within a given epoch, which here
//     entails issuing replication traffic for work slotted in a given epoch.
//   - Fan-in effects to consider for epoch-LIFO, assuming below-raft orderings
//     are as described above (i.e. log-position based).
//     - When there's no epoch-LIFO happening, we have cluster-wide FIFO
//       ordering within a <tenant,priority> pair as described above.
//     - When epoch-LIFO is happening across all senders issuing replication
//       traffic to a given receiver store, we're seeing work within the same
//       epoch, but we'll be returning flow tokens to nodes issuing work with
//       older CreateTimes. So defeating the LIFO in epoch-LIFO, but we're still
//       completing work slotted into some given epoch.
//     - When epoch-LIFO is happening only on a subset of the senders issuing
//       replication traffic, we'll again be returning flow tokens to nodes
//       issuing work with older CreateTimes. This is undefined.
//       - It's strange that different nodes can admit work from "different
//         epochs"[^10]. What are we to do below-raft, when deciding where to
//         return flow tokens back to, since it's all for the same
//         <tenant,priority>? Maybe we need to pass down whether the work was
//         admitted at the sender with LIFO/FIFO, and return flow tokens in
//         {LIFO,FIFO} order across all nodes that issued {LIFO,FIFO} work? Or
//         also pass down the enqueuing timestamp, so we have a good sense
//         below-raft on whether this work is past the epoch expiration and
//         should be deprioritized.
//   - Because the fan-in effects of epoch-LIFO are not well understood (by this
//     author at least), we just disable it below-raft.
//
// I13. What happens when a range {un,}quiesces?
// - Quiescing a range only prevents its internal raft group from being ticked,
//   which stops it from issuing MsgHeartbeats or calling elections. Quiesced
//   ranges still have a raft leader and/or a leaseholder. Any raft operation
//   (for example, proposals) on any replica ends up unquiescing the range,
//   typically under stable raft leadership. As far as flow tokens are
//   concerned:
//   - Quiesced ranges have no steady stream of RaftTransport messages, which we
//     use to piggyback flow token returns. But we guarantee timely delivery
//     even without messages to piggyback on top of. See I8 above.
//   - When returning flow tokens to a quiesced range's leaseholder, that's ok,
//     we're able to look up the right kvflowcontrol.Handle since the replica is
//     still around. When quiescing a range, we don't need to release all-held
//     tokens, or wait until there are no held flow tokens.
//   - We use the replica ticking to periodically refresh the set of
//     replication streams we're connected to, ticking that's disabled for
//     quiesced ranges. The mechanism is responsible for disconnecting streams
//     from paused, disconnected (per the raft transport), behind (per the raft
//     log), and inactive (those we haven't heard from recently) followers. We
//     don't quiesce ranges in the presence of paused followers, and if
//     followers disconnect, we intercept that directly and release flow tokens.
//     We don't quiesce if live followers are behind on the raft log (dead
//     followers are allowed to be behind). Since the ticking is the only
//     mechanism that frees up tokens for inactive followers, what happens if a
//     range quiesces after observing caught up replicas, one of which is
//     holding onto tokens due to slow below-raft admission, and post-quiescence
//     the node crashes? We do react to the raft transport breaking, so that
//     frees up tokens. But it's perhaps surprising that the replica-inactivity
//     check is disabled. It's not clear what to do here.
//     - We could observe node liveness directly, but we're already effectively
//       doing that when reacting to raft transport streams breaking.
//     - We could release all flow tokens whenever replicas quiesce (though
//       risking over-admission).
//     - We could make the last-updated map more evented, releasing tokens
//       directly whenever replicas expire, to not need depend on this explicit
//       ticking that's disabled when quiesced. We could continue ticking (at
//       low frequency) even when quiesced.
//     - If we only had expiration based leases/no quiescence, this would not be
//       a problem.
//
// See kvserver/flow_control_*.go for where we address all the interactions
// above. The guiding principle is to 'only deduct flow tokens when actively
// replicating a proposal along specific streams', which excludes
// dead/paused/lagging/pre-split RHS/non-longer-group-member replicas, and
// explains why we only do it on replicas that are both leaseholder and leader.
// It also explains why we don't re-deduct on reproposals, or try to intercept
// raft-initiated re-transmissions. For each of these scenarios, we know when
// not to deduct flow tokens, and we simply free up all held tokens and
// safeguard against a subsequent double returns. We care about safety (no token
// leaks, no double returns) and liveness (eventual token returns).
//
// ---
//
// [^1]: kvserverpb.RaftMessageRequest is the unit of what's sent
//       back-and-forth between two nodes over their two uni-directional raft
//       transport streams.
// [^2]: Over which we're dispatching kvflowcontrolpb.AdmittedRaftLogEntries.
// [^3]: kvflowcontrol.DispatchReader implementations do this as part of
//       PendingDispatchFor.
// [^4]: Using DisconnectStream on kvflowcontrol.Handler.
// [^5]: Using ConnectStream on kvflowcontrol.Handler.
// [^6]: DeductTokens on kvflowcontrol.Controller returns whether the deduction
//       was done.
// [^7]: When a node is crashed, instead of ignoring the underlying flow token
//       buckets, an alternative is to DeductTokens without going through Admit
//       (which blocks until flow tokens > 0). That way if the node comes back
//       up and catches up via replay (and not snapshot) we have accounted for
//       the load being placed on it. Though similar to I3a, the risk there is
//       that if start going through Admit() as soon as the node comes back up,
//       the tokens will still be negative and writes will stall. Whether we (i)
//       DeductTokens but wait post node-restart lag before going through
//       Admit(), or (ii) don't DeductTokens (Admit() is rendered a no-op),
//       we're being somewhat optimistic, which is fine.
// [^8]: Using ReturnTokensUpto on kvflowcontrol.Handle.
// [^9]: With async raft storage writes (#17500, etcd-io/raft#8), we can
//       decouple raft log appends and state machine application (see #94854 and
//       #94853). So we could append at a higher rate than applying. Since
//       application can be arbitrarily deferred, we cause severe LSM
//       inversions. Do we want some form of pacing of log appends then,
//       relative to observed state machine application? Perhaps specifically in
//       cases where we're more likely to append faster than apply, like node
//       restarts. We're likely to defeat AC's IO control otherwise.
//       - For what it's worth, this "deferred application with high read-amp"
//         was also a problem before async raft storage writes. Consider many
//         replicas on an LSM, all of which appended a few raft log entries
//         without applying, and at apply time across all those replicas, we end
//         up inverting the LSM.
//       - Since we don't want to wait below raft, one way bound the lag between
//         appended entries and applied ones is to only release flow tokens for
//         an entry at position P once the applied state position >= P - delta.
//         We'd have to be careful, if we're not applying due to quorum loss
//         (as a result of remote node failure(s)), we don't want to deplete
//         flow tokens and cause interference on other ranges.
//       - If this all proves too complicated, we could just not let state
//         machine application get significantly behind due to local scheduling
//         reasons by using the same goroutine to do both async raft log writes
//         and state machine application.
// [^10]: This relies on partially synchronized clocks without requiring
//        explicit coordination.
//        - Isn't the decision to start using epoch-LIFO a node-local one, based
//          on node-local max queuing latency for a <tenant,priority>? So what
//          happens when work for a transaction is operating across multiple
//          nodes, some using epoch-LIFO and some not?
//          Is this why we use the max queuing latency as the trigger to switch
//          into epoch-LIFO? All queued work for an epoch E across all nodes, if
//          still queued after ~100ms, will trigger epoch-LIFO everywhere.
// [^11]: See the implementation for kvflowcontrol.Dispatch.
// [^12]: See UpToRaftLogPosition in AdmittedRaftLogEntries.
// [^13]: See admission.sequencer and its use in admission.StoreWorkQueue.
// [^14]: See the high_create_time_low_position_different_range test case for
//        TestReplicatedWriteAdmission.
