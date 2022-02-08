// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/*
Package hlc implements the Hybrid Logical Clock outlined in "Logical Physical
Clocks and Consistent Snapshots in Globally Distributed Databases", available
online at http://www.cse.buffalo.edu/tech-reports/2014-04.pdf.

Each node within a CockroachDB cluster maintains a hybrid logical clock (HLC),
which provides timestamps that are a combination of physical and logical time.
Physical time is based on a node’s coarsely-synchronized system clock, and
logical time is based on Lamport’s clocks.

Hybrid-logical clocks provide a few important properties:

Causality tracking

HLCs provide causality tracking through a combination of a physical and logical
(to break ties) component upon each inter-node exchange. Nodes attach HLC
timestamps to each message that they send and use HLC timestamps from each
message that they receive to update their local clock.

There are currently three channels through which HLC timestamps are passed
between nodes in a cluster:

 - Raft (unidirectional): proposers of Raft commands (i.e. leaseholders) attach
   clock readings to some of these command (e.g. lease transfers, range merges),
   which are later consumed by followers when commands are applied to their Raft
   state machine.

   Ref: (roachpb.Lease).Start.
   Ref: (roachpb.MergeTrigger).FreezeStart.

 - BatchRequest API (bidirectional): clients and servers of the KV BatchRequest
   API will attach HLC clock readings on requests and responses (successes and
   errors).

   Ref: (roachpb.Header).Timestamp.
   Ref: (roachpb.BatchResponse_Header).Now.
   Ref: (roachpb.Error).Now.

 - DistSQL flows (unidirectional): leaves of a DistSQL flow will pass clock
   readings back to the root of the flow. Currently, this only takes place on
   errors, and relates to the "Transaction retry errors" interaction detailed
   below.

   Ref: (roachpb.Error).Now.

Capturing causal relationships between events on different nodes is critical for
enforcing invariants within CockroachDB. What follows is an enumeration of each
of the interactions in which causality passing through an HLC is necessary for
correctness. It is intended to be exhaustive.

For context, recall CockroachDB's transactional model: we guarantee
serializability (transactions appear to be executed in _some_ serial order), and
linearizability (transactions appear to execute in real-time order) for
transactions that have overlapping read/write sets. Notably, we are not strictly
serializable, so transactions with disjoint read/write sets can appear to
execute in any order to a concurrent third observer that has overlapping
read/write sets with both transactions (deemed a "causal reverse").

The linearizability guarantee is important to note as two sequential (in real
time) transactions via two different gateway nodes can be assigned timestamps
in reverse order (the second gateway's clock may be behind), but must still see
results according to real-time order if they access overlapping keys (e.g. B
must see A's write). Also keep in mind that an intent's written timestamp
signifies when the intent itself was written, but the final value will be
resolved to the transaction's commit timestamp, which may be later than the
written timestamp. Since the commit status and timestamp are non-local
properties, a range may contain committed values (as unresolved intents) that
turn out to exist in the future of the local HLC when the intent gets resolved.

TODO(nvanbenschoten): Update the above on written timestamps after #72121.

 - Cooperative lease transfers (Raft channel). During a cooperative lease
   transfer from one replica of a range to another, the outgoing leaseholder
   revokes its lease before its expiration time and consults its clock to
   determine the start time of the next lease. It then proposes this new lease
   through Raft (see the raft channel above). Upon application of this Raft
   entry, the incoming leaseholder forwards its HLC to the start time of the
   lease, ensuring that its clock is >= the new lease's start time.

   The invariant that a leaseholder's clock is always >= its lease's start time
   is used in a few places. First, it ensures that the leaseholder's clock
   always leads the written_timestamp of any value in its keyspace written by a
   prior leaseholder on its range, which is an important property for the
   correctness of observed timestamps. Second, it ensures that the leaseholder
   immediately views itself as the leaseholder. Third, it ensures that if the
   new leaseholder was to transfer the lease away at some point in the future,
   this later lease's start time could be pulled from the local clock and be
   guaranteed to receive an even greater starting timestamp.

   TODO(nvanbenschoten): the written_timestamp concept does not yet exist in
   code. It will be introduced in the replacement to #72121.

 - Range merges (Raft + BatchRequest channels). During a merge of two ranges,
   the right-hand side of the merge passes a "frozen timestamp" clock reading
   from the right-hand side leaseholder, through the merge transaction
   coordinator, all the way to the left-hand side's leaseholder. This timestamp
   is captured after the right-hand side has been subsumed and has stopped
   serving KV traffic. When the left-hand side's leaseholder applies the range
   merge and officially takes control of the combined range, it forwards its HLC
   to this frozen timestamp. Like the previous interaction, this one is also
   necessary to ensure that the leaseholder of the joint range has a clock that
   leads the written_timestamp of any value in its keyspace, even one written
   originally on the right-hand side range.

 - Observed timestamps (Raft + BatchRequest channels). During the lifetime of a
   transaction, its coordinator issues BatchRequests to other nodes in the
   cluster. Each time a given transaction visits a node for the first time, it
   captures an observation from the node's HLC. Separately, when a leaseholder
   on a given node serves a write, it ensures that the node's HLC clock is >=
   the written_timestamp of the write. This written_timestamp is retained even
   if an intent is moved to a higher timestamp if it is asynchronously resolved.
   As a result, these "observed timestamps" captured during the lifetime of a
   transaction can be used to make a claim about values that could not have been
   written yet at the time that the transaction first visited the node, and by
   extension, at the time that the transaction began. This allows the
   transaction to avoid uncertainty restarts in some circumstances.

   A variant of this same mechanism applies to non-transactional requests that
   defer their timestamp allocation to the leaseholder of their (single) range.
   These requests do not collect observed timestamps directly, but they do
   establish an uncertainty interval immediately upon receipt by their target
   leaseholder, using a clock reading from the leaseholder's local HLC as the
   local limit and this clock reading + the cluster's maximum clock skew as the
   global limit. This limit can be used to make claims about values that could
   not have been written yet at the time that the non-transaction request first
   reached the leaseholder node.

   For more, see pkg/kv/kvserver/uncertainty/doc.go.

 - Transaction retry errors (BatchRequest and DistSQL channels).
   TODO(nvanbenschoten/andreimatei): is this a real case where passing a remote
   clock signal through the local clock is necessary? The DistSQL channel and
   its introduction in 72fa944 seem to imply that it is, but I don't really see
   it, because we don't use the local clock to determine which timestamp to
   restart the transaction at. Maybe we were just concerned about a transaction
   restarting at a timestamp above the local clock back then because we had yet
   to separate the "clock timestamp" domain from the "transaction timestamp"
   domain.

Strict monotonicity

HLCs, as implemented by CockroachDB, provide strict monotonicity within and
across restarts on a single node. Within a continuous process, providing this
property is trivial. Across restarts, this property is enforced by waiting out
the maximum clock offset upon process startup before serving any requests in
ensureClockMonotonicity.

The cluster setting server.clock.persist_upper_bound_interval also provides
additional protection here by persisting the wall time of the clock
periodically, although this protection is disabled by default.

Strictly monotonic timestamp allocation ensures that two causally dependent
transactions originating from the same node are given timestamps that reflect
their ordering in real time. However, this itself is not a crucial guarantee,
as CockroachDB does not and can not assume that causally dependent operations
originate from the same node.

Strictly monotonic timestamp allocation underpins the causality tracking uses
detailed above.

Self-stabilization

HLCs provide self-stabilization in the presence of isolated transient clock skew
fluctuations. As stated above, a node forwards its HLC upon its receipt of a
network message. The effect of this is that given sufficient intra-cluster
communication, HLCs across nodes tend to converge and stabilize even if their
individual physical clocks diverge. This provides no strong guarantees but can
mask clock synchronization errors in practice.

Bounded skew

HLCs within a CockroachDB deployment are configured with a maximum allowable
offset between their physical time component and that of other HLCs in the
cluster. This is referred to as the MaxOffset. The configuration is static and
must be identically configured on all nodes in a cluster, which is enforced by
the HeartbeatService.

The notion of a maximum clock skew at all times between all HLCs in a cluster is
a foundational assumption used in different parts of the system. What follows is
an enumeration of the interactions that assume a bounded clock skew, along with
a discussion for each about the consequences of that assumption being broken and
the maximum clock skew between two nodes in the cluster exceeding the configured
limit.

 - Transaction uncertainty intervals. The single-key linearizability property is
   satisfied in CockroachDB by tracking an uncertainty interval for each
   transaction, within which the real-time ordering between two transactions is
   indeterminate. Upon its creation, a transaction is given a provisional commit
   timestamp commit_ts from the transaction coordinator’s local HLC and an
   uncertainty interval of [commit_ts, commit_ts + max_offset].

   When a transaction encounters a value on a key at a timestamp below its
   provisional commit timestamp, it trivially observes the value during reads
   and overwrites the value at a higher timestamp during writes. This alone
   would satisfy single-key linearizability if transactions had access to a
   perfectly synchronized global clock.

   Without global synchronization, the uncertainty interval is needed because it
   is possible for a transaction to receive a provisional commit timestamp up to
   the cluster’s max_offset earlier than a transaction that causally preceded
   this new transaction in real time. When a transaction encounters a value on a
   key at a timestamp above its provisional commit timestamp but within its
   uncertainty interval, it performs an uncertainty restart, moving its
   provisional commit timestamp above the uncertain value but keeping the upper
   bound of its uncertainty interval fixed.

   This corresponds to treating all values in a transaction’s uncertainty window
   as past writes. As a result, the operations on each key performed by
   transactions take place in an order consistent with the real time ordering of
   those transactions.

   HAZARD: If the maximum clock offset is exceeded, it is possible for a
   transaction to serve a stale read that violates single-key linearizability.
   For example, it is possible for a transaction A to write to a key and commit
   at a timestamp t1, then for its client to hear about the commit. The client
   may then initiate a second transaction B on a different gateway that has a
   slow clock. If this slow clock is more than max_offset from other clocks in
   the system, it is possible for transaction B's uncertainty interval not to
   extend up to t1 and therefore for a read of the key that transaction A wrote
   to be missed. Notably, this is a violation of consistency (linearizability)
   but not of isolation (serializability) — transaction isolation has no clock
   dependence.

 - Non-cooperative lease transfers. In the happy case, range leases move from
   replica to replica using a coordinated handoff. However, in the unhappy case
   where a leaseholder crashes or otherwise becomes unresponsive, other replicas
   are able to attempt to acquire a new lease for the range as soon as they
   observe the old lease expire. In this case, the max_offset plays a role in
   ensuring that two replicas do not both consider themselves the leaseholder
   for a range at the same (wallclock) time. This is ensured by designating a
   "stasis" period equal in size to the max_offset at the end of each lease,
   immediately before its expiration, as unusable. By preventing a lease from
   being used within this stasis period, two replicas will never think that they
   hold a valid lease at the same time, even if the outgoing leaseholder has a
   slow clock and the incoming leaseholder has a fast clock (within bounds). For
   more, see LeaseState_UNUSABLE.

   Note however that it is easy to overstate the salient point here if one is
   not careful. Lease start and end times operate in the MVCC time domain, and
   any two leases are always guaranteed to cover disjoint intervals of MVCC
   time. Leases entitle their holder to serve reads at any MVCC time below their
   expiration and to serve writes at any MVCC time at or after their start time
   and below their expiration. Additionally, the lease sequence is attached to
   all writes and checked during Raft application, so a stale leaseholder is
   unable to perform a write after it has been replaced (in "consensus time").
   This combines to mean that even if two replicas believed that they hold the
   lease for a range at the same time, they can not perform operations that
   would be incompatible with one another (e.g. two conflicting writes). Again,
   transaction isolation has no clock dependence.

   HAZARD: If the maximum clock offset is exceeded, it is possible for two
   replicas to both consider themselves leaseholders at the same time. This can
   not lead to stale reads for transactional requests, because a transaction
   with an uncertainty interval that extends past a lease's expiration will not
   be able to use that lease to perform a read (which is enforced by a stasis
   period immediately before its expiration). However, because some
   non-transactional requests receive their timestamp on the server and do not
   carry an uncertainty interval, they would be susceptible to stale reads
   during this period. This is equivalent to the hazard for operations that do
   use uncertainty intervals, but the mechanics differ slightly.

 - "Linearizable" transactions. By default, transactions in CockroachDB provide
   single-key linearizability and guarantee that as long as clock skew remains
   below the configured bounds, transactions will not serve stale reads.
   However, by default, transactions do not provide strict serializability, as
   they are susceptible to the "causal reverse" anomaly.

   However, CockroachDB does supports a stricter model of consistency through
   its COCKROACH_EXPERIMENTAL_LINEARIZABLE environment variable. When in
   "linearizable" mode (also known as "strict serializable" mode), all writing
   transactions (but not read-only transactions) must wait ("commit-wait") an
   additional max_offset after committing to ensure that their commit timestamp
   is below the current HLC clock time of any other node in the system. In doing
   so, all causally dependent transactions are guaranteed to start with higher
   timestamps, regardless of the gateway they use. This ensures that all
   causally dependent transactions commit with higher timestamps, even if their
   read and writes sets do not conflict with the original transaction's. This
   prevents the "causal reverse" anomaly which can be observed by a third,
   concurrent transaction.

   HAZARD: If the maximum clock offset is exceeded, it is possible that even
   after a transaction commit-waits for the full max_offset, a causally
   dependent transaction that evaluates on a different gateway node receives and
   commits with an earlier timestamp. This resuscitates the possibility of the
   causal reverse anomaly, along with the possibility for stale reads, as
   detailed above.

   HAZARD: This mode of operation is completely untested.

To reduce the likelihood of stale reads, nodes periodically measure their
clock’s offset from other nodes. If any node exceeds the configured maximum
offset by more than 80% compared to a majority of other nodes, the node in the
minority self-terminates. This best-effort validation is done in
(rpc.RemoteClockMonitor).VerifyClockOffset.
*/
package hlc
