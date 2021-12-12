// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package uncertainty contains logic and documentation related to transaction
// uncertainty intervals, which are time windows following readers' timestamps
// within which a reading transaction cannot make real-time ordering guarantees.
// The use of uncertainty intervals allows CockroachDB to guarantee single-key
// linearizability even with only loose (but bounded) clock synchronization.
//
// The package also contains logic and documentation related to the observed
// timestamp system, which allows transactions to track causality between
// themselves and other, possibly-concurrent, transactions in order to avoid
// uncertainty related restarts.
package uncertainty

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// D1 ————————————————————————————————————————————————
//
// Transaction.GlobalUncertaintyLimit
//
// A transaction's global uncertainty limit is the inclusive upper bound of its
// uncertainty interval. The value is set to the transaction's initial timestamp
// + the cluster's maximum clock skew. Assuming maximum clock skew bounds are
// respected, this maximum timestamp places an upper bound on the commit
// timestamp of any other transaction that committed causally before the new
// transaction.
var D1 = roachpb.Transaction{}.GlobalUncertaintyLimit

// D2 ————————————————————————————————————————————————
//
// Interval
//
// When the transaction sends a request to a replica, an uncertainty interval
// is computed. This interval consists of a global and a local component. The
// global uncertainty limit is pulled directly from the transaction. The local
// uncertainty limit is an optional tighter bound established through HLC clock
// observations on individual nodes in the system.
var D2 = Interval{}

// D3 ————————————————————————————————————————————————
//
// ReadWithinUncertaintyIntervalError
//
// While reading, if a transaction encounters a value above its read timestamp
// but equal to or below its global limit, it triggers a read within uncertainty
// interval error.
//
// This error forces the transaction to increase its read timestamp, either
// through a refresh or a retry, in order to ensure that the transaction
// observes the "uncertain" value. In doing so, the reading transaction is
// guaranteed to observe any value written by any other transaction with a
// happened-before relation to it, which is paramount to ensure single-key
// linearizability and avoid stale reads.
var D3 = roachpb.ReadWithinUncertaintyIntervalError{}

// D4 ————————————————————————————————————————————————
//
// ObservedTimestamp
//
// An observed timestamp is a combination of a NodeID and a Timestamp. The
// timestamp is said to have been "observed" because it was pulled from the
// corresponding node's HLC clock.
//
// Because HLC clocks are strictly monotonic, an observed timestamp represents a
// promise that any later clock reading from the same node will have a larger
// value. Similarly, they are a promise that any prior update to the clock was
// given a smaller value.
var D4 = roachpb.ObservedTimestamp{}

// D5 ————————————————————————————————————————————————
//
// Transaction.UpdateObservedTimestamp
//
// A transaction collects observed timestamps as it visits nodes in the cluster
// when performing reads and writes.
var D5 = (&roachpb.Transaction{}).UpdateObservedTimestamp

// D6 ————————————————————————————————————————————————
//
// Transaction.ObservedTimestamps
//
// The observed timestamps are collected in a list on the transaction proto. The
// purpose of this list is to avoid uncertainty related restarts which occur
// when reading a value in the near future, per the global_uncertainty_limit
// field. The list helps avoid these restarts by establishing a lower
// local_uncertainty_limit when evaluating a request on a node in the list.
//
// Meaning
//
// Morally speaking, having an entry for a node in this list means that this
// node has been visited before, and that no more uncertainty restarts are
// expected for operations served from it. However, this is not entirely
// accurate. For example, say a txn starts with read_timestamp=1 (and some large
// global_uncertainty_limit). It then reads key "a" from node A, registering an
// entry `A -> 5` in the process (`5` happens to be a timestamp taken off that
// node's clock at the start of the read).
//
// Now assume that some other transaction writes and commits a value at key "b"
// and timestamp 4 (again, served by node A), and our transaction attempts to
// read that key. Since there is an entry in its observed_timestamps for A, our
// uncertainty window is `[read_timestamp, 5) = [1, 5)` but the value at key "b"
// is in that window, and so we will restart. However, we will restart with a
// timestamp that is at least high as our entry in the list for node A, so no
// future operation on node A will be uncertain.
//
// Correctness
//
// Thus, expressed properly, we can say that when a node has been read from
// successfully before by a transaction, uncertainty for values written by a
// leaseholder on that node is restricted to values with timestamps in the
// interval [read_timestamp, first_visit_timestamp). An upper bound can be
// placed on the uncertainty window because we are guaranteed that at the time
// that the transaction first visited the node, none of the Ranges that it was a
// leaseholder for had served any writes at higher timestamps than the clock
// reading we observe. This implies the following property:
//
//    Any writes that the transaction may later see written by leaseholders on
//    this node at higher timestamps than the observed timestamp could not have
//    taken place causally before this transaction and can be ignored for the
//    purposes of uncertainty.
//
// There are two invariants necessary for this property to hold:
//
//  1. a leaseholder's clock must always be equal to or greater than the timestamp
//     of all writes that it has served. This is trivial to enforce for
//     non-transactional writes. It is more complicated for transactional writes
//     which may move their commit timestamp forward over their lifetime before
//     committing, even after writing intents on remote Ranges. To accommodate
//     this situation, transactions ensure that at the time of their commit, any
//     leaseholder for a Range that contains one of its intent has an HLC clock
//     with an equal or greater timestamp than the transaction's commit timestamp.
//     TODO(nvanbenschoten): This is violated by txn refreshes. See #36431.
//
//  2. a leaseholder's clock must always be equal to or greater than the timestamp
//     of all writes that previous leaseholders for its Range have served. We
//     enforce that when a Replica acquires a lease it bumps its node's clock to a
//     time higher than the previous leaseholder's clock when it stopped serving
//     writes. This is accomplished cooperatively for lease transfers and through
//     a statis period before lease expiration for lease acquisitions. It then
//     follows by induction that, in conjunction with the previous invariant, this
//     invariant holds for all leaseholders, given that a Range's initial
//     leaseholder assumes responsibility for an empty range with no writes.
//
// Usage
//
// The property ensures that when this list holds a corresponding entry for the
// node who owns the lease that the current request is executing under, we can
// run the request with the list's timestamp as the upper bound for its
// uncertainty interval, limiting (and often avoiding) uncertainty restarts. We
// do this by establishing a separate local_uncertainty_limit, which is set to
// the minimum of the global_uncertainty_limit and the node's observed timestamp
// entry in ComputeLocalUncertaintyLimit.
//
// However, as stated, the correctness property only holds for values at higher
// timestamps than the observed timestamp written *by leaseholders on this
// node*. This is critical, as the property tells us nothing about values
// written by leaseholders on different nodes, even if a lease for one of those
// Ranges has since moved to a node that we have an observed timestamp entry
// for. To accommodate this limitation, ComputeLocalUncertaintyLimit first
// forwards the timestamp in the observed timestamp entry by the start timestamp
// of the lease that the request is executing under before using it to limit the
// request's uncertainty interval.
//
// When a transaction is first initialized on a node, it may use a timestamp
// from the local hybrid logical clock to initialize the corresponding entry in
// the list. In particular, if `read_timestamp` is taken from that node's clock,
// we may add that to the list, which eliminates read uncertainty for reads on
// that node.
//
// Follower Reads
//
// If the replica serving a transaction's read is not the leaseholder for its
// range, an observed timestamp pulled from the follower node's clock has no
// meaning for the purpose of reducing the transaction's uncertainty interval.
// This is because there is no guarantee that at the time of acquiring the
// observed timestamp from the follower node, the leaseholder hadn't already
// served writes at higher timestamps than the follower node's clock reflected.
//
// However, if the transaction performing a follower read happens to have an
// observed timestamp from the current leaseholder, this timestamp can be used
// to reduce the transaction's uncertainty interval. Even though the read is
// being served from a different replica in the range, the observed timestamp
// still places a bound on the values in the range that may have been written
// before the transaction began.
var D6 = roachpb.Transaction{}.ObservedTimestamps

// D7 ————————————————————————————————————————————————
//
// TimestampFromServerClock
//
// Non-transactional requests that defer their timestamp allocation to the
// leaseholder of their (single) range also have uncertainty intervals, which
// ensures that they also guarantee single-key linearizability even with only
// loose (but bounded) clock synchronization. Non-transactional requests that
// use a client-provided timestamp do not have uncertainty intervals and do not
// make real-time ordering guarantees.
//
// Unlike transactions, which establish an uncertainty interval on their
// coordinator node during initialization, non-transactional requests receive
// uncertainty intervals from their target leaseholder, using a clock reading
// from the leaseholder's local HLC as the local limit and this clock reading +
// the cluster's maximum clock skew as the global limit.
//
// Non-transactional requests also receive their MVCC timestamp from their
// target leaseholder. As a result of this delayed timestamp and uncertainty
// interval allocation, these requests almost always operate with a local
// uncertainty limit equal to their MVCC timestamp, which minimizes (but does
// not eliminate, see below) their chance of needing to perform an uncertainty
// restart. Additionally, because these requests cannot span ranges, they can
// always retry any uncertainty restart on the server, so the client will never
// receive an uncertainty error.
//
// It is somewhat non-intuitive that non-transactional requests need uncertainty
// intervals — after all, they receive their timestamp to the leaseholder of the
// only range that they talk to, so isn't every value with a commit timestamp
// above their read timestamp certainly concurrent? The answer is surprisingly
// "no" for the following reasons, so they cannot forgo the use of uncertainty
// interval:
//
// 1. the request timestamp is allocated before consulting the replica's lease.
//    This means that there are times when the replica is not the leaseholder at
//    the point of timestamp allocation, and only becomes the leaseholder later.
//    In such cases, the timestamp assigned to the request is not guaranteed to
//    be greater than the written_timestamp of all writes served by the range at
//    the time of allocation. This is true despite invariants 1 & 2 from above,
//    because the replica allocating the timestamp is not yet the leaseholder.
//
//    In cases where the replica that assigned the non-transactional request's
//    timestamp takes over as the leaseholder after the timestamp allocation, we
//    expect minimumLocalLimitForLeaseholder to forward the local uncertainty
//    limit above TimestampFromServerClock, to the lease start time.
//
//    For example, consider the following series of events:
//    - client writes k = v1
//    - leaseholder writes v1 at ts = 100
//    - client receives ack for write
//    - client later wants to read k using a non-txn request
//    - follower replica with slower clock receives non-txn request
//    - follower replica assigns request ts = 95
//    - lease transferred to follower replica with lease start time = 101
//    - non-txn request must use 101 as local limit of uncertainty interval to
//      ensure that it observes k = v1 in its uncertainty interval, performs a
//      server-side retry, bumps its read timestamp, and returns k = v1. Failure
//      to do so would be a stale read.
//
// 2. even if the replica's lease is stable and the timestamp is assigned to the
//    non-transactional request by the leaseholder, the assigned clock reading
//    only reflects the written_timestamp of all of the writes served by the
//    leaseholder (and previous leaseholders) thus far. This clock reading is
//    not guaranteed to lead the commit timestamp of all of these writes,
//    especially if they are committed remotely and resolved after the request
//    has received its clock reading but before the request begins evaluating.
//
//    As a result, the non-transactional request needs an uncertainty interval
//    with a global uncertainty limit far enough in advance of the leaseholder's
//    local HLC clock to ensure that it considers any value that was part of a
//    transaction which could have committed before the request was received by
//    the leaseholder to be uncertain. Concretely, the non-transactional request
//    needs to consider values of the following form to be uncertain:
//
//      written_timestamp < local_limit && commit_timestamp < global_limit
//
//    The value that the non-transactional request is observing may have been
//    written on the local leaseholder at time 10, its transaction may have been
//    committed remotely at time 20, acknowledged, then the non-transactional
//    request may have begun and received a timestamp of 15 from the local
//    leaseholder, then finally the value may have been resolved asynchronously
//    and moved to timestamp 20 (written_timestamp: 10, commit_timestamp: 20).
//    The failure of the non-transactional request to observe this value would
//    be a stale read.
//
//    For example, consider the following series of events:
//    - client begins a txn and is assigned provisional commit timestamp = 10
//    - client's txn performs a Put(k, v1)
//    - leaseholder serves Put(k, v1), lays down intent at written_timestamp = 10
//    - client's txn performs a write elsewhere and hits a WriteTooOldError
//      that bumps its provisional commit timestamp to 20
//    - client's txn refreshes to ts = 20. This notably happens without
//      involvement of the leaseholder that served the Put (this is at the heart
//      of #36431), so that leaseholder's clock is not updated
//    - client's txn commits remotely and client receives the acknowledgment
//    - client later initiates non-txn read of k
//    - leaseholder assigns read timestamp ts = 15
//    - asynchronous intent resolution resolves the txn's intent at k, moving v1
//      to ts = 20 in the process
//    - non-txn request must use an uncertainty interval that extends past 20
//      to ensure that it observes k = v1 in uncertainty interval, performs a
//      server-side retry, bumps its read timestamp, and returns k = v1. Failure
//      to do so would be a stale read.
//
//    TODO(nvanbenschoten): expand on this when we fix #36431. For now, this can
//    be framed in relation to synthetic timestamps, but it's easier to discuss
//    in terms of the impending "written_timestamp" attribute of each value,
//    even though written_timestamps do not yet exist in code.
//
//    TODO(nvanbenschoten): add more direct testing for this when we fix #36431.
//
//    TODO(nvanbenschoten): add another reason here once we address #73292.
//
// Convenient, because non-transactional requests are always scoped to a
// single-range, those that hit uncertainty errors can always retry on the
// server, so these errors never bubble up to the client that initiated the
// request.
var D7 = roachpb.Header{}.TimestampFromServerClock

// D8 ————————————————————————————————————————————————
//
// ComputeInterval
//
// Observed timestamps allow transactions to avoid uncertainty related restarts
// because they allow transactions to bound their uncertainty limit when reading
// on a node which they have previously collected an observed timestamp from.
// Similarly, observed timestamps can also assist a transaction even on its
// first visit to a node in cases where it gets stuck waiting on locks for long
// periods of time.
var D8 = ComputeInterval

// Ignore unused warnings.
var _, _, _, _, _, _, _, _ = D1, D2, D3, D4, D5, D6, D7, D8
