// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package observedts contains logic and documentation related to the observed
// timestamp system, which allows transactions to track causality between
// themselves and other, possibly-concurrent, transactions in order to avoid
// uncertainty related restarts.
package observedts

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
// ReadWithinUncertaintyIntervalError
//
// While reading, if a transaction encounters a value above its read timestamp
// but equal to or below its max timestamp, it triggers a read within
// uncertainty interval error.
//
// This error forces the transaction to increase its read timestamp, either
// through a refresh or a retry, in order to ensure that the transaction
// observes the "uncertain" value. In doing so, the reading transaction is
// guaranteed to observe any value written by any other transaction with a
// happened-before relation to it, which is paramount to ensure single-key
// linearizability and avoid stale reads.
var D2 = roachpb.ReadWithinUncertaintyIntervalError{}

// D3 ————————————————————————————————————————————————
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
var D3 = roachpb.ObservedTimestamp{}

// D4 ————————————————————————————————————————————————
//
// Transaction.UpdateObservedTimestamp
//
// A transaction collects observed timestamps as it visits nodes in the cluster
// when performing reads and writes.
var D4 = (&roachpb.Transaction{}).UpdateObservedTimestamp

// D5 ————————————————————————————————————————————————
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
//  1. a leaseholder's clock must always be equal to or greater than the timestamp
//     of all writes that it has served.
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
// Unfortunately, this first invariant does not always hold. It does hold for
// non-transactional writes. It also holds for transactions that perform all of
// their intent writes at the same timestamp and then commit at this timestamp.
// However, it does not hold for transactions which move their commit timestamp
// forward over their lifetime before committing, writing intents at different
// timestamps along the way and "pulling them up" to the commit timestamp after
// committing.
//
// In violating invariant 1, this third case reveals an ambiguity in what it
// means for a leaseholder to "serve a write at a timestamp". The meaning of
// this phrase is straightforward for non-transactional writes. However, for an
// intent write whose original timestamp is provisional and whose eventual
// commit timestamp is stored indirectly in its transaction record at its time
// of commit, the meaning is less clear. This reconciliation to move the intent
// write's timestamp up to its transaction's commit timestamp is asynchronous
// from the transaction commit (and after it has been externally acknowledged).
// So even if a leaseholder has only served writes with provisional timestamps
// up to timestamp 100 (placing a lower bound on its clock of 100), it can be in
// possession of intents that, when resolved, will carry a timestamp of 200. To
// uphold the real-time ordering property, this value must be observed by any
// transaction that begins after the value's transaction committed and was
// acknowledged. So for observed timestamps to be correct as currently written,
// we would need a guarantee that this value's leaseholder would never return an
// observed timestamp < 200 at any point after the transaction commits. But with
// the transaction commit possibly occurring on another node and with
// communication to resolve the intent occurring asynchronously, this seems like
// an impossible guarantee to make.
//
// This would appear to undermine observed timestamps to the point where they
// cannot be used. However, we can claw back some utility by recognizing that
// only a small fraction of transactions commit at a different timestamps than
// the one they used while writing intents. We can also recognize that if we
// were to compare observed timestamps against the timestamp that a committed
// value was originally written (its provisional value if it was once an intent)
// instead of the timestamp that it had been moved to on commit, then invariant
// 1 would hold.
//
// We currently do not take full advantage of this second observation, because
// we do not retain "written timestamps" in committed values. Instead, we do
// something less optimal but cheaper and more convenient. Any intent whose
// timestamp is changed during asynchronous intent resolution is marked as
// "synthetic". Doing so is a compressed way of saying that the value could have
// originally been written as an intent at any time (even min_timestamp) and so
// observed timestamps cannot be used to limit uncertainty by pulling a read's
// uncertainty interval below the value's timestamp.
//
// As a result of this refinement, the following property does properly hold:
//
//    Any writes that the transaction may later see written by leaseholders on
//    this node at higher timestamps than the observed timestamp *and that are
//    not marked as synthetic* could not have taken place causally before this
//    transaction and can be ignored for the purposes of uncertainty.
//
// This application of the synthetic bit prevents observed timestamps from being
// used to avoid uncertainty restarts with the set of committed values whose
// timestamps do not reflect their original write time and therefore do not make
// a claim about the clock of their leaseholder at the time that they were
// committed. It does not change the interaction between observed timestamps and
// any other committed value.
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
var D5 = roachpb.Transaction{}.ObservedTimestamps

// D6 ————————————————————————————————————————————————
//
// ComputeLocalUncertaintyLimit
//
// Observed timestamps allow transactions to avoid uncertainty related restarts
// because they allow transactions to bound their uncertainty limit when reading
// on a node which they have previously collected an observed timestamp from.
// Similarly, observed timestamps can also assist a transaction even on its
// first visit to a node in cases where it gets stuck waiting on locks for long
// periods of time.
var D6 = ComputeLocalUncertaintyLimit

// Ignore unused warnings.
var _, _, _, _, _, _ = D1, D2, D3, D4, D5, D6
