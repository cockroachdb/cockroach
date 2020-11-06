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

// D1 | A transaction's "max timestamp" is the upper bound of its uncertainty
// interval. The value is set to the transaction's initial timestamp + the
// cluster's maximum clock skew. Assuming maximum clock skew bounds are
// respected, this maximum timestamp places an upper bound on the commit
// timestamp of any other transaction that committed causally before the new
// transaction.
var D1 = roachpb.Transaction{}.MaxTimestamp

// D2 | While reading, if a transaction encounters a value above its read
// timestamp but equal to or below its max timestamp, it triggers a read within
// uncertainty interval error.
//
// This error forces the transaction to increase its read timestamp, either
// through a refresh or a retry, in order to ensure that the transaction
// observes the "uncertain" value. In doing so, the reading transaction is
// guaranteed to observe any value written by an other transaction with a
// happened-before relation to it, which is paramount to ensure single-key
// linearizability and avoid stale reads.
var D2 = roachpb.ReadWithinUncertaintyIntervalError{}

// D3 | An observed timestamp is a combination of a NodeID and a Timestamp. The
// timestamp is said to have been "observed" because it was pulled from the
// corresponding node's HLC clock.
//
// Because HLC clocks are strictly monotonic, an observed timestamp represents a
// promise that any later clock reading from the same node will have a larger
// value. Similarly, they are a promise that any prior update to the clock was
// given a smaller value.
var D3 = roachpb.ObservedTimestamp{}

// D4 | A transaction collects observed timestamps as it visits nodes in the
// cluster when performing reads and writes.
var D4 = (&roachpb.Transaction{}).UpdateObservedTimestamp

// D5 | The observed timestamps are collected in a list on the transaction
// proto. The purpose of this list is to avoid uncertainty related restarts
// which normally occur when reading a value in the near future as per the
// max_timestamp field.
//
// ### Meaning:
//
// Morally speaking, having an entry for a node in this list means that this
// node has been visited before, and that no more uncertainty restarts are
// expected for operations served from it. However, this is not entirely
// accurate. For example, say a txn starts with read_timestamp=1 (and some large
// max_timestamp). It then reads key "a" from node A, registering an entry `A ->
// 5` in the process (`5` happens to be a timestamp taken off that node's clock
// at the start of the read).
//
// Now assume that some other transaction writes and commits a value at key "b"
// and timestamp 4 (again, served by node A), and our transaction attempts to
// read that key. Since there is an entry in its observed_timestamps for A, our
// uncertainty window is `[read_timestamp, 5) = [1, 5)` but the value at key "b"
// is in that window, and so we will restart. However, we will restart with a
// timestamp that is at least high as our entry in the list for node A, so no
// future operation on node A will be uncertain.
//
// ### Correctness:
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
// 1. a leaseholder's clock must always be equal to or greater than the timestamp
//    of all writes that it has served. This is trivial to enforce for
//    non-transactional writes. It is more complicated for transactional writes
//    which may move their commit timestamp forward over their lifetime before
//    committing, even after writing intents on remote Ranges. To accommodate
//    this situation, transactions ensure that at the time of their commit, any
//    leaseholder for a Range that contains one of its intent has an HLC clock
//    with an equal or greater timestamp than the transaction's commit timestamp.
//    TODO(nvanbenschoten): This is violated by txn refreshes. See #36431.
// 2. a leaseholder's clock must always be equal to or greater than the timestamp
//    of all writes that previous leaseholders for its Range have served. We
//    enforce that when a Replica acquires a lease it bumps its node's clock to a
//    time higher than the previous leaseholder's clock when it stopped serving
//    writes. This is accomplished cooperatively for lease transfers and through
//    a statis period before lease expiration for lease acquisitions. It then
//    follows by induction that, in conjunction with the previous invariant, this
//    invariant holds for all leaseholders, given that a Range's initial
//    leaseholder assumes responsibility for an empty range with no writes.
//
// ### Usage:
//
// The property ensures that when this list holds a corresponding entry for the
// node who owns the lease that the current request is executing under, we can
// run the request with the list's timestamp as the upper bound for its
// uncertainty interval, limiting (and often avoiding) uncertainty restarts. We
// do this by lowering the request's max_timestamp down to the timestamp in the
// observed timestamp entry, which is done in Replica.limitTxnMaxTimestamp.
//
// However, as stated, the correctness property only holds for values at higher
// timestamps than the observed timestamp written *by leaseholders on this
// node*. This is critical, as the property tells us nothing about values
// written by leaseholders on different nodes, even if a lease for one of those
// Ranges has since moved to a node that we have an observed timestamp entry
// for. To accommodate this limitation, Replica.limitTxnMaxTimestamp first
// forwards the timestamp in the observed timestamp entry by the start timestamp
// of the lease that the request is executing under before using it to limit the
// request's uncertainty interval.
//
// When a transaction is first initialized on a node, it may use a timestamp
// from the local hybrid logical clock to initialize the corresponding entry in
// the list. In particular, if `read_timestamp` is taken from that node's clock,
// we may add that to the list, which eliminates read uncertainty for reads on
// that node.
var D5 = roachpb.Transaction{}.ObservedTimestamps

// D6 | Observed timestamps allow transactions to avoid uncertainty related
// restarts because they allow transactions to bound their "effective max
// timestamp" when reading on a node which they have previously collected an
// observed timestamp from. Similarly, observed timestamps can also assist a
// transaction even on its first visit to a node in cases where it gets stuck
// waiting on locks for long periods of time.
var D6 = LimitTxnMaxTimestamp

// Ignore unused warnings.
var _, _, _, _, _, _ = D1, D2, D3, D4, D5, D6
