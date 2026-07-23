// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package kvnemesis exercises the KV API with random concurrent traffic (as
// well as splits, merges, etc) and then validates that the observed behaviors
// are serializable.
//
// It does so in polynomial time based on the techniques used by [Elle] (see in
// particular section 4.2.3), using the after-the-fact MVCC history as a record
// of truth. It ensures that all write operations embed a unique identifier that
// is stored in MVCC history, and can thus identify which of its operations'
// mutations are reflected in the database ("recoverability" in Elle parlance).
//
// A run of kvnemesis proceeds as follows.
//
// First, a Generator is instantiated. It can create, upon request, a sequence
// in which each element is a random Operation. Operations that are mutations
// (i.e. not pure reads) are assigned a unique kvnemesisutil.Seq which will be
// stored alongside the MVCC data written by the Operation.
//
// A pool of worker threads concurrently generates Operations and executes them
// against a CockroachDB cluster. Some of these Operations may
// succeed, some may fail, and for some of them an ambiguous result may be
// encountered.
// Alongside this random traffic, kvnemesis maintains a RangeFeed that ingests
// the MVCC history. This creates a "carbon copy" of the MVCC history.
//
// All of these workers wind down once they have in aggregate completed a
// configured number of steps.
//
// Next, kvnemesis validates that the Operations that were executed and the
// results they saw match the MVCC history, i.e. checks for Serializability. In
// general, this is an NP-hard problem[^1], but the use of unique sequence
// numbers (kvnemesisutil.Seq) makes it tractable, as each mutation in the MVCC
// keyspace uniquely identifies the Operation that must have written the value.
//
// We make use of this property as follows. First, the Validate method iterates
// through all Operations performed and, for each, inspects
//
// - the type of the Operation (i.e. Put, Delete, Get, ...),
// - the (point or range) key of the operation, and
// - its results (i.e. whether there was an error or which key-value pairs were returned).
//
// Each atomic unit (i.e. individual non-transactional operation, or batch of
// non-transactional operations, or transaction) results in a slice (in order
// in which the Operations within executed[^2]) of observations, where each
// element is either an observed read or an observed write.
//
// For example, a Batch that first writes `v1` to `k1`, then scans `[k1-k3)`
// (reading its own write as well as some other key k2 with value v2) and then
// deletes `k3` would generate the following slice:
//
//	[
//	  observedWrite(k1->v1),
//	  observedScan(k1->v1, k2->v2),
//	  observedWrite(k3->v3),
//	]
//
// Each such slice (i.e. atomic unit) will then be compared to the MVCC history.
// For all units that succeeded, their writes must match up with versions in
// the MVCC history, and this matching is trivial thanks to unique values
// (including for deletions, since we embed the kvnemesisutil.Seq in the value),
// and in particular a single write will entirely fix the MVCC timestamp at
// which the atomic unit must have executed.
//
// For each read (i.e. get or scan), we compute at which time intervals each
// read would have been valid. For example, if the MVCC history for a key `k1`
// is as follows:
//
//	           k1
//
//		 -----------------
//		 t5      v2
//		 t4
//		 t3
//		 t2     <del>
//		 t1      v1
//
// then
//
//   - observedGet(k1->v1)  is valid for [t1,t2),
//   - observedGet(k1->nil) is valid for [0,t1) and [t2,t5), and
//   - observedGet(k1->v2)  is valid for [t5,inf).
//
// By intersecting the time intervals for each Operation in an atomic unit, we
// then get the MVCC timestamps at which this Operation would have observed what
// it ended up observing. If this intersection is empty, serializability must have
// been violated.
//
// In the error case, kvnemesis verifies that no part of the Operation became visible.
// For ambiguous results, kvnemesis requires that either no Operation become visible,
// or otherwise treats the atomic unit as committed.
//
// The KV API also has the capability to return the actual execution timestamp directly
// with responses. At the time of writing, kvnemesis does verify that it does do this
// reliably, but it does not verify that the execution timestamp is contained in the
// intersection of time intervals obtained from inspecting MVCC history[^3].
//
// [Elle]: https://arxiv.org/pdf/2003.10554.pdf
// [^1]: https://dl.acm.org/doi/10.1145/322154.322158
// [^2]: there is currently concurrency within the atomic unit in kvnemesis. It
// could in theory carry out multiple reads concurrently while not also writing,
// such as DistSQL does, but this is not implemented today. See:
// https://github.com/cockroachdb/cockroach/issues/64825
// [^3]: tracked in https://github.com/cockroachdb/cockroach/issues/92898.
package kvnemesis
