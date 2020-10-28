// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/*
Package kvdoc is the landing page for learning about the KV layer.

How to use this documentation

Each type declared in this package represents a high-level KV concept (for example: Transactions), and serves as an anchor for further reading. Methods defined on a type are subconcepts or related concepts
that the reader is likely to be interested in given their interest in the topic.
The return types of each method optionally reference anchor types defined in
packages related to the concept, the documentation for which follows the same
pattern outlined here. Package-level documentation types are typically more in-
depth, as they can reference internal concepts and unexported symbols.

The comment on each type provides an overview over the concept
a high-level overview over the concepts as well as references for further
reading. Each concept is enriched with examples which provide stable references
into the codebase.

How to maintain this documentation

TODO
Package kvdoc references package-level documentation, but not vice versa.


*/
package kvdoc

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
)

// KV concepts.

// Transactions are the main way through which the KV layer receives and returns
// data. They provide serializable behavior without stale reads, see:
//
// https://www.cockroachlabs.com/blog/consistency-model/
//
// Transactions are optimized to avoid unnecessary replication latencies and
// their implementation is complex. However, to a first approximation,
type Transactions struct{}

// ParallelCommits is the optimized commit protocol that allows transactions to
// (usually) incur only a single replication round-trip latency in order to
// commit. Without ParallelCommits, a transaction performing multiple writes
// would incur at least two replication latencies: one to replicate all writes
// except the commit, and the commit. ParallelCommits allows a round of writes
// to be carried out in parallel with the commit write.
func (Transactions) ParallelCommits() (kvcoord.ParallelCommits, batcheval.ParallelCommits) {
	panic(nil)
}

func (Transactions) TransactionRecord() {}
func (Transactions) Pipelining()        {}

// ClosedTimestamps enable follower reads by providing a guarantee that a follower
// is "up to date" for a given MVCC timestamp.
//
// For in-depth documentation, refer to package `closedts`.
type ClosedTimestamps struct {
	Tracker  closedts.TrackerI
	Provider closedts.Provider
}
