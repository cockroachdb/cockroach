// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
// First, the generator creates a random slice of operations, each with a unique
// sequence number. These are distributed across a number of concurrent worker
// threads and are executed against the database. Some of these operations may
// succeed, some may fail, and for some of them an ambiguous result may be
// encountered. A rangefeed consumes the entire MVCC history.
//
// Second, the activity thus generated is validated. An unambiguously failed
// operation is verified for not having left any writes in the MVCC history[^1].
// For an ambiguously failed operation, we check whether any writes materialized
// and if so, treat it as committed, otherwise as failed. A committed operation
// is translated into an atomic sequence of read (`observedRead` and
// `observedScan`) and write (`observedWrite`) operations (based on its
// result[^2]). Each write operation checks whether a write with a matching
// sequence number is present on the affected key and if so, marks itself as
// "materialized", i.e. fills in the timestamp field from the MVCC write. For
// read/scan operation, we compute the range of timestamps at which the
// read/write was valid[^3]. Within an atomic unit, the timestamps thus obtained
// must be compatible with each other if the history is serializable. Also, all
// MVCC writes must be reflected by exactly one observedWrite.
//
// [Elle]: https://arxiv.org/pdf/2003.10554.pdf
// [^1]: this happens indirectly: at the end of validation, any unclaimed writes
// fail validation.
// [^2]: the absence of a result can cause issues, for instance for a DeleteRange that
// was batched with a transaction commit, we don't know which keys were touched.
// [^3]: txns always read their own write, so this requires more work than may be obvious.
package kvnemesis
