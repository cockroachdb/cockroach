// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package kvnemesis exercises the KV api with random traffic and then validates
// that the observed behaviors are consistent with our guarantees.
//
// A set of Operations are generated which represent usage of the public KV api.
// These include both "workload" operations like Gets and Puts as well as
// "admin" operations like rebalances. These Operations can be handed to an
// Applier, which runs them against the KV api and records the results.
//
// Operations do allow for concurrency (this testing is much less interesting
// otherwise), which means that the state of the KV map is not recoverable from
// _only_ the input. TODO(dan): We can use RangeFeed to recover the exact KV
// history. This plus some Kyle magic can be used to check our transactional
// guarantees.
//
// TODO
// - CPut/InitPut/Increment/Delete
// - DeleteRange/ClearRange/RevertRange/ReverseScan
// - AdminRelocateRange
// - AdminUnsplit
// - AdminScatter
// - CheckConsistency
// - ExportRequest
// - AddSSTable
// - Root and leaf transactions
// - GCRequest
// - Protected timestamps
package kvnemesis
