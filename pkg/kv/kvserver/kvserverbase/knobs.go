// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package workload provides an abstraction for generators of sql query loads
// (and requisite initial data) as well as tools for working with these
// generators.

package kvserverbase

// BatchEvalTestingKnobs contains testing helpers that are used during batch evaluation.
type BatchEvalTestingKnobs struct {
	// TestingEvalFilter is called before evaluating each command.
	TestingEvalFilter ReplicaCommandFilter

	// TestingPostEvalFilter is called after evaluating each command.
	TestingPostEvalFilter ReplicaCommandFilter

	// NumKeysEvaluatedForRangeIntentResolution is set by the stores to the
	// number of keys evaluated for range intent resolution.
	NumKeysEvaluatedForRangeIntentResolution *int64

	// RecoverIndeterminateCommitsOnFailedPushes will propagate indeterminate
	// commit errors to trigger transaction recovery even if the push that
	// discovered the indeterminate commit was going to fail. This increases
	// the chance that conflicting transactions will prevent parallel commit
	// attempts from succeeding.
	RecoverIndeterminateCommitsOnFailedPushes bool

	// AllowGCWithNewThresholdAndKeys configures whether GC requests are allowed
	// to increase the GC threshold and to GC individual keys at the same time. By
	// default, this is not allowed because it is unsafe. See cmd_gc.go for an
	// explanation of why.
	AllowGCWithNewThresholdAndKeys bool

	// DisableInitPutFailOnTombstones disables FailOnTombstones for InitPut. This
	// is useful together with e.g. StoreTestingKnobs.GlobalMVCCRangeTombstone,
	// where we still want InitPut to succeed on top of the range tombstone.
	DisableInitPutFailOnTombstones bool

	// UseRangeTombstonesForPointDeletes will use point-sized MVCC range
	// tombstones when deleting point keys, to increase test coverage. These
	// should not appear different from a point tombstone to a KV client.
	UseRangeTombstonesForPointDeletes bool
}

// IntentResolverTestingKnobs contains testing helpers that are used during
// intent resolution.
type IntentResolverTestingKnobs struct {
	// DisableAsyncIntentResolution disables the async intent resolution
	// path (but leaves synchronous resolution). This can avoid some
	// edge cases in tests that start and stop servers.
	DisableAsyncIntentResolution bool

	// ForceSyncIntentResolution forces all asynchronous intent resolution to be
	// performed synchronously. It is equivalent to setting IntentResolverTaskLimit
	// to -1.
	ForceSyncIntentResolution bool

	// MaxGCBatchSize overrides the maximum number of transaction record gc
	// requests which can be sent in a single batch.
	MaxGCBatchSize int

	// MaxIntentResolutionBatchSize overrides the maximum number of intent
	// resolution requests which can be sent in a single batch.
	MaxIntentResolutionBatchSize int
}
