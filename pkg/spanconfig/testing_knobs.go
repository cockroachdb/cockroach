// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfig

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
)

// TestingKnobs provide fine-grained control over the various span config
// components for testing.
type TestingKnobs struct {
	// ManagerDisableJobCreation disables creating the auto span config
	// reconciliation job.
	ManagerDisableJobCreation bool

	// ManagerCheckJobInterceptor, if set, is invoked when checking to see if
	// the reconciliation job exists.
	ManagerCheckJobInterceptor func()

	// ManagerCreatedJobInterceptor expects a *jobs.Job to be passed into it. It
	// takes an interface here to resolve a circular dependency.
	ManagerCreatedJobInterceptor func(interface{})

	// ManagerAfterCheckedReconciliationJobExistsInterceptor is run after the
	// manager has checked if the auto span config reconciliation job exists or
	// not.
	ManagerAfterCheckedReconciliationJobExistsInterceptor func(exists bool)

	// JobDisablePersistingCheckpoints disables the span config reconciliation
	// job from persisting checkpoints.
	JobDisablePersistingCheckpoints bool

	// JobDisableInternalRetry disables the span config reconciliation job's
	// internal retry loop.
	JobDisableInternalRetry bool

	// JobOverrideRetryOptions, if set, controls the internal retry behavior for
	// the reconciliation job.
	JobOverrideRetryOptions *retry.Options

	// JobPersistCheckpointInterceptor, if set, is invoked before the
	// reconciliation job persists checkpoints.
	JobOnCheckpointInterceptor func() error

	// KVSubscriberRangeFeedKnobs control lifecycle events for the rangefeed
	// underlying the KVSubscriber.
	KVSubscriberRangeFeedKnobs base.ModuleTestingKnobs

	// StoreKVSubscriberOverride is used to override the KVSubscriber used when
	// setting up a new store.
	StoreKVSubscriberOverride KVSubscriber

	// KVAccessorPaginationInterceptor, if set, is invoked on every pagination
	// event.
	KVAccessorPaginationInterceptor func()

	// KVAccessorBatchSizeOverride overrides the batch size KVAccessor makes use
	// of internally.
	KVAccessorBatchSizeOverrideFn func() int

	// KVAccessorPreCommitMinTSWaitInterceptor, if set, is invoked before
	// UpdateSpanConfigRecords waits for present time to be in advance of the
	// minimum commit timestamp.
	KVAccessorPreCommitMinTSWaitInterceptor func()

	// KVAccessorPostCommitDeadlineSetInterceptor is invoked after we set the
	// commit deadline.
	KVAccessorPostCommitDeadlineSetInterceptor func(*kv.Txn)

	// SQLWatcherOnEventInterceptor, if set, is invoked when the SQLWatcher
	// receives an event on one of its rangefeeds.
	SQLWatcherOnEventInterceptor func() error

	// SQLWatcherCheckpointNoopsEveryDurationOverride, if set, overrides how
	// often the SQLWatcher checkpoints noops.
	SQLWatcherCheckpointNoopsEveryDurationOverride time.Duration

	// SplitterStepLogger is used to capture internal steps the splitter is
	// making, for debugging and test-readability purposes.
	SplitterStepLogger func(string)

	// ExcludeDroppedDescriptorsFromLookup is used to control if the
	// SQLTranslator ignores dropped descriptors. If enabled, dropped
	// descriptors appear as missing -- a convenient+faster alternative to
	// waiting for the descriptor to actually get GC-ed in tests.
	ExcludeDroppedDescriptorsFromLookup bool

	// ConfigureScratchRange controls whether the scratch range (used in tests)
	// applies the RANGE DEFAULT configuration.
	ConfigureScratchRange bool

	// ReconcilerInitialInterceptor, if set, is invoked at the very outset of
	// the reconciliation process.
	ReconcilerInitialInterceptor func(startTS hlc.Timestamp)

	// ProtectedTSReaderOverrideFn returns a ProtectedTSReader which is used to
	// override the ProtectedTSReader used when setting up a new store.
	ProtectedTSReaderOverrideFn func(clock *hlc.Clock) ProtectedTSReader

	// LimiterLimitOverride, if set, allows tests to dynamically override the span
	// config limit.
	LimiterLimitOverride func() int64

	// StoreDisableCoalesceAdjacent, if set, disables coalescing of
	// adjacent-and-identical span configs.
	StoreDisableCoalesceAdjacent bool

	// StoreIgnoreCoalesceAdjacentExceptions, if set, ignores the cluster settings
	// spanconfig.{host,tenant}_coalesce_adjacent.enabled. It also allows
	// coalescing system database ranges for the host tenant.
	StoreIgnoreCoalesceAdjacentExceptions bool

	// StoreInternConfigsInDryRuns, if set, will intern span configs even when
	// applying mutations in dry run mode.
	StoreInternConfigsInDryRuns bool
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (t *TestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = (*TestingKnobs)(nil)
