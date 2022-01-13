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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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

	// KVSubscriberPostRangefeedStartInterceptor is invoked after the rangefeed
	// is started.
	KVSubscriberPostRangefeedStartInterceptor func()

	// KVSubscriberPreExitInterceptor is invoked right before returning from
	// subscribeInner, after tearing down internal components.
	KVSubscriberPreExitInterceptor func()

	// KVSubscriberOnTimestampAdvanceInterceptor is invoked each time the
	// KVSubscriber has process all updates before the provided timestamp.
	KVSubscriberOnTimestampAdvanceInterceptor func(hlc.Timestamp)

	// KVSubscriberErrorInjectionCh is a way for tests to conveniently inject
	// buffer overflow errors into the subscriber in order to test recovery.
	KVSubscriberErrorInjectionCh chan error

	// StoreKVSubscriberOverride is used to override the KVSubscriber used when
	// setting up a new store.
	StoreKVSubscriberOverride KVSubscriber

	// SQLWatcherOnEventInterceptor, if set, is invoked when the SQLWatcher
	// receives an event on one of its rangefeeds.
	SQLWatcherOnEventInterceptor func() error

	// SQLWatcherCheckpointNoopsEveryDurationOverride, if set, overrides how
	// often the SQLWatcher checkpoints noops.
	SQLWatcherCheckpointNoopsEveryDurationOverride time.Duration

	// ExcludeDroppedDescriptorsFromLookup is used to control if the
	// SQLTranslator ignores dropped descriptors. If enabled, dropped
	// descriptors appear as missing -- a convenient+faster alternative to
	// waiting for the descriptor to actually get GC-ed in tests.
	ExcludeDroppedDescriptorsFromLookup bool

	// ReconcilerInitialInterceptor, if set, is invoked at the very outset of
	// the reconciliation process.
	ReconcilerInitialInterceptor func()
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (t *TestingKnobs) ModuleTestingKnobs() {}

var _ base.ModuleTestingKnobs = (*TestingKnobs)(nil)
