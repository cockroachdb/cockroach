// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvfeed"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
)

// TestingKnobs are the testing knobs for changefeed.
type TestingKnobs struct {
	// BeforeEmitRow is called before every sink emit row operation.
	BeforeEmitRow func(context.Context) error
	// MemMonitor, if non-nil, overrides memory monitor to use for changefeed..
	MemMonitor *mon.BytesMonitor
	// HandleDistChangfeedError is called with the result error from
	// the distributed changefeed.
	HandleDistChangefeedError func(error) error
	// WrapSink, if set, is a function that is invoked before the Sink is returned.
	// It allows the tests to muck with the Sink, and even return altogether different
	// implementation.
	WrapSink func(s Sink, jobID jobspb.JobID) Sink
	// ShouldSkipResolved is a filter returning true if the resolved span event should
	// be skipped.
	ShouldSkipResolved func(resolved *jobspb.ResolvedSpan) bool
	// FeedKnobs are kvfeed testing knobs.
	FeedKnobs kvfeed.TestingKnobs
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
