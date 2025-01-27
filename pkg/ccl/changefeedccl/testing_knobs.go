// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvfeed"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/resolvedspan"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// TestingKnobs are the testing knobs for changefeed.
type TestingKnobs struct {
	// BeforeEmitRow is called before every sink emit row operation.
	BeforeEmitRow func(context.Context) error
	// MemMonitor, if non-nil, overrides memory monitor to use for changefeed..
	MemMonitor *mon.BytesMonitor
	// BeforeDistChangefeed invoked before dist changefeed starts.
	BeforeDistChangefeed func()
	// HandleDistChangfeedError is called with the result error from
	// the distributed changefeed.
	HandleDistChangefeedError func(error) error
	// WrapSink, if set, is a function that is invoked before the Sink is returned.
	// It allows the tests to muck with the Sink, and even return altogether different
	// implementation.
	WrapSink func(s Sink, jobID jobspb.JobID) Sink
	// PubsubClientSkipClientCreation, if set, skips creating a google cloud
	// client as it is expected that the test manually sets a client.
	PubsubClientSkipClientCreation bool
	// FilterSpanWithMutation is a filter returning true if the resolved span event should
	// be skipped. This method takes a pointer in case resolved spans need to be mutated.
	FilterSpanWithMutation func(resolved *jobspb.ResolvedSpan) (bool, error)
	// FeedKnobs are kvfeed testing knobs.
	FeedKnobs kvfeed.TestingKnobs
	// NullSinkIsExternalIOAccounted controls whether we record
	// tenant usage for the null sink. By default the null sink is
	// not accounted but it is useful to treat it as accounted in
	// tests.
	NullSinkIsExternalIOAccounted bool
	// OnDistflowSpec is called when specs for distflow planning have been created
	OnDistflowSpec func(aggregatorSpecs []*execinfrapb.ChangeAggregatorSpec, frontierSpec *execinfrapb.ChangeFrontierSpec)
	// RaiseRetryableError is a knob used to possibly return an error.
	RaiseRetryableError func() error
	// StartDistChangefeedInitialHighwater is called when starting the dist changefeed with the initial highwater
	// of the changefeed. Note that this will be called when the changefeed starts and subsequently when the changefeed
	// is retried.
	StartDistChangefeedInitialHighwater func(ctx context.Context, initialHighwater hlc.Timestamp)
	// LoadJobErr is called when the changefeed loads the job record during a retry to check for progress updates.
	LoadJobErr func() error
	// This is currently used to test negative timestamp in cursor i.e of the form
	// "-3us". Check TestChangefeedCursor for more info. This function needs to be in the
	// knobs as current statement time will only be available once the create changefeed statement
	// starts executing.
	OverrideCursor func(currentTime *hlc.Timestamp) string

	// FilterDrainingNodes is a callback that's invoked by changefeed dist planner
	// in order to filter draining nodes from the list of eligible nodes.
	// Normally, we rely on dist sql planner to do that for us.
	FilterDrainingNodes func(
		partitions []sql.SpanPartition, draining []roachpb.NodeID,
	) ([]sql.SpanPartition, error)

	// ShouldCheckpointToJobRecord returns true if change frontier should checkpoint itself
	// to the job record.
	ShouldCheckpointToJobRecord func(hw hlc.Timestamp) bool

	// OnDrain returns the channel to select on to detect node drain
	OnDrain func() <-chan struct{}

	// SpanPartitionsCallback is called with the span partition
	// when the changefeed is planned.
	SpanPartitionsCallback func([]sql.SpanPartition)

	// PreserveDeprecatedPts is used to prevent a changefeed from upgrading
	// its PTS record from the deprecated style to the new style.
	PreserveDeprecatedPts func() bool

	// PreservePTSTargets is used to prevent a changefeed from upgrading
	// its PTS record to include all required targets.
	PreservePTSTargets func() bool

	// PulsarClientSkipCreation skips creating the sink client when
	// dialing.
	PulsarClientSkipCreation bool

	// TimeSource is used to override the time source used by the changefeed (currently only used by the usage metric goroutine).
	TimeSource timeutil.TimeSource

	// OverrideExecCfg returns a modified ExecutorConfig to use under tests.
	OverrideExecCfg func(actual *sql.ExecutorConfig) *sql.ExecutorConfig

	// AsyncFlushSync is called in async flush goroutines as a way to provide synchronization between them.
	AsyncFlushSync func()

	// AfterCoordinatorFrontierRestore is called on the start of the changefeed coordinator so we can
	// make assertions about its frontier.
	AfterCoordinatorFrontierRestore func(frontier *resolvedspan.CoordinatorFrontier)

	// WrapTelemetryLogger is used to wrap the periodic telemetry logger in tests.
	WrapTelemetryLogger func(logger telemetryLogger) telemetryLogger
}

// ModuleTestingKnobs is part of the base.ModuleTestingKnobs interface.
func (*TestingKnobs) ModuleTestingKnobs() {}
