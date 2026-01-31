// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulk

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
)

// ConstructTracingAggregatorProducerMeta constructs a ProducerMetadata that
// contains all the aggregated events stored in the tracing aggregator.
func ConstructTracingAggregatorProducerMeta(
	ctx context.Context,
	sqlInstanceID base.SQLInstanceID,
	flowID execinfrapb.FlowID,
	agg *tracing.TracingAggregator,
) *execinfrapb.ProducerMetadata {
	aggEvents := &execinfrapb.TracingAggregatorEvents{
		SQLInstanceID: sqlInstanceID,
		FlowID:        flowID,
		Events:        make(map[string][]byte),
	}

	agg.ForEachAggregatedEvent(func(name string, event tracing.AggregatorEvent) {
		if data, err := tracing.AggregatorEventToBytes(ctx, event); err != nil {
			// This should never happen but if it does skip the aggregated event.
			log.Dev.Warningf(ctx, "failed to unmarshal aggregated event: %v", err.Error())
			return
		} else {
			aggEvents.Events[name] = data
		}
	})

	sp := tracing.SpanFromContext(ctx)
	if sp != nil {
		recType := sp.RecordingType()
		if recType != tracingpb.RecordingOff {
			aggEvents.SpanTotals = sp.GetFullTraceRecording(recType).Root.ChildrenMetadata
		}
	}
	return &execinfrapb.ProducerMetadata{AggregatorEvents: aggEvents}
}

// ComponentAggregatorStats is a mapping from a component to all the Aggregator
// Stats collected for that component.
type ComponentAggregatorStats map[execinfrapb.ComponentID]execinfrapb.TracingAggregatorEvents

// DeepCopy takes a deep copy of the component aggregator stats map.
func (c ComponentAggregatorStats) DeepCopy() ComponentAggregatorStats {
	mapCopy := make(ComponentAggregatorStats, len(c))
	for k, v := range c {
		copied := v
		copied.Events = make(map[string][]byte, len(v.Events))
		copied.SpanTotals = make(map[string]tracingpb.OperationMetadata, len(v.SpanTotals))
		for k2, v2 := range v.Events {
			// Create a copy of the byte slice to avoid modifying the original data.
			dataCopy := make([]byte, len(v2))
			copy(dataCopy, v2)
			copied.Events[k2] = dataCopy
		}
		for k2, v2 := range v.SpanTotals {
			copied.SpanTotals[k2] = v2
		}
		mapCopy[k] = copied
	}
	return mapCopy
}

func sortedKeys[T any](m map[string]T) []string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// FlushTracingAggregatorStats persists the following files to the
// `system.job_info` table for consumption by job observability tools:
//
// - A file per node, for each aggregated AggregatorEvent. These files
// contain the machine-readable proto bytes of the AggregatorEvent.
//
// - A text file that contains a cluster-wide and per-node summary of each
// AggregatorEvent in its human-readable format.
func FlushTracingAggregatorStats(
	ctx context.Context,
	jobID jobspb.JobID,
	db isql.DB,
	perNodeAggregatorStats ComponentAggregatorStats,
) error {
	clusterWideSpanStats := make(map[string]tracingpb.OperationMetadata)
	clusterWideAggregatorStats := make(map[string]tracing.AggregatorEvent)
	ids := make([]execinfrapb.ComponentID, 0, len(perNodeAggregatorStats))

	for component := range perNodeAggregatorStats {
		ids = append(ids, component)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i].SQLInstanceID < ids[j].SQLInstanceID })

	// Write a summary for each per-node to a buffer. While doing so, accumulate a
	// cluster-wide summary as well to be written to a second buffer below.
	var perNode bytes.Buffer
	fmt.Fprintf(&perNode, "# Per-componant Details (%d)\n", len(perNodeAggregatorStats))
	for _, component := range ids {
		nodeStats := perNodeAggregatorStats[component]
		fmt.Fprintf(&perNode, "# SQL Instance ID: %s (%s); Flow/proc ID: %s/%d\n\n",
			component.SQLInstanceID, component.Region, component.FlowID, component.ID)

		// Print span stats.
		perNode.WriteString("## Span Totals\n\n")
		for _, name := range sortedKeys(nodeStats.SpanTotals) {
			stats := nodeStats.SpanTotals[name]
			fmt.Fprintf(&perNode, "- %-40s (%d):\t%s\n", name, stats.Count, stats.Duration)
		}
		perNode.WriteString("\n")

		// Add span stats to the cluster-wide span stats.
		for spanName, totals := range nodeStats.SpanTotals {
			clusterWideSpanStats[spanName] = clusterWideSpanStats[spanName].Combine(totals)
		}

		perNode.WriteString("## Aggregate Stats\n\n")
		for _, name := range sortedKeys(nodeStats.Events) {
			event := nodeStats.Events[name]
			msg, err := protoreflect.DecodeMessage(name, event)
			if err != nil {
				continue
			}
			aggEvent := msg.(tracing.AggregatorEvent)
			fmt.Fprintf(&perNode, "- %s:\n%s\n\n", name, aggEvent)

			// Populate the cluster-wide aggregator stats.
			if _, ok := clusterWideAggregatorStats[name]; ok {
				clusterWideAggregatorStats[name].Combine(aggEvent)
			} else {
				clusterWideAggregatorStats[name] = aggEvent
			}
		}
		perNode.WriteString("\n")
	}

	// Write the cluster-wide summary.
	var combined bytes.Buffer
	combined.WriteString("# Cluster-wide\n\n")
	combined.WriteString("## Span Totals\n\n")
	for _, name := range sortedKeys(clusterWideSpanStats) {
		stats := clusterWideSpanStats[name]
		fmt.Fprintf(&combined, " - %-40s (%d):\t%s\n", name, stats.Count, stats.Duration)
	}
	combined.WriteString("\n")
	combined.WriteString("## Aggregate Stats\n\n")
	for _, name := range sortedKeys(clusterWideAggregatorStats) {
		fmt.Fprintf(&combined, " - %s:\n%s\n", name, clusterWideAggregatorStats[name])
	}
	combined.WriteString("\n")

	return db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		asOf := timeutil.Now().Format("20060102_150405.00")
		combinedFilename := fmt.Sprintf("%s/trace-stats-cluster-wide.txt", asOf)
		perNodeFilename := fmt.Sprintf("%s/trace-stats-by-node.txt", asOf)
		if err := jobs.WriteExecutionDetailFile(ctx, combinedFilename, combined.Bytes(), txn, jobID); err != nil {
			return err
		}
		return jobs.WriteExecutionDetailFile(ctx, perNodeFilename, perNode.Bytes(), txn, jobID)
	})
}

type TraceBasedDelayLogger interface {
	Process(ctx context.Context, latestTrace ComponentAggregatorStats)
}

type TraceDelayReader interface {
	// Work returns the amount of time spent doing and/or waiting for work to be
	// done, using the prior and current trace info.
	Work(prior, now ComponentAggregatorStats) time.Duration
	// Delay returns the amount of time spent waiting for some cause of delays
	// while doing work or waiting for work to be done, i.e. a subset of the time
	// returned by Work.
	Delay(prior, now ComponentAggregatorStats) (time.Duration, error)
}

type delayNotifer interface {
	// Notify is called with the delay fraction over the last `since` duration,
	// and chooses if and how to nofify the user.
	Notify(ctx context.Context, since time.Duration, frac float64) error
}

type traceDelayLogger struct {
	lastTrace     ComponentAggregatorStats
	lastTraceTime time.Time
	infoFreq      log.EveryN
	reader        TraceDelayReader
	notifier      delayNotifer
}

// Process implements the TraceBasedDelayLogger interface.
func (a *traceDelayLogger) Process(
	ctx context.Context, latestTrace ComponentAggregatorStats,
) error {
	now := timeutil.Now()
	elapsed := a.reader.Work(a.lastTrace, latestTrace)
	delay, err := a.reader.Delay(a.lastTrace, latestTrace)
	if err != nil {
		return err
	}
	a.lastTrace = latestTrace
	since := now.Sub(a.lastTraceTime)
	a.lastTraceTime = timeutil.Now()

	if delay == 0 {
		return nil
	}
	if a.infoFreq.ShouldLog() {
		if delay > elapsed {
			// TODO(dt): should we just bail out if elapsed == 0, i.e. we don't have a
			// meaningful denominator to compare against?
			log.Infof(ctx, "job reported delay in excess of total work time: %s vs %s", delay, elapsed)
			elapsed = delay
		}
		log.Infof(ctx, "job delayed by %s across %s of work", delay, elapsed)
	}
	return a.notifier.Notify(ctx, since, float64(delay)/float64(elapsed))
}

// jobDelayLogger is a delay notifier that logs to the job's message log.
type jobDelayLogger struct {
	jobID     jobspb.JobID
	threshold *settings.FloatSetting
	execCfg   *sql.ExecutorConfig
	freq      log.EveryN
	cause     string
}

// Notify implements the delayNotifer interface.
func (l *jobDelayLogger) Notify(ctx context.Context, since time.Duration, f float64) error {
	if !l.execCfg.Settings.Version.IsActive(ctx, clusterversion.V25_2) {
		return nil
	}
	if threshold := l.threshold.Get(l.execCfg.SV()); threshold > 0 && f > threshold && l.freq.ShouldLog() {
		msg := fmt.Sprintf("job execution over last %s delayed by %d%% due to %s", humanizeutil.LongDuration(since), int(f*100), l.cause)

		return l.execCfg.InternalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			return jobs.MessageStorage(l.jobID).Record(ctx, txn, "overload", msg)
		})
	}
	return nil
}

// NewJobDelayLogger creates a new job delay logger that will log to the job's
// message log if the ratio of delay, as computed by the passed reader, to the
// work time, also computed by said reader, exceeds the value specified by the
// passed threshold setting, logging a message no more often than the passed
// frequency. The message will specify the ratio and the passed cause string.
func NewJobDelayLogger(
	jobID jobspb.JobID,
	execCfg *sql.ExecutorConfig,
	msgFreq time.Duration,
	threshold *settings.FloatSetting,
	reader TraceDelayReader,
	cause string,
) *traceDelayLogger {
	return &traceDelayLogger{
		notifier: &jobDelayLogger{
			jobID:     jobID,
			execCfg:   execCfg,
			threshold: threshold,
			freq:      log.Every(msgFreq),
			cause:     cause,
		},
		reader:   reader,
		infoFreq: log.Every(time.Minute),
	}
}
