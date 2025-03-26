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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
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
			log.Warningf(ctx, "failed to unmarshal aggregated event: %v", err.Error())
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
		for name, stats := range nodeStats.SpanTotals {
			fmt.Fprintf(&perNode, "- %-40s (%d):\t%s\n", name, stats.Count, stats.Duration)
		}
		perNode.WriteString("\n")

		// Add span stats to the cluster-wide span stats.
		for spanName, totals := range nodeStats.SpanTotals {
			clusterWideSpanStats[spanName] = clusterWideSpanStats[spanName].Combine(totals)
		}

		perNode.WriteString("## Aggregate Stats\n\n")
		for name, event := range nodeStats.Events {
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
	for name, stats := range clusterWideSpanStats {
		fmt.Fprintf(&combined, " - %-40s (%d):\t%s\n", name, stats.Count, stats.Duration)
	}
	combined.WriteString("\n")
	combined.WriteString("## Aggregate Stats\n\n")
	for name, ev := range clusterWideAggregatorStats {
		fmt.Fprintf(&combined, " - %s:\n%s\n", name, ev)
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
