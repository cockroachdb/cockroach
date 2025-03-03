// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package bulk

import (
	"bytes"
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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

	return &execinfrapb.ProducerMetadata{AggregatorEvents: aggEvents}
}

// ComponentAggregatorStats is a mapping from a component to all the Aggregator
// Stats collected for that component.
type ComponentAggregatorStats map[execinfrapb.ComponentID]map[string][]byte

// DeepCopy takes a deep copy of the component aggregator stats map.
func (c ComponentAggregatorStats) DeepCopy() ComponentAggregatorStats {
	mapCopy := make(ComponentAggregatorStats, len(c))
	for k, v := range c {
		innerMap := make(map[string][]byte, len(v))
		for k2, v2 := range v {
			// Create a copy of the byte slice to avoid modifying the original data.
			dataCopy := make([]byte, len(v2))
			copy(dataCopy, v2)
			innerMap[k2] = dataCopy
		}
		mapCopy[k] = innerMap
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
	return db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		clusterWideAggregatorStats := make(map[string]tracing.AggregatorEvent)
		asOf := timeutil.Now().Format("20060102_150405.00")

		var clusterWideSummary bytes.Buffer
		for component, nameToEvent := range perNodeAggregatorStats {
			clusterWideSummary.WriteString(fmt.Sprintf("## SQL Instance ID: %s; Flow ID: %s\n\n",
				component.SQLInstanceID.String(), component.FlowID.String()))
			for name, event := range nameToEvent {
				// Write a proto file per tag. This machine-readable file can be consumed
				// by other places we want to display this information egs: annotated
				// DistSQL diagrams, DBConsole etc.
				filename := fmt.Sprintf("%s/%s",
					component.SQLInstanceID.String(), asOf)
				msg, err := protoreflect.DecodeMessage(name, event)
				if err != nil {
					clusterWideSummary.WriteString(fmt.Sprintf("invalid protocol message: %v", err))
					// If we failed to decode the event write the error to the file and
					// carry on.
					continue
				}

				if err := jobs.WriteProtobinExecutionDetailFile(ctx, filename, msg, txn, jobID); err != nil {
					return err
				}

				// Construct a single text file that contains information on a per-node
				// basis as well as a cluster-wide aggregate.
				clusterWideSummary.WriteString(fmt.Sprintf("# %s\n", name))

				aggEvent := msg.(tracing.AggregatorEvent)
				clusterWideSummary.WriteString(aggEvent.String())
				clusterWideSummary.WriteString("\n")

				if _, ok := clusterWideAggregatorStats[name]; ok {
					clusterWideAggregatorStats[name].Combine(aggEvent)
				} else {
					clusterWideAggregatorStats[name] = aggEvent
				}
			}
		}

		for tag, event := range clusterWideAggregatorStats {
			clusterWideSummary.WriteString("## Cluster-wide\n\n")
			clusterWideSummary.WriteString(fmt.Sprintf("# %s\n", tag))
			clusterWideSummary.WriteString(event.String())
		}

		// Ensure the file always has a trailing newline, regardless of whether or
		// not the loops above wrote anything.
		clusterWideSummary.WriteString("\n")
		filename := fmt.Sprintf("aggregatorstats.%s.txt", asOf)
		return jobs.WriteExecutionDetailFile(ctx, filename, clusterWideSummary.Bytes(), txn, jobID)
	})
}
