// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package bulk

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var flushTracingAggregatorFrequency = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"jobs.aggregator_stats_flush_frequency",
	"frequency at which the coordinator node processes and persists tracing aggregator stats to storage",
	10*time.Minute,
)

// ConstructTracingAggregatorProducerMeta constructs a ProducerMetadata that
// contains all the aggregated events stored in the tracing aggregator.
func ConstructTracingAggregatorProducerMeta(
	ctx context.Context,
	sqlInstanceID base.SQLInstanceID,
	flowID execinfrapb.FlowID,
	agg *TracingAggregator,
) *execinfrapb.ProducerMetadata {
	aggEvents := &execinfrapb.TracingAggregatorEvents{
		SQLInstanceID: sqlInstanceID,
		FlowID:        flowID,
		Events:        make(map[string][]byte),
	}

	agg.ForEachAggregatedEvent(func(name string, event TracingAggregatorEvent) {
		if data, err := TracingAggregatorEventToBytes(ctx, event); err != nil {
			// This should never happen but if it does skip the aggregated event.
			log.Warningf(ctx, "failed to unmarshal aggregated event: %v", err.Error())
			return
		} else {
			aggEvents.Events[name] = data
		}
	})

	return &execinfrapb.ProducerMetadata{AggregatorEvents: aggEvents}
}

// flushTracingStats persists the following files to the `system.job_info` table
// for consumption by job observability tools:
//
// - A file per node, for each aggregated TracingAggregatorEvent. These files
// contain the machine-readable proto bytes of the TracingAggregatorEvent.
//
// - A text file that contains a cluster-wide and per-node summary of each
// TracingAggregatorEvent in its human-readable format.
func flushTracingStats(
	ctx context.Context,
	jobID jobspb.JobID,
	db isql.DB,
	perNodeStats map[execinfrapb.ComponentID]map[string][]byte,
) error {
	return db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		clusterWideAggregatorStats := make(map[string]TracingAggregatorEvent)
		asOf := timeutil.Now().Format("20060102_150405.00")

		var clusterWideSummary bytes.Buffer
		for component, nameToEvent := range perNodeStats {
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

				aggEvent := msg.(TracingAggregatorEvent)
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

		filename := fmt.Sprintf("aggregatorstats.%s.txt", asOf)
		return jobs.WriteExecutionDetailFile(ctx, filename, clusterWideSummary.Bytes(), txn, jobID)
	})
}

// AggregateTracingStats listens for AggregatorEvents on a channel and
// periodically processes them to human and machine-readable file formats that
// are persisted in the system.job_info table. These files can then be consumed
// for improved observability into the job's execution.
//
// This method does not return until the passed in channel is closed or an error
// is encountered.
func AggregateTracingStats(
	ctx context.Context,
	jobID jobspb.JobID,
	st *cluster.Settings,
	db isql.DB,
	tracingAgg chan *execinfrapb.TracingAggregatorEvents,
) error {
	if !st.Version.IsActive(ctx, clusterversion.V23_2Start) {
		return errors.Newf("aggregator stats are supported when the cluster version >= %s",
			clusterversion.V23_2Start.String())
	}
	perNodeAggregatorStats := make(map[execinfrapb.ComponentID]map[string][]byte)

	// AggregatorEvents are periodically received from each node in the DistSQL
	// flow.
	flushTimer := timeutil.NewTimer()
	defer flushTimer.Stop()
	flushTimer.Reset(flushTracingAggregatorFrequency.Get(&st.SV))

	var flushOnClose bool
	for agg := range tracingAgg {
		flushOnClose = true
		componentID := execinfrapb.ComponentID{
			FlowID:        agg.FlowID,
			SQLInstanceID: agg.SQLInstanceID,
		}

		// Update the running aggregate of the component with the latest received
		// aggregate.
		perNodeAggregatorStats[componentID] = agg.Events

		select {
		case <-flushTimer.C:
			flushTimer.Read = true
			flushTimer.Reset(flushTracingAggregatorFrequency.Get(&st.SV))
			// Flush the per-node and cluster wide aggregator stats to the job_info
			// table.
			if err := flushTracingStats(ctx, jobID, db, perNodeAggregatorStats); err != nil {
				return err
			}
			flushOnClose = false
		default:
		}
	}
	if flushOnClose {
		if err := flushTracingStats(ctx, jobID, db, perNodeAggregatorStats); err != nil {
			return err
		}
	}
	return nil
}
