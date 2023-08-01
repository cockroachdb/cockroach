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

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

var tagToAggregatorEventConstructor map[string]func(aggEvent []byte) (TracingAggregatorEvent, error)

// RegisterAggregatorEventConstructor registers a constructor method for a
// particular TracingAggregatorEvent identified by its tag.
func RegisterAggregatorEventConstructor(
	aggEventTag string, constructor func(content []byte) (TracingAggregatorEvent, error),
) {
	if tagToAggregatorEventConstructor == nil {
		tagToAggregatorEventConstructor = make(map[string]func(aggEvent []byte) (TracingAggregatorEvent, error))
	}
	_, ok := tagToAggregatorEventConstructor[aggEventTag]
	if ok {
		panic(fmt.Sprintf("duplicate stringers registered for %s", aggEventTag))
	}
	tagToAggregatorEventConstructor[aggEventTag] = constructor
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
	clusterWideAggregatorStats := make(map[string]TracingAggregatorEvent)
	asOf := timeutil.Now().Format("20060102_150405.00")

	var clusterWideSummary bytes.Buffer
	for component, tagToEvent := range perNodeStats {
		clusterWideSummary.WriteString(fmt.Sprintf("## SQL Instance ID: %s; Flow ID: %s\n\n",
			component.SQLInstanceID.String(), component.FlowID.String()))
		for tag, event := range tagToEvent {
			// Write a proto file per tag. This machine-readable file can be consumed
			// by other places we want to display this information egs: annotated
			// DistSQL diagrams, DBConsole etc.
			filename := fmt.Sprintf("%s/%s/%s.binpb",
				component.SQLInstanceID.String(), tag, asOf)
			if err := jobs.WriteExecutionDetailFile(ctx, filename, event, db, jobID); err != nil {
				return err
			}

			// Construct a single text file that contains information on a per-node
			// basis as well as a cluster-wide aggregate.
			clusterWideSummary.WriteString(fmt.Sprintf("# %s\n", tag))

			f, ok := tagToAggregatorEventConstructor[tag]
			if !ok {
				return errors.Newf("could not find constructor for AggregatorEvent %s", tag)
			}
			aggEvent, err := f(event)
			if err != nil {
				return err
			}
			clusterWideSummary.WriteString(aggEvent.String())
			clusterWideSummary.WriteString("\n")

			if _, ok := clusterWideAggregatorStats[tag]; ok {
				clusterWideAggregatorStats[tag].Combine(aggEvent)
			} else {
				clusterWideAggregatorStats[tag] = aggEvent
			}
		}
	}

	for tag, event := range clusterWideAggregatorStats {
		clusterWideSummary.WriteString("## Cluster-wide\n\n")
		clusterWideSummary.WriteString(fmt.Sprintf("# %s\n", tag))
		clusterWideSummary.WriteString(event.String())
	}

	filename := fmt.Sprintf("aggregatorstats.%s.txt", asOf)
	return jobs.WriteExecutionDetailFile(ctx, filename, clusterWideSummary.Bytes(), db, jobID)
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
	db isql.DB,
	tracingAgg chan *execinfrapb.TracingAggregatorEvents,
) error {
	perNodeAggregatorStats := make(map[execinfrapb.ComponentID]map[string][]byte)

	// AggregatorEvents are periodically received from each node in the DistSQL
	// flow.
	flushTimer := timeutil.NewTimer()
	defer flushTimer.Stop()
	flushTimer.Reset(5 * time.Second)

	for agg := range tracingAgg {
		componentID := execinfrapb.ComponentID{
			FlowID:        agg.FlowID,
			SQLInstanceID: agg.NodeID,
		}

		// Update the running aggregate of the component with the latest received
		// aggregate.
		perNodeAggregatorStats[componentID] = agg.Events

		select {
		case <-flushTimer.C:
			flushTimer.Read = true
			flushTimer.Reset(5 * time.Second)
			// Flush the per-node and cluster wide aggregator stats to the job_info
			// table.
			if err := flushTracingStats(ctx, jobID, db, perNodeAggregatorStats); err != nil {
				return err
			}
		default:
		}
	}
	return nil
}
