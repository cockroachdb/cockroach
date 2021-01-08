// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streamclient"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/logtags"
)

func distStreamIngestionPlanSpecs(
	topology streamclient.Topology, nodes []roachpb.NodeID,
) ([]*execinfrapb.StreamIngestionDataSpec, error) {

	// For each stream partition in the topology, assign it to a node.
	streamIngestionSpecs := make([]*execinfrapb.StreamIngestionDataSpec, 0, len(nodes))

	for i, partition := range topology.Partitions {
		// Round robin assign the stream partitions to nodes. Partitions 0 through
		// len(nodes) - 1 creates the spec. Future partitions just add themselves to
		// the partition addresses.
		if i < len(nodes) {
			spec := &execinfrapb.StreamIngestionDataSpec{
				PartitionAddress: make([]streamclient.PartitionAddress, 0),
			}
			streamIngestionSpecs = append(streamIngestionSpecs, spec)
		}
		n := i % len(nodes)
		streamIngestionSpecs[n].PartitionAddress = append(streamIngestionSpecs[n].PartitionAddress, partition)
	}

	return streamIngestionSpecs, nil
}

func distStreamIngest(
	ctx context.Context,
	execCtx sql.JobExecContext,
	nodes []roachpb.NodeID,
	planCtx *sql.PlanningCtx,
	dsp *sql.DistSQLPlanner,
	streamIngestionSpecs []*execinfrapb.StreamIngestionDataSpec,
) error {
	ctx = logtags.AddTag(ctx, "stream-ingest-distsql", nil)
	evalCtx := execCtx.ExtendedEvalContext()
	var noTxn *kv.Txn

	if len(streamIngestionSpecs) == 0 {
		return nil
	}

	// Setup a one-stage plan with one proc per input spec.
	corePlacement := make([]physicalplan.ProcessorCorePlacement, len(streamIngestionSpecs))
	for i := range streamIngestionSpecs {
		corePlacement[i].NodeID = nodes[i]
		corePlacement[i].Core.StreamIngestionData = streamIngestionSpecs[i]
	}

	p := planCtx.NewPhysicalPlan()
	p.AddNoInputStage(
		corePlacement,
		execinfrapb.PostProcessSpec{},
		streamIngestionResultTypes,
		execinfrapb.Ordering{},
	)

	// TODO(adityamaru): It is likely that we will add a StreamIngestFrontier
	// processor on the coordinator node. All the StreamIngestionProcessors will
	// feed their results into this frontier. This is similar to the relationship
	// between the ChangeAggregator and ChangeFrontier processors. The
	// StreamIngestFrontier will be responsible for updating the job watermark
	// with the min of the resolved ts outputted by all the processors.

	// TODO(adityamaru): Once result types are updated, add PlanToStreamColMap.
	dsp.FinalizePlan(planCtx, p)

	recv := sql.MakeDistSQLReceiver(
		ctx,
		// TODO(adityamaru): Are there any results we want to surface to the user?
		nil, /* resultWriter */
		tree.Rows,
		nil, /* rangeCache */
		noTxn,
		nil, /* clockUpdater */
		evalCtx.Tracing,
	)
	defer recv.Release()

	// Copy the evalCtx, as dsp.Run() might change it.
	evalCtxCopy := *evalCtx
	dsp.Run(planCtx, noTxn, p, recv, &evalCtxCopy, nil /* finishedSetupFn */)
	return nil
}
