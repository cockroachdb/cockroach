// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/logtags"
)

// DistBackup is used to plan the processors for a distributed backup. It
// streams back progress updates over progCh, which is used to incrementally
// build up the BulkOpSummary.
func DistBackup(
	ctx context.Context,
	phs PlanHookState,
	spans roachpb.Spans,
	introducedSpans roachpb.Spans,
	pkIDs map[uint64]bool,
	defaultURI string,
	urisByLocalityKV map[string]string,
	encryption *roachpb.FileEncryptionOptions,
	mvccFilter roachpb.MVCCFilter,
	startTime, endTime hlc.Timestamp,
	progCh chan *execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
) error {
	ctx = logtags.AddTag(ctx, "backup-distsql", nil)

	dsp := phs.DistSQLPlanner()
	evalCtx := phs.ExtendedEvalContext()

	planCtx, nodes, err := dsp.setupAllNodesPlanning(ctx, evalCtx, phs.ExecCfg())
	if err != nil {
		return err
	}

	exportSpecs, err := makeExportDataProcessorSpecs(
		planCtx,
		dsp,
		spans,
		introducedSpans,
		pkIDs,
		defaultURI,
		urisByLocalityKV,
		mvccFilter,
		encryption,
		startTime, endTime,
	)
	if err != nil {
		return err
	}

	if len(exportSpecs) == 0 {
		close(progCh)
		return nil
	}

	var p PhysicalPlan

	// Setup a one-stage plan with one proc per input spec.
	stageID := p.NewStageID()
	p.ResultRouters = make([]physicalplan.ProcessorIdx, len(exportSpecs))
	for i, rcs := range exportSpecs {
		proc := physicalplan.Processor{
			Node: nodes[i],
			Spec: execinfrapb.ProcessorSpec{
				Core:    execinfrapb.ProcessorCoreUnion{ExportData: rcs},
				Output:  []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
				StageID: stageID,
			},
		}
		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}

	// All of the progress information is sent through the metadata stream, so we
	// have an empty result stream.
	p.PlanToStreamColMap = []int{}
	p.ResultTypes = []*types.T{}

	dsp.FinalizePlan(planCtx, &p)

	metaFn := func(_ context.Context, meta *execinfrapb.ProducerMetadata) error {
		if meta.BulkProcessorProgress != nil {
			// Send the progress up a level to be written to the manifest.
			progCh <- meta.BulkProcessorProgress
		}
		return nil
	}

	rowResultWriter := NewRowResultWriter(nil)

	recv := MakeDistSQLReceiver(
		ctx,
		&metadataCallbackWriter{rowResultWriter: rowResultWriter, fn: metaFn},
		tree.Rows,
		nil, /* rangeCache */
		nil, /* leaseCache */
		nil, /* txn - the flow does not read or write the database */
		func(ts hlc.Timestamp) {},
		evalCtx.Tracing,
	)
	defer recv.Release()

	g := ctxgroup.WithContext(ctx)

	g.GoCtx(func(ctx context.Context) error {
		defer close(progCh)
		// Copy the evalCtx, as dsp.Run() might change it.
		evalCtxCopy := *evalCtx
		dsp.Run(planCtx, nil, &p, recv, &evalCtxCopy, nil /* finishedSetupFn */)()
		return rowResultWriter.Err()
	})

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}

func makeExportDataProcessorSpecs(
	planCtx *PlanningCtx,
	dsp *DistSQLPlanner,
	spans roachpb.Spans,
	introducedSpans roachpb.Spans,
	pkIDs map[uint64]bool,
	defaultURI string,
	urisByLocalityKV map[string]string,
	mvccFilter roachpb.MVCCFilter,
	encryption *roachpb.FileEncryptionOptions,
	startTime, endTime hlc.Timestamp,
) ([]*execinfrapb.ExportDataSpec, error) {
	var spanPartitions []SpanPartition
	var introducedSpanPartitions []SpanPartition
	var err error
	if len(spans) > 0 {
		spanPartitions, err = dsp.PartitionSpans(planCtx, spans)
		if err != nil {
			return nil, err
		}
	}
	if len(introducedSpans) > 0 {
		introducedSpanPartitions, err = dsp.PartitionSpans(planCtx, introducedSpans)
		if err != nil {
			return nil, err
		}
	}

	// First construct spans based on span partitions. Then add on
	// introducedSpans based on how those partition.
	nodeToSpec := make(map[roachpb.NodeID]*execinfrapb.ExportDataSpec)
	exportSpecs := make([]*execinfrapb.ExportDataSpec, 0, len(spanPartitions))
	for _, partition := range spanPartitions {
		spec := &execinfrapb.ExportDataSpec{
			Spans:            partition.Spans,
			DefaultURI:       defaultURI,
			URIsByLocalityKV: urisByLocalityKV,
			MVCCFilter:       mvccFilter,
			Encryption:       encryption,
			PKIDs:            pkIDs,
			BackupStartTime:  startTime,
			BackupEndTime:    endTime,
		}
		nodeToSpec[partition.Node] = spec
		exportSpecs = append(exportSpecs, spec)
	}

	for _, partition := range introducedSpanPartitions {
		if spec, ok := nodeToSpec[partition.Node]; ok {
			spec.IntroducedSpans = partition.Spans
		} else {
			// We may need to introduce a new spec in the case that there is a node
			// which is not the leaseholder for any of the spans, but is for an
			// introduced span.
			spec := &execinfrapb.ExportDataSpec{
				IntroducedSpans:  partition.Spans,
				DefaultURI:       defaultURI,
				URIsByLocalityKV: urisByLocalityKV,
				MVCCFilter:       mvccFilter,
				Encryption:       encryption,
				PKIDs:            pkIDs,
				BackupStartTime:  startTime,
				BackupEndTime:    endTime,
			}
			nodeToSpec[partition.Node] = spec
			exportSpecs = append(exportSpecs, spec)
		}
	}

	return exportSpecs, nil
}
