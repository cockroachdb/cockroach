// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	roachpb "github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	hlc "github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/logtags"
)

// distBackup is used to plan the processors for a distributed backup. It
// streams back progress updates over progCh, which is used to incrementally
// build up the BulkOpSummary.
func distBackup(
	ctx context.Context,
	phs sql.PlanHookState,
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
	var noTxn *kv.Txn

	dsp := phs.DistSQLPlanner()
	evalCtx := phs.ExtendedEvalContext()

	planCtx, _, err := dsp.SetupAllNodesPlanning(ctx, evalCtx, phs.ExecCfg())
	if err != nil {
		return err
	}

	backupSpecs, err := makeBackupDataProcessorSpecs(
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
		phs.User(),
	)
	if err != nil {
		return err
	}

	if len(backupSpecs) == 0 {
		close(progCh)
		return nil
	}

	gatewayNodeID, err := evalCtx.ExecCfg.NodeID.OptionalNodeIDErr(47970)
	if err != nil {
		return err
	}
	p := sql.MakePhysicalPlan(gatewayNodeID)

	// Setup a one-stage plan with one proc per input spec.
	corePlacement := make([]physicalplan.ProcessorCorePlacement, len(backupSpecs))
	i := 0
	for node, spec := range backupSpecs {
		corePlacement[i].NodeID = node
		corePlacement[i].Core.BackupData = spec
		i++
	}

	// All of the progress information is sent through the metadata stream, so we
	// have an empty result stream.
	p.AddNoInputStage(corePlacement, execinfrapb.PostProcessSpec{}, []*types.T{}, execinfrapb.Ordering{})
	p.PlanToStreamColMap = []int{}

	dsp.FinalizePlan(planCtx, &p)

	metaFn := func(_ context.Context, meta *execinfrapb.ProducerMetadata) error {
		if meta.BulkProcessorProgress != nil {
			// Send the progress up a level to be written to the manifest.
			progCh <- meta.BulkProcessorProgress
		}
		return nil
	}

	rowResultWriter := sql.NewRowResultWriter(nil)

	recv := sql.MakeDistSQLReceiver(
		ctx,
		sql.NewMetadataCallbackWriter(rowResultWriter, metaFn),
		tree.Rows,
		nil,   /* rangeCache */
		noTxn, /* txn - the flow does not read or write the database */
		func(ts hlc.Timestamp) {},
		evalCtx.Tracing,
	)
	defer recv.Release()

	defer close(progCh)
	// Copy the evalCtx, as dsp.Run() might change it.
	evalCtxCopy := *evalCtx
	dsp.Run(planCtx, noTxn, &p, recv, &evalCtxCopy, nil /* finishedSetupFn */)()
	return rowResultWriter.Err()
}

func makeBackupDataProcessorSpecs(
	planCtx *sql.PlanningCtx,
	dsp *sql.DistSQLPlanner,
	spans roachpb.Spans,
	introducedSpans roachpb.Spans,
	pkIDs map[uint64]bool,
	defaultURI string,
	urisByLocalityKV map[string]string,
	mvccFilter roachpb.MVCCFilter,
	encryption *roachpb.FileEncryptionOptions,
	startTime, endTime hlc.Timestamp,
	user string,
) (map[roachpb.NodeID]*execinfrapb.BackupDataSpec, error) {
	var spanPartitions []sql.SpanPartition
	var introducedSpanPartitions []sql.SpanPartition
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
	nodeToSpec := make(map[roachpb.NodeID]*execinfrapb.BackupDataSpec)
	for _, partition := range spanPartitions {
		spec := &execinfrapb.BackupDataSpec{
			Spans:            partition.Spans,
			DefaultURI:       defaultURI,
			URIsByLocalityKV: urisByLocalityKV,
			MVCCFilter:       mvccFilter,
			Encryption:       encryption,
			PKIDs:            pkIDs,
			BackupStartTime:  startTime,
			BackupEndTime:    endTime,
			User:             user,
		}
		nodeToSpec[partition.Node] = spec
	}

	for _, partition := range introducedSpanPartitions {
		if spec, ok := nodeToSpec[partition.Node]; ok {
			spec.IntroducedSpans = partition.Spans
		} else {
			// We may need to introduce a new spec in the case that there is a node
			// which is not the leaseholder for any of the spans, but is for an
			// introduced span.
			spec := &execinfrapb.BackupDataSpec{
				IntroducedSpans:  partition.Spans,
				DefaultURI:       defaultURI,
				URIsByLocalityKV: urisByLocalityKV,
				MVCCFilter:       mvccFilter,
				Encryption:       encryption,
				PKIDs:            pkIDs,
				BackupStartTime:  startTime,
				BackupEndTime:    endTime,
				User:             user,
			}
			nodeToSpec[partition.Node] = spec
		}
	}

	return nodeToSpec, nil
}
