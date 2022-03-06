// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"bytes"
	"context"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// distRestore plans a 2 stage distSQL flow for a distributed restore. It
// streams back progress updates over the given progCh. The first stage is a
// splitAndScatter processor on every node that is running a compatible version.
// Those processors will then route the spans after they have split and
// scattered them to the restore data processors - the second stage. The spans
// should be routed to the node that is the leaseholder of that span. The
// restore data processor will finally download and insert the data, and this is
// reported back to the coordinator via the progCh.
// This method also closes the given progCh.
func distRestore(
	ctx context.Context,
	execCtx sql.JobExecContext,
	jobID int64,
	chunks [][]execinfrapb.RestoreSpanEntry,
	pkIDs map[uint64]bool,
	encryption *jobspb.BackupEncryptionOptions,
	tableRekeys []execinfrapb.TableRekey,
	tenantRekeys []execinfrapb.TenantRekey,
	restoreTime hlc.Timestamp,
	progCh chan *execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
) error {
	defer close(progCh)
	var noTxn *kv.Txn

	dsp := execCtx.DistSQLPlanner()
	evalCtx := execCtx.ExtendedEvalContext()

	if encryption != nil && encryption.Mode == jobspb.EncryptionMode_KMS {
		kms, err := cloud.KMSFromURI(encryption.KMSInfo.Uri, &backupKMSEnv{
			settings: execCtx.ExecCfg().Settings,
			conf:     &execCtx.ExecCfg().ExternalIODirConfig,
		})
		if err != nil {
			return err
		}

		encryption.Key, err = kms.Decrypt(ctx, encryption.KMSInfo.EncryptedDataKey)
		if err != nil {
			return errors.Wrap(err,
				"failed to decrypt data key before starting BackupDataProcessor")
		}
	}
	// Wrap the relevant BackupEncryptionOptions to be used by the Restore
	// processor.
	var fileEncryption *roachpb.FileEncryptionOptions
	if encryption != nil {
		fileEncryption = &roachpb.FileEncryptionOptions{Key: encryption.Key}
	}

	planCtx, sqlInstanceIDs, err := dsp.SetupAllNodesPlanning(ctx, evalCtx, execCtx.ExecCfg())
	if err != nil {
		return err
	}

	splitAndScatterSpecs, err := makeSplitAndScatterSpecs(sqlInstanceIDs, chunks, tableRekeys, tenantRekeys)
	if err != nil {
		return err
	}

	restoreDataSpec := execinfrapb.RestoreDataSpec{
		JobID:        jobID,
		RestoreTime:  restoreTime,
		Encryption:   fileEncryption,
		TableRekeys:  tableRekeys,
		TenantRekeys: tenantRekeys,
		PKIDs:        pkIDs,
	}

	if len(splitAndScatterSpecs) == 0 {
		// We should return an error here as there are no nodes that are compatible,
		// but we should have at least found ourselves.
		return nil
	}

	p := planCtx.NewPhysicalPlan()

	// Plan SplitAndScatter in a round-robin fashion.
	splitAndScatterStageID := p.NewStageOnNodes(sqlInstanceIDs)
	splitAndScatterProcs := make(map[base.SQLInstanceID]physicalplan.ProcessorIdx)

	defaultStream := int32(0)
	rangeRouterSpec := execinfrapb.OutputRouterSpec_RangeRouterSpec{
		Spans:       nil,
		DefaultDest: &defaultStream,
		Encodings: []execinfrapb.OutputRouterSpec_RangeRouterSpec_ColumnEncoding{
			{
				Column:   0,
				Encoding: descpb.DatumEncoding_ASCENDING_KEY,
			},
		},
	}
	for stream, sqlInstanceID := range sqlInstanceIDs {
		startBytes, endBytes, err := routingSpanForSQLInstance(sqlInstanceID)
		if err != nil {
			return err
		}

		span := execinfrapb.OutputRouterSpec_RangeRouterSpec_Span{
			Start:  startBytes,
			End:    endBytes,
			Stream: int32(stream),
		}
		rangeRouterSpec.Spans = append(rangeRouterSpec.Spans, span)
	}
	// The router expects the spans to be sorted.
	sort.Slice(rangeRouterSpec.Spans, func(i, j int) bool {
		return bytes.Compare(rangeRouterSpec.Spans[i].Start, rangeRouterSpec.Spans[j].Start) == -1
	})

	for _, n := range sqlInstanceIDs {
		spec := splitAndScatterSpecs[n]
		if spec == nil {
			// We may have fewer chunks than we have nodes for very small imports. In
			// this case we only want to plan splitAndScatter nodes on a subset of
			// nodes. Note that we still want to plan a RestoreData processor on every
			// node since each entry could be scattered anywhere.
			continue
		}
		proc := physicalplan.Processor{
			SQLInstanceID: n,
			Spec: execinfrapb.ProcessorSpec{
				Core: execinfrapb.ProcessorCoreUnion{SplitAndScatter: splitAndScatterSpecs[n]},
				Post: execinfrapb.PostProcessSpec{},
				Output: []execinfrapb.OutputRouterSpec{
					{
						Type:            execinfrapb.OutputRouterSpec_BY_RANGE,
						RangeRouterSpec: rangeRouterSpec,
					},
				},
				StageID:     splitAndScatterStageID,
				ResultTypes: splitAndScatterOutputTypes,
			},
		}
		pIdx := p.AddProcessor(proc)
		splitAndScatterProcs[n] = pIdx
	}

	// Plan RestoreData.
	restoreDataStageID := p.NewStageOnNodes(sqlInstanceIDs)
	restoreDataProcs := make(map[base.SQLInstanceID]physicalplan.ProcessorIdx)
	for _, sqlInstanceID := range sqlInstanceIDs {
		proc := physicalplan.Processor{
			SQLInstanceID: sqlInstanceID,
			Spec: execinfrapb.ProcessorSpec{
				Input: []execinfrapb.InputSyncSpec{
					{ColumnTypes: splitAndScatterOutputTypes},
				},
				Core:        execinfrapb.ProcessorCoreUnion{RestoreData: &restoreDataSpec},
				Post:        execinfrapb.PostProcessSpec{},
				Output:      []execinfrapb.OutputRouterSpec{{Type: execinfrapb.OutputRouterSpec_PASS_THROUGH}},
				StageID:     restoreDataStageID,
				ResultTypes: []*types.T{},
			},
		}
		pIdx := p.AddProcessor(proc)
		restoreDataProcs[sqlInstanceID] = pIdx
		p.ResultRouters = append(p.ResultRouters, pIdx)
	}

	for _, srcProc := range splitAndScatterProcs {
		slot := 0
		for _, destSQLInstanceID := range sqlInstanceIDs {
			// Streams were added to the range router in the same order that the
			// nodes appeared in `nodes`. Make sure that the `slot`s here are
			// ordered the same way.
			destProc := restoreDataProcs[destSQLInstanceID]
			p.Streams = append(p.Streams, physicalplan.Stream{
				SourceProcessor:  srcProc,
				SourceRouterSlot: slot,
				DestProcessor:    destProc,
				DestInput:        0,
			})
			slot++
		}
	}

	dsp.FinalizePlan(planCtx, p)

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
		nil,   /* clockUpdater */
		evalCtx.Tracing,
		evalCtx.ExecCfg.ContentionRegistry,
		nil, /* testingPushCallback */
	)
	defer recv.Release()

	// Copy the evalCtx, as dsp.Run() might change it.
	evalCtxCopy := *evalCtx
	dsp.Run(planCtx, noTxn, p, recv, &evalCtxCopy, nil /* finishedSetupFn */)()
	return rowResultWriter.Err()
}

// makeSplitAndScatterSpecs returns a map from nodeID to the SplitAndScatter
// spec that should be planned on that node. Given the chunks of ranges to
// import it round-robin distributes the chunks amongst the given nodes.
func makeSplitAndScatterSpecs(
	sqlInstanceIDs []base.SQLInstanceID,
	chunks [][]execinfrapb.RestoreSpanEntry,
	tableRekeys []execinfrapb.TableRekey,
	tenantRekeys []execinfrapb.TenantRekey,
) (map[base.SQLInstanceID]*execinfrapb.SplitAndScatterSpec, error) {
	specsBySQLInstanceID := make(map[base.SQLInstanceID]*execinfrapb.SplitAndScatterSpec)
	for i, chunk := range chunks {
		sqlInstanceID := sqlInstanceIDs[i%len(sqlInstanceIDs)]
		if spec, ok := specsBySQLInstanceID[sqlInstanceID]; ok {
			spec.Chunks = append(spec.Chunks, execinfrapb.SplitAndScatterSpec_RestoreEntryChunk{
				Entries: chunk,
			})
		} else {
			specsBySQLInstanceID[sqlInstanceID] = &execinfrapb.SplitAndScatterSpec{
				Chunks: []execinfrapb.SplitAndScatterSpec_RestoreEntryChunk{{
					Entries: chunk,
				}},
				TableRekeys:  tableRekeys,
				TenantRekeys: tenantRekeys,
			}
		}
	}
	return specsBySQLInstanceID, nil
}
