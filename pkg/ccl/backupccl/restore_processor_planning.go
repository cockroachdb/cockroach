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
	"math"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type restoreJobMetadata struct {
	jobID              jobspb.JobID
	dataToRestore      restorationData
	restoreTime        hlc.Timestamp
	encryption         *jobspb.BackupEncryptionOptions
	kmsEnv             cloud.KMSEnv
	uris               []string
	backupLocalityInfo []jobspb.RestoreDetails_BackupLocalityInfo
	spanFilter         spanCoveringFilter
	numImportSpans     int
	execLocality       roachpb.Locality
	exclusiveEndKeys   bool
}

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
	md restoreJobMetadata,
	progCh chan *execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
	tracingAggCh chan *execinfrapb.TracingAggregatorEvents,
) error {
	defer close(progCh)
	defer close(tracingAggCh)
	var noTxn *kv.Txn

	if md.encryption != nil && md.encryption.Mode == jobspb.EncryptionMode_KMS {
		kms, err := cloud.KMSFromURI(ctx, md.encryption.KMSInfo.Uri, md.kmsEnv)
		if err != nil {
			return errors.Wrap(err, "creating KMS")
		}
		defer func() {
			err := kms.Close()
			if err != nil {
				log.Infof(ctx, "failed to close KMS: %+v", err)
			}
		}()

		md.encryption.Key, err = kms.Decrypt(ctx, md.encryption.KMSInfo.EncryptedDataKey)
		if err != nil {
			return errors.Wrap(err,
				"failed to decrypt data key before starting BackupDataProcessor")
		}
	}
	// Wrap the relevant BackupEncryptionOptions to be used by the Restore
	// processor.
	var fileEncryption *kvpb.FileEncryptionOptions
	if md.encryption != nil {
		fileEncryption = &kvpb.FileEncryptionOptions{Key: md.encryption.Key}
	}

	makePlan := func(ctx context.Context, dsp *sql.DistSQLPlanner) (*sql.PhysicalPlan, *sql.PlanningCtx, error) {

		planCtx, sqlInstanceIDs, err := dsp.SetupAllNodesPlanningWithOracle(
			ctx, execCtx.ExtendedEvalContext(), execCtx.ExecCfg(),
			physicalplan.DefaultReplicaChooser, md.execLocality,
		)
		if err != nil {
			return nil, nil, err
		}

		numNodes := len(sqlInstanceIDs)
		p := planCtx.NewPhysicalPlan()

		restoreDataSpec := execinfrapb.RestoreDataSpec{
			JobID:        int64(md.jobID),
			RestoreTime:  md.restoreTime,
			Encryption:   fileEncryption,
			TableRekeys:  md.dataToRestore.getRekeys(),
			TenantRekeys: md.dataToRestore.getTenantRekeys(),
			PKIDs:        md.dataToRestore.getPKIDs(),
			ValidateOnly: md.dataToRestore.isValidateOnly(),
		}

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
					Encoding: catenumpb.DatumEncoding_ASCENDING_KEY,
				},
			},
		}
		for stream, sqlInstanceID := range sqlInstanceIDs {
			startBytes, endBytes, err := routingSpanForSQLInstance(sqlInstanceID)
			if err != nil {
				return nil, nil, err
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

		// TODO(pbardea): This not super principled. I just wanted something that
		// wasn't a constant and grew slower than linear with the length of
		// importSpans. It seems to be working well for BenchmarkRestore2TB but
		// worth revisiting.
		// It tries to take the cluster size into account so that larger clusters
		// distribute more chunks amongst them so that after scattering there isn't
		// a large varience in the distribution of entries.
		chunkSize := int(math.Sqrt(float64(md.numImportSpans))) / numNodes
		if chunkSize == 0 {
			chunkSize = 1
		}

		id := execCtx.ExecCfg().NodeInfo.NodeID.SQLInstanceID()

		spec := &execinfrapb.GenerativeSplitAndScatterSpec{
			TableRekeys:                 md.dataToRestore.getRekeys(),
			TenantRekeys:                md.dataToRestore.getTenantRekeys(),
			ValidateOnly:                md.dataToRestore.isValidateOnly(),
			URIs:                        md.uris,
			Encryption:                  md.encryption,
			EndTime:                     md.restoreTime,
			Spans:                       md.dataToRestore.getSpans(),
			BackupLocalityInfo:          md.backupLocalityInfo,
			HighWater:                   md.spanFilter.highWaterMark,
			UserProto:                   execCtx.User().EncodeProto(),
			TargetSize:                  md.spanFilter.targetSize,
			MaxFileCount:                int64(md.spanFilter.maxFileCount),
			ChunkSize:                   int64(chunkSize),
			NumEntries:                  int64(md.numImportSpans),
			NumNodes:                    int64(numNodes),
			UseFrontierCheckpointing:    md.spanFilter.useFrontierCheckpointing,
			JobID:                       int64(md.jobID),
			ExclusiveFileSpanComparison: md.exclusiveEndKeys,
		}
		if md.spanFilter.useFrontierCheckpointing {
			spec.CheckpointedSpans = persistFrontier(md.spanFilter.checkpointFrontier, 0)
		}

		proc := physicalplan.Processor{
			SQLInstanceID: id,
			Spec: execinfrapb.ProcessorSpec{
				Core: execinfrapb.ProcessorCoreUnion{GenerativeSplitAndScatter: spec},
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
		splitAndScatterProcs[id] = pIdx

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

		sql.FinalizePlan(ctx, planCtx, p)
		return p, planCtx, nil
	}

	dsp := execCtx.DistSQLPlanner()
	evalCtx := execCtx.ExtendedEvalContext()

	p, planCtx, err := makePlan(ctx, dsp)
	if err != nil {
		return errors.Wrap(err, "making distSQL plan")
	}

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		metaFn := func(_ context.Context, meta *execinfrapb.ProducerMetadata) error {
			if meta.BulkProcessorProgress != nil {
				// Send the progress up a level to be written to the manifest.
				progCh <- meta.BulkProcessorProgress
			}

			if meta.AggregatorEvents != nil {
				tracingAggCh <- meta.AggregatorEvents
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
		)
		defer recv.Release()

		execCfg := execCtx.ExecCfg()
		jobsprofiler.StorePlanDiagram(ctx, execCfg.DistSQLSrv.Stopper, p, execCfg.InternalDB, md.jobID)

		// Copy the evalCtx, as dsp.Run() might change it.
		evalCtxCopy := *evalCtx
		dsp.Run(ctx, planCtx, noTxn, p, recv, &evalCtxCopy, nil /* finishedSetupFn */)
		return errors.Wrap(rowResultWriter.Err(), "running distSQL flow")
	})

	return g.Wait()
}
