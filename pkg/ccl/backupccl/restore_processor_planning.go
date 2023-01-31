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
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
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

var replanRestoreThreshold = settings.RegisterFloatSetting(
	settings.TenantWritable,
	"bulkio.restore.replan_flow_threshold",
	"fraction of initial flow instances that would be added or updated above which a RESTORE execution plan is restarted from the last checkpoint (0=disabled)",
	0.0,
)

var replanRestoreFrequency = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"bulkio.restore.replan_flow_frequency",
	"frequency at which RESTORE checks to see if restarting would change its updates its physical execution plan",
	time.Minute*2,
	settings.PositiveDuration,
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
	pkIDs map[uint64]bool,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	tableRekeys []execinfrapb.TableRekey,
	tenantRekeys []execinfrapb.TenantRekey,
	restoreTime hlc.Timestamp,
	validateOnly bool,
	uris []string,
	requiredSpans []roachpb.Span,
	backupLocalityInfo []jobspb.RestoreDetails_BackupLocalityInfo,
	lowWaterMark roachpb.Key,
	targetSize int64,
	numNodes int,
	numImportSpans int,
	useSimpleImportSpans bool,
	progCh chan *execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
) error {
	defer close(progCh)
	var noTxn *kv.Txn

	if encryption != nil && encryption.Mode == jobspb.EncryptionMode_KMS {
		kms, err := cloud.KMSFromURI(ctx, encryption.KMSInfo.Uri, kmsEnv)
		if err != nil {
			return err
		}
		defer func() {
			err := kms.Close()
			if err != nil {
				log.Infof(ctx, "failed to close KMS: %+v", err)
			}
		}()

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

	makePlan := func(ctx context.Context, dsp *sql.DistSQLPlanner) (*sql.PhysicalPlan, *sql.PlanningCtx, error) {

		planCtx, sqlInstanceIDs, err := dsp.SetupAllNodesPlanning(ctx, execCtx.ExtendedEvalContext(), execCtx.ExecCfg())
		if err != nil {
			return nil, nil, err
		}

		p := planCtx.NewPhysicalPlan()

		restoreDataSpec := execinfrapb.RestoreDataSpec{
			JobID:        jobID,
			RestoreTime:  restoreTime,
			Encryption:   fileEncryption,
			TableRekeys:  tableRekeys,
			TenantRekeys: tenantRekeys,
			PKIDs:        pkIDs,
			ValidateOnly: validateOnly,
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
		chunkSize := int(math.Sqrt(float64(numImportSpans))) / numNodes
		if chunkSize == 0 {
			chunkSize = 1
		}

		id := execCtx.ExecCfg().NodeInfo.NodeID.SQLInstanceID()

		spec := &execinfrapb.GenerativeSplitAndScatterSpec{
			TableRekeys:          tableRekeys,
			TenantRekeys:         tenantRekeys,
			ValidateOnly:         validateOnly,
			URIs:                 uris,
			Encryption:           encryption,
			EndTime:              restoreTime,
			Spans:                requiredSpans,
			BackupLocalityInfo:   backupLocalityInfo,
			HighWater:            lowWaterMark,
			UserProto:            execCtx.User().EncodeProto(),
			TargetSize:           targetSize,
			ChunkSize:            int64(chunkSize),
			NumEntries:           int64(numImportSpans),
			NumNodes:             int64(numNodes),
			UseSimpleImportSpans: useSimpleImportSpans,
			JobID:                jobID,
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

		dsp.FinalizePlan(ctx, planCtx, p)
		return p, planCtx, nil
	}

	dsp := execCtx.DistSQLPlanner()
	evalCtx := execCtx.ExtendedEvalContext()

	p, planCtx, err := makePlan(ctx, dsp)
	if err != nil {
		return err
	}

	replanner, stopReplanner := sql.PhysicalPlanChangeChecker(ctx,
		p,
		makePlan,
		execCtx,
		sql.ReplanOnChangedFraction(func() float64 { return replanRestoreThreshold.Get(execCtx.ExecCfg().SV()) }),
		func() time.Duration { return replanRestoreFrequency.Get(execCtx.ExecCfg().SV()) },
	)

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		defer stopReplanner()

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
		)
		defer recv.Release()

		// Copy the evalCtx, as dsp.Run() might change it.
		evalCtxCopy := *evalCtx
		dsp.Run(ctx, planCtx, noTxn, p, recv, &evalCtxCopy, nil /* finishedSetupFn */)
		return rowResultWriter.Err()
	})

	g.GoCtx(replanner)

	return g.Wait()
}
