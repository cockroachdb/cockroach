// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/backup/backuppb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

func distBackupPlanSpecs(
	ctx context.Context,
	planCtx *sql.PlanningCtx,
	execCtx sql.JobExecContext,
	dsp *sql.DistSQLPlanner,
	jobID int64,
	spans roachpb.Spans,
	introducedSpans roachpb.Spans,
	pkIDs map[uint64]bool,
	defaultURI string,
	urisByLocalityKV map[string]string,
	encryption *jobspb.BackupEncryptionOptions,
	kmsEnv cloud.KMSEnv,
	mvccFilter kvpb.MVCCFilter,
	startTime, endTime hlc.Timestamp,
	elide execinfrapb.ElidePrefix,
	includeValueHeader bool,
) (map[base.SQLInstanceID]*execinfrapb.BackupDataSpec, error) {
	var span *tracing.Span
	ctx, span = tracing.ChildSpan(ctx, "backup.distBackupPlanSpecs")
	defer span.Finish()
	user := execCtx.User()

	var spanPartitions []sql.SpanPartition
	var introducedSpanPartitions []sql.SpanPartition
	var err error
	if len(spans) > 0 {
		spanPartitions, err = dsp.PartitionSpans(ctx, planCtx, spans, sql.PartitionSpansBoundDefault)
		if err != nil {
			return nil, err
		}
	}
	if len(introducedSpans) > 0 {
		introducedSpanPartitions, err = dsp.PartitionSpans(ctx, planCtx, introducedSpans, sql.PartitionSpansBoundDefault)
		if err != nil {
			return nil, err
		}
	}

	if encryption != nil && encryption.Mode == jobspb.EncryptionMode_KMS {
		kms, err := cloud.KMSFromURI(ctx, encryption.KMSInfo.Uri, kmsEnv)
		if err != nil {
			return nil, err
		}
		defer func() {
			err := kms.Close()
			if err != nil {
				log.Infof(ctx, "failed to close KMS: %+v", err)
			}
		}()

		encryption.Key, err = kms.Decrypt(ctx, encryption.KMSInfo.EncryptedDataKey)
		if err != nil {
			return nil, errors.Wrap(err,
				"failed to decrypt data key before starting BackupDataProcessor")
		}
	}
	// Wrap the relevant BackupEncryptionOptions to be used by the Backup
	// processor and KV ExportRequest.
	var fileEncryption *kvpb.FileEncryptionOptions
	if encryption != nil {
		fileEncryption = &kvpb.FileEncryptionOptions{Key: encryption.Key}
	}

	// First construct spans based on span partitions. Then add on
	// introducedSpans based on how those partition.
	sqlInstanceIDToSpec := make(map[base.SQLInstanceID]*execinfrapb.BackupDataSpec)
	for _, partition := range spanPartitions {
		spec := &execinfrapb.BackupDataSpec{
			JobID:                  jobID,
			Spans:                  partition.Spans,
			DefaultURI:             defaultURI,
			URIsByLocalityKV:       urisByLocalityKV,
			MVCCFilter:             mvccFilter,
			Encryption:             fileEncryption,
			PKIDs:                  pkIDs,
			BackupStartTime:        startTime,
			BackupEndTime:          endTime,
			UserProto:              user.EncodeProto(),
			ElidePrefix:            elide,
			IncludeMVCCValueHeader: includeValueHeader,
		}
		sqlInstanceIDToSpec[partition.SQLInstanceID] = spec
	}

	for _, partition := range introducedSpanPartitions {
		if spec, ok := sqlInstanceIDToSpec[partition.SQLInstanceID]; ok {
			spec.IntroducedSpans = partition.Spans
		} else {
			// We may need to introduce a new spec in the case that there is a node
			// which is not the leaseholder for any of the spans, but is for an
			// introduced span.
			spec := &execinfrapb.BackupDataSpec{
				JobID:                  jobID,
				IntroducedSpans:        partition.Spans,
				DefaultURI:             defaultURI,
				URIsByLocalityKV:       urisByLocalityKV,
				MVCCFilter:             mvccFilter,
				Encryption:             fileEncryption,
				PKIDs:                  pkIDs,
				BackupStartTime:        startTime,
				BackupEndTime:          endTime,
				UserProto:              user.EncodeProto(),
				IncludeMVCCValueHeader: includeValueHeader,
			}
			sqlInstanceIDToSpec[partition.SQLInstanceID] = spec
		}
	}

	backupPlanningTraceEvent := backuppb.BackupProcessorPlanningTraceEvent{
		NodeToNumSpans: make(map[int32]int64),
	}
	for node, spec := range sqlInstanceIDToSpec {
		numSpans := int64(len(spec.Spans) + len(spec.IntroducedSpans))
		backupPlanningTraceEvent.NodeToNumSpans[int32(node)] = numSpans
		backupPlanningTraceEvent.TotalNumSpans += numSpans
	}
	span.RecordStructured(&backupPlanningTraceEvent)

	return sqlInstanceIDToSpec, nil
}

// distBackup is used to plan the processors for a distributed backup. It
// streams back progress updates over progCh, which is used to incrementally
// build up the BulkOpSummary.
func distBackup(
	ctx context.Context,
	execCtx sql.JobExecContext,
	planCtx *sql.PlanningCtx,
	dsp *sql.DistSQLPlanner,
	progCh chan *execinfrapb.RemoteProducerMetadata_BulkProcessorProgress,
	tracingAggCh chan *execinfrapb.TracingAggregatorEvents,
	backupSpecs map[base.SQLInstanceID]*execinfrapb.BackupDataSpec,
) error {
	ctx, span := tracing.ChildSpan(ctx, "backup.distBackup")
	defer span.Finish()
	evalCtx := execCtx.ExtendedEvalContext()
	var noTxn *kv.Txn

	if len(backupSpecs) == 0 {
		close(progCh)
		close(tracingAggCh)
		return nil
	}

	// Setup a one-stage plan with one proc per input spec.
	corePlacement := make([]physicalplan.ProcessorCorePlacement, len(backupSpecs))
	i := 0
	var jobID jobspb.JobID
	for sqlInstanceID, spec := range backupSpecs {
		if i == 0 {
			jobID = jobspb.JobID(spec.JobID)
		}
		corePlacement[i].SQLInstanceID = sqlInstanceID
		corePlacement[i].Core.BackupData = spec
		i++
	}

	p := planCtx.NewPhysicalPlan()
	// All of the progress information is sent through the metadata stream, so we
	// have an empty result stream.
	p.AddNoInputStage(corePlacement, execinfrapb.PostProcessSpec{}, []*types.T{}, execinfrapb.Ordering{})
	p.PlanToStreamColMap = []int{}

	sql.FinalizePlan(ctx, planCtx, p)

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

	defer close(progCh)
	defer close(tracingAggCh)
	execCfg := execCtx.ExecCfg()
	jobsprofiler.StorePlanDiagram(ctx, execCfg.DistSQLSrv.Stopper, p, execCfg.InternalDB, jobID)

	// Copy the evalCtx, as dsp.Run() might change it.
	evalCtxCopy := *evalCtx
	dsp.Run(ctx, planCtx, noTxn, p, recv, &evalCtxCopy, nil /* finishedSetupFn */)
	return rowResultWriter.Err()
}
