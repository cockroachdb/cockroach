// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	gogotypes "github.com/gogo/protobuf/types"
)

// IndexBackfillPlanner holds dependencies for an index backfiller
// for use in the declarative schema changer.
type IndexBackfillPlanner struct {
	execCfg *ExecutorConfig
}

// NewIndexBackfiller creates a new IndexBackfillPlanner.
func NewIndexBackfiller(execCfg *ExecutorConfig) *IndexBackfillPlanner {
	return &IndexBackfillPlanner{execCfg: execCfg}
}

// MaybePrepareDestIndexesForBackfill is part of the scexec.Backfiller interface.
func (ib *IndexBackfillPlanner) MaybePrepareDestIndexesForBackfill(
	ctx context.Context, current scexec.BackfillProgress, td catalog.TableDescriptor,
) (scexec.BackfillProgress, error) {
	if !current.MinimumWriteTimestamp.IsEmpty() {
		return current, nil
	}
	minWriteTimestamp := ib.execCfg.Clock.Now()
	targetSpans := make([]roachpb.Span, len(current.DestIndexIDs))
	for i, idxID := range current.DestIndexIDs {
		targetSpans[i] = td.IndexSpan(ib.execCfg.Codec, idxID)
	}
	if err := scanTargetSpansToPushTimestampCache(
		ctx, ib.execCfg.DB, minWriteTimestamp, targetSpans,
	); err != nil {
		return scexec.BackfillProgress{}, err
	}
	return scexec.BackfillProgress{
		Backfill:              current.Backfill,
		MinimumWriteTimestamp: minWriteTimestamp,
	}, nil
}

// BackfillIndexes is part of the scexec.Backfiller interface.
func (ib *IndexBackfillPlanner) BackfillIndexes(
	ctx context.Context,
	progress scexec.BackfillProgress,
	tracker scexec.BackfillerProgressWriter,
	job *jobs.Job,
	descriptor catalog.TableDescriptor,
) (retErr error) {
	var completed = struct {
		syncutil.Mutex
		g roachpb.SpanGroup
	}{}
	addCompleted := func(c ...roachpb.Span) []roachpb.Span {
		completed.Lock()
		defer completed.Unlock()
		completed.g.Add(c...)
		return completed.g.Slice()
	}
	// Add spans that were already completed before the job resumed.
	addCompleted(progress.CompletedSpans...)
	sstManifestBuf := backfill.NewSSTManifestBuffer(progress.SSTManifests)
	progress.SSTManifests = sstManifestBuf.Snapshot()
	updateSSTManifests := func(newManifests []jobspb.IndexBackfillSSTManifest) {
		progress.SSTManifests = sstManifestBuf.Append(newManifests)
	}
	mode, err := getIndexBackfillDistributedMergeMode(job)
	if err != nil {
		return err
	}
	updateFunc := func(
		ctx context.Context, meta *execinfrapb.ProducerMetadata,
	) error {
		if meta.BulkProcessorProgress == nil {
			return nil
		}
		progress.CompletedSpans = addCompleted(meta.BulkProcessorProgress.CompletedSpans...)
		var mapProgress execinfrapb.IndexBackfillMapProgress
		if gogotypes.Is(&meta.BulkProcessorProgress.ProgressDetails, &mapProgress) {
			if err := gogotypes.UnmarshalAny(&meta.BulkProcessorProgress.ProgressDetails, &mapProgress); err != nil {
				return err
			}
			updateSSTManifests(mapProgress.SSTManifests)
		}
		// Make sure the progress update does not contain overlapping spans.
		// This is a sanity check that only runs in test configurations, since it
		// is an expensive n^2 check.
		if buildutil.CrdbTestBuild {
			for i, span1 := range progress.CompletedSpans {
				for j, span2 := range progress.CompletedSpans {
					if i <= j {
						continue
					}
					if span1.Overlaps(span2) {
						return errors.Newf("progress update contains overlapping spans: %s and %s", span1, span2)
					}
				}
			}
		}

		knobs := &ib.execCfg.DistSQLSrv.TestingKnobs
		if knobs.RunBeforeIndexBackfillProgressUpdate != nil {
			knobs.RunBeforeIndexBackfillProgressUpdate(ctx, meta.BulkProcessorProgress.CompletedSpans)
		}
		return tracker.SetBackfillProgress(ctx, progress)
	}
	var spansToDo []roachpb.Span
	{
		sourceIndexSpan := descriptor.IndexSpan(ib.execCfg.Codec, progress.SourceIndexID)
		var g roachpb.SpanGroup
		g.Add(sourceIndexSpan)
		g.Sub(progress.CompletedSpans...)
		spansToDo = g.Slice()
	}
	if len(spansToDo) == 0 { // already done
		return nil
	}

	now := ib.execCfg.DB.Clock().Now()
	// Pick now as the read timestamp for the backfill. It's safe to use this
	// timestamp to read even if we've partially backfilled at an earlier
	// timestamp because other writing transactions have been writing at the
	// appropriate timestamps in-between.
	readAsOf := now
	useDistributedMerge := mode == jobspb.IndexBackfillDistributedMergeMode_Enabled
	run, retErr := ib.plan(
		ctx,
		job.ID(),
		descriptor,
		now,
		progress.MinimumWriteTimestamp,
		readAsOf,
		spansToDo,
		progress.DestIndexIDs,
		progress.SourceIndexID,
		useDistributedMerge,
		updateFunc,
	)
	if retErr != nil {
		return retErr
	}
	if err := run(ctx); err != nil {
		return err
	}
	if !useDistributedMerge {
		return nil
	}
	merged, err := ib.runDistributedMerge(ctx, job, descriptor, progress, sstManifestBuf.Snapshot())
	if err != nil {
		return err
	}
	progress.SSTManifests = merged
	if err := tracker.SetBackfillProgress(ctx, progress); err != nil {
		return err
	}
	return ib.runDistributedIngest(ctx, job, descriptor, progress, merged)
}

// Index backfilling ingests SSTs that don't play nicely with running txns
// since they just add their keys blindly. Running a Scan of the target
// spans at the time the SSTs' keys will be written will calcify history up
// to then since the scan will resolve intents and populate tscache to keep
// anything else from sneaking under us. Since these are new indexes, these
// spans should be essentially empty, so this should be a pretty quick and
// cheap scan.
func scanTargetSpansToPushTimestampCache(
	ctx context.Context, db *kv.DB, backfillTimestamp hlc.Timestamp, targetSpans []roachpb.Span,
) error {
	const pageSize = 10000
	return db.TxnWithAdmissionControl(
		ctx, kvpb.AdmissionHeader_FROM_SQL, admissionpb.BulkNormalPri,
		kv.SteppingDisabled,
		func(
			ctx context.Context, txn *kv.Txn,
		) error {
			if err := txn.SetFixedTimestamp(ctx, backfillTimestamp); err != nil {
				return err
			}
			for _, span := range targetSpans {
				// TODO(dt): a Count() request would be nice here if the target isn't
				// empty, since we don't need to drag all the results back just to
				// then ignore them -- we just need the iteration on the far end.
				if err := txn.Iterate(ctx, span.Key, span.EndKey, pageSize, iterateNoop); err != nil {
					return err
				}
			}
			return nil
		})
}

func iterateNoop(_ []kv.KeyValue) error { return nil }

var _ scexec.Backfiller = (*IndexBackfillPlanner)(nil)

func (ib *IndexBackfillPlanner) plan(
	ctx context.Context,
	jobID jobspb.JobID,
	tableDesc catalog.TableDescriptor,
	nowTimestamp, writeAsOf, readAsOf hlc.Timestamp,
	sourceSpans []roachpb.Span,
	indexesToBackfill []descpb.IndexID,
	sourceIndexID descpb.IndexID,
	useDistributedMerge bool,
	callback func(_ context.Context, meta *execinfrapb.ProducerMetadata) error,
) (runFunc func(context.Context) error, _ error) {

	var p *PhysicalPlan
	var extEvalCtx extendedEvalContext
	var planCtx *PlanningCtx
	td := tabledesc.NewBuilder(tableDesc.TableDesc()).BuildExistingMutableTable()
	if err := DescsTxn(ctx, ib.execCfg, func(
		ctx context.Context, txn isql.Txn, descriptors *descs.Collection,
	) error {
		sd := NewInternalSessionData(ctx, ib.execCfg.Settings, "plan-index-backfill")
		extEvalCtx = createSchemaChangeEvalCtx(ctx, ib.execCfg, sd, nowTimestamp, descriptors)
		planCtx = ib.execCfg.DistSQLPlanner.NewPlanningCtx(
			ctx, &extEvalCtx, nil /* planner */, txn.KV(), FullDistribution,
		)
		// TODO(ajwerner): Adopt metamorphic.ConstantWithTestRange for the
		// batch size. Also plumb in a testing knob.
		chunkSize := indexBackfillBatchSize.Get(&ib.execCfg.Settings.SV)
		const writeAtRequestTimestamp = true
		spec := initIndexBackfillerSpec(
			*td.TableDesc(), writeAsOf, writeAtRequestTimestamp, chunkSize,
			indexesToBackfill, sourceIndexID,
		)
		if useDistributedMerge {
			backfill.EnableDistributedMergeIndexBackfillSink(ib.execCfg.NodeInfo.NodeID.SQLInstanceID(), jobID, &spec)
		}
		var err error
		p, err = ib.execCfg.DistSQLPlanner.createBackfillerPhysicalPlan(ctx, planCtx, spec, sourceSpans)
		return err
	}); err != nil {
		return nil, err
	}

	return func(ctx context.Context) error {
		cbw := MetadataCallbackWriter{rowResultWriter: &errOnlyResultWriter{}, fn: callback}
		recv := MakeDistSQLReceiver(
			ctx,
			&cbw,
			tree.Rows, /* stmtType - doesn't matter here since no result are produced */
			ib.execCfg.RangeDescriptorCache,
			nil, /* txn - the flow does not run wholly in a txn */
			ib.execCfg.Clock,
			extEvalCtx.Tracing,
		)
		defer recv.Release()
		// Copy the eval.Context, as dsp.Run() might change it.
		evalCtxCopy := extEvalCtx.Context.Copy()
		ib.execCfg.DistSQLPlanner.Run(ctx, planCtx, nil, p, recv, evalCtxCopy, nil)
		return cbw.Err()
	}, nil
}

func getIndexBackfillDistributedMergeMode(
	job *jobs.Job,
) (jobspb.IndexBackfillDistributedMergeMode, error) {
	payload := job.Payload()
	details := payload.GetNewSchemaChange()
	if details == nil {
		return jobspb.IndexBackfillDistributedMergeMode_Disabled,
			errors.AssertionFailedf("expected new schema change details on job %d", job.ID())
	}
	return details.DistributedMergeMode, nil
}

// runDistributedMerge runs a distributed merge of the provided SSTs into larger
// SSTs. This is part of the distributed merge pipeline and is only run if the
// index backfill has enabled distributed merging.
func (ib *IndexBackfillPlanner) runDistributedMerge(
	ctx context.Context,
	job *jobs.Job,
	descriptor catalog.TableDescriptor,
	progress scexec.BackfillProgress,
	manifests []jobspb.IndexBackfillSSTManifest,
) ([]jobspb.IndexBackfillSSTManifest, error) {
	if len(manifests) == 0 {
		return nil, nil
	}
	ssts := make([]execinfrapb.BulkMergeSpec_SST, 0, len(manifests))
	for _, manifest := range manifests {
		if manifest.Span == nil {
			return nil, errors.AssertionFailedf("manifest missing span metadata")
		}
		ssts = append(ssts, execinfrapb.BulkMergeSpec_SST{
			URI:      manifest.URI,
			StartKey: append([]byte(nil), manifest.Span.Key...),
			EndKey:   append([]byte(nil), manifest.Span.EndKey...),
		})
	}
	targetSpans := make([]roachpb.Span, 0, len(progress.DestIndexIDs))
	for _, idxID := range progress.DestIndexIDs {
		span := descriptor.IndexSpan(ib.execCfg.Codec, idxID)
		targetSpans = append(targetSpans, span)
	}
	if len(targetSpans) == 0 {
		return nil, errors.AssertionFailedf("no destination index spans provided for merge")
	}

	mem := &MemoryMetrics{}
	jobExecCtx, cleanup := MakeJobExecContext(ctx, "index-backfill-distributed-merge", username.NodeUserName(), mem, ib.execCfg)
	defer cleanup()

	outputURI := func(instanceID base.SQLInstanceID) string {
		// Use nodelocal for temporary storage of merged SSTs. These SSTs are
		// only needed during the lifetime of the job. The '/job/<jobID>' prefix
		// allows for easy cleanup in the event of job cancellation or failure.
		// TODO(158873): handle cleanup of nodelocal SSTs
		//
		// The 'iter-0' suffix is to allow for future iterations of
		// merging in case we want to do multiple stages of merging.
		return fmt.Sprintf("nodelocal://%d/job/%d/merge/iter-0/", instanceID, job.ID())
	}

	writeTS := progress.MinimumWriteTimestamp

	// TODO(159374): use single-pass merge by setting iteration < maxIterations
	merged, err := invokeBulkMerge(ctx, jobExecCtx, ssts, targetSpans, outputURI,
		1 /* iteration */, 2 /* maxIterations */, &writeTS)
	if err != nil {
		return nil, err
	}

	out := make([]jobspb.IndexBackfillSSTManifest, 0, len(merged))
	for _, sst := range merged {
		span := roachpb.Span{
			Key:    append([]byte(nil), sst.StartKey...),
			EndKey: append([]byte(nil), sst.EndKey...),
		}
		ts := writeTS
		out = append(out, jobspb.IndexBackfillSSTManifest{
			URI:            sst.URI,
			Span:           &span,
			WriteTimestamp: &ts,
		})
	}
	return out, nil
}

// runDistributedIngest runs a final ingest of the SSTs produced by
// the runDistributedMerge call. This is only used when the distributed
// merge pipeline is enabled for index backfills.
// TODO(159374): we can remove this stage of the pipeline if the merge processor
// can write directly into the KV in it's final iteration.
func (ib *IndexBackfillPlanner) runDistributedIngest(
	ctx context.Context,
	job *jobs.Job,
	descriptor catalog.TableDescriptor,
	progress scexec.BackfillProgress,
	outputs []jobspb.IndexBackfillSSTManifest,
) error {
	if len(outputs) == 0 {
		return nil
	}
	spans := make([]roachpb.Span, len(progress.DestIndexIDs))
	for i, idxID := range progress.DestIndexIDs {
		spans[i] = descriptor.IndexSpan(ib.execCfg.Codec, idxID)
	}
	ssts := make([]execinfrapb.BulkMergeSpec_SST, len(outputs))
	for i, manifest := range outputs {
		if manifest.Span == nil {
			return errors.AssertionFailedf("manifest missing span metadata")
		}
		ssts[i] = execinfrapb.BulkMergeSpec_SST{
			URI:      manifest.URI,
			StartKey: append([]byte(nil), manifest.Span.Key...),
			EndKey:   append([]byte(nil), manifest.Span.EndKey...),
		}
	}

	mem := &MemoryMetrics{}
	jobExecCtx, cleanup := MakeJobExecContext(ctx, "index-backfill-ingest", username.NodeUserName(), mem, ib.execCfg)
	defer cleanup()

	return invokeBulkIngest(ctx, jobExecCtx, spans, ssts)
}
