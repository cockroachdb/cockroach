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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/bulksst"
	"github.com/cockroachdb/cockroach/pkg/sql/bulkutil"
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
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	useDistributedMerge, err := shouldUseDistributedMerge(ctx, mode, descriptor, progress)
	if err != nil {
		return err
	}

	// addStoragePrefix records storage prefixes before any SST is written to those
	// locations, ensuring cleanup can occur even if the job fails mid-backfill or
	// mid-merge. Duplicates are skipped by checking the existing prefixes.
	addStoragePrefix := func(ctx context.Context, prefixes []string) error {
		existingSet := make(map[string]struct{}, len(progress.SSTStoragePrefixes))
		for _, existing := range progress.SSTStoragePrefixes {
			existingSet[existing] = struct{}{}
		}

		var newPrefixes []string
		for _, prefix := range prefixes {
			if _, exists := existingSet[prefix]; !exists {
				newPrefixes = append(newPrefixes, prefix)
				existingSet[prefix] = struct{}{}
			}
		}

		if len(newPrefixes) == 0 {
			return nil
		}
		progress.SSTStoragePrefixes = append(progress.SSTStoragePrefixes, newPrefixes...)
		return tracker.SetBackfillProgress(ctx, progress)
	}

	// Compute remaining spans for map phase.
	var spansToDo []roachpb.Span
	{
		sourceIndexSpan := descriptor.IndexSpan(ib.execCfg.Codec, progress.SourceIndexID)
		var g roachpb.SpanGroup
		g.Add(sourceIndexSpan)
		g.Sub(progress.CompletedSpans...)
		spansToDo = g.Slice()
	}

	// Process any remaining spans to do. If this is the distributed merge
	// pipeline, this is the map phase.
	if len(spansToDo) > 0 {
		now := ib.execCfg.DB.Clock().Now()
		// Pick now as the read timestamp for the backfill. It's safe to use this
		// timestamp to read even if we've partially backfilled at an earlier
		// timestamp because other writing transactions have been writing at the
		// appropriate timestamps in-between.
		readAsOf := now
		run, err := ib.plan(
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
			addStoragePrefix,
		)
		if err != nil {
			return err
		}
		if err := run(ctx); err != nil {
			return err
		}
	}

	if !useDistributedMerge {
		return nil
	}

	// Check if there are manifests to merge. On resume, sstManifestBuf is
	// initialized from progress.SSTManifests (see NewSSTManifestBuffer call
	// above), so this correctly returns checkpointed manifests even when the
	// map phase was already complete in a previous run.
	manifests := sstManifestBuf.Snapshot()
	if len(manifests) == 0 {
		return nil
	}

	// Call testing hook after map phase completes, before merge iterations begin.
	if knobs := &ib.execCfg.DistSQLSrv.TestingKnobs; knobs.AfterDistributedMergeMapPhase != nil {
		knobs.AfterDistributedMergeMapPhase(ctx, manifests)
	}

	// Determine merge iteration parameters.
	//
	// Phase semantics:
	//   - phase = 0: Map phase complete, no merge iterations completed yet.
	//   - phase = N (N >= 1): Merge iteration N completed.
	//
	// We always run 2 merge iterations: a local merge (iteration 1) followed by
	// a final cross-node merge (iteration 2). The local merge reduces network
	// traffic by having each node merge its local SSTs first.
	//
	// WARNING: The phased progress tracking in calculatePhasedProgress assumes
	// exactly 2 iterations. Changing this value requires updating the progress
	// calculation in that function.
	const maxIterations = 2
	startIteration := int(progress.DistributedMergePhase) + 1

	return ib.runDistributedMerge(
		ctx, job, descriptor, &progress, tracker, manifests, startIteration, maxIterations, addStoragePrefix,
	)
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
	addStoragePrefix func(ctx context.Context, prefixes []string) error,
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
			backfill.EnableDistributedMergeIndexBackfillSink(jobID, &spec)
		}
		var err error
		p, err = ib.execCfg.DistSQLPlanner.createBackfillerPhysicalPlan(ctx, planCtx, spec, sourceSpans)
		return err
	}); err != nil {
		return nil, err
	}

	// Pre-register storage prefixes for all nodes that will run processors.
	// This must happen before any SSTs are written to ensure cleanup can find
	// orphaned files if the job fails mid-write.
	if useDistributedMerge {
		prefixes := make([]string, 0, len(p.Processors))
		for i := range p.Processors {
			prefix := fmt.Sprintf("nodelocal://%d/", p.Processors[i].SQLInstanceID)
			prefixes = append(prefixes, prefix)
		}
		if err := addStoragePrefix(ctx, prefixes); err != nil {
			return nil, err
		}
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

// shouldUseDistributedMerge decides whether the backfill should run through
// the distributed merge pipeline. Force mode always enables it. Enabled mode
// falls back to the regular path when every destination index shares leading
// key columns with the source, since the SSTs are already non-overlapping.
func shouldUseDistributedMerge(
	ctx context.Context,
	mode jobspb.IndexBackfillDistributedMergeMode,
	descriptor catalog.TableDescriptor,
	progress scexec.BackfillProgress,
) (bool, error) {
	if mode == jobspb.IndexBackfillDistributedMergeMode_Force {
		return true, nil
	}
	if mode != jobspb.IndexBackfillDistributedMergeMode_Enabled {
		return false, nil
	}
	// Mode is Enabled: check whether the backfill data is already sorted
	// relative to the PK. If every destination index shares leading key
	// columns with the source index, the SSTs from different PK ranges are
	// non-overlapping and distributed merge adds only overhead.
	sourceIndex, err := catalog.MustFindIndexByID(descriptor, progress.SourceIndexID)
	if err != nil {
		return false, err
	}
	for _, destIndexID := range progress.DestIndexIDs {
		destIndex, err := catalog.MustFindIndexByID(descriptor, destIndexID)
		if err != nil {
			return false, err
		}
		if !backfill.IsBackfillDataSorted(sourceIndex, destIndex) {
			return true, nil
		}
	}
	log.Dev.Infof(ctx, "index backfill data is sorted relative to source index %d; "+
		"falling back to non-distributed merge", progress.SourceIndexID)
	return false, nil
}

// runDistributedMerge runs a multi-pass distributed merge of the provided SSTs.
// Intermediate iterations emit merged SSTs to nodelocal storage and update the
// progress with the new manifests; the final iteration ingests the files directly
// into KV and clears the manifests.
//
// The addStoragePrefix callback is invoked before any SST is written to new
// storage locations, allowing the caller to persist the prefixes for cleanup.
func (ib *IndexBackfillPlanner) runDistributedMerge(
	ctx context.Context,
	job *jobs.Job,
	descriptor catalog.TableDescriptor,
	progress *scexec.BackfillProgress,
	tracker scexec.BackfillerProgressWriter,
	manifests []jobspb.IndexBackfillSSTManifest,
	startIteration int,
	maxIterations int,
	addStoragePrefix func(ctx context.Context, prefixes []string) error,
) error {
	if len(manifests) == 0 {
		return nil
	}

	// Convert manifests to SSTFiles format for CombineFileInfo. We create a
	// single SSTFiles entry containing all manifest data.
	sstFileInfos := make([]*bulksst.SSTFileInfo, 0, len(manifests))
	rowSamples := make([]string, 0, len(manifests))
	for _, manifest := range manifests {
		if manifest.Span == nil {
			return errors.AssertionFailedf("manifest missing span metadata")
		}
		sstFileInfos = append(sstFileInfos, &bulksst.SSTFileInfo{
			URI:       manifest.URI,
			StartKey:  append(roachpb.Key(nil), manifest.Span.Key...),
			EndKey:    append(roachpb.Key(nil), manifest.Span.EndKey...),
			FileSize:  manifest.FileSize,
			RowSample: append(roachpb.Key(nil), manifest.RowSample...),
			KeyCount:  manifest.KeyCount,
		})
		if len(manifest.RowSample) > 0 {
			rowSamples = append(rowSamples, string(manifest.RowSample))
		}
	}
	sstFiles := []bulksst.SSTFiles{{SST: sstFileInfos, RowSamples: rowSamples}}

	// Build schema spans from destination indexes.
	schemaSpans := make([]roachpb.Span, 0, len(progress.DestIndexIDs))
	for _, idxID := range progress.DestIndexIDs {
		span := descriptor.IndexSpan(ib.execCfg.Codec, idxID)
		schemaSpans = append(schemaSpans, span)
	}
	if len(schemaSpans) == 0 {
		return errors.AssertionFailedf("no destination index spans provided for merge")
	}

	// CombineFileInfo consolidates SST metadata and uses row samples to split
	// schema spans into smaller merge spans for better parallelization.
	ssts, mergeSpans, err := bulksst.CombineFileInfo(sstFiles, schemaSpans)
	if err != nil {
		return err
	}

	mem := &MemoryMetrics{}
	jobExecCtx, cleanup := MakeJobExecContext(ctx, "index-backfill-distributed-merge", username.NodeUserName(), mem, ib.execCfg)
	defer cleanup()

	// Iterate through merge passes, with all but the final iteration writing to
	// intermediate storage, and the final iteration ingesting directly into KV.
	currentSSTs := ssts
	for iteration := startIteration; iteration <= maxIterations; iteration++ {
		// Check if the job has been paused or cancelled before starting work.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Reset task tracking for this iteration and set progress so that we
		// persist it.
		progress.MergeIterationCompletedTasks = nil
		if err := tracker.SetBackfillProgress(ctx, *progress); err != nil {
			return err
		}

		genOutputURIAndRecordPrefix := func(instanceID base.SQLInstanceID) (string, error) {
			// Use nodelocal for temporary storage of merged SSTs. These SSTs are
			// only needed during the lifetime of the job. Record the storage prefix
			// for cleanup on job completion - this must happen before writing any SST.
			prefix := fmt.Sprintf("nodelocal://%d/", instanceID)
			if err := addStoragePrefix(ctx, []string{prefix}); err != nil {
				return "", err
			}
			// The '/job/<jobID>' prefix allows for easy cleanup in the event of job
			// cancellation or failure.
			return prefix + bulkutil.NewDistMergePaths(job.ID()).MergePath(iteration), nil
		}

		var writeTS *hlc.Timestamp
		if iteration == maxIterations {
			// Final iteration: ingest directly into KV.
			ts := progress.MinimumWriteTimestamp
			writeTS = &ts
		}

		// Determine if any destination indexes are unique. If so, we need to
		// enforce uniqueness checking during the final merge iteration.
		enforceUniqueness := false
		for _, idxID := range progress.DestIndexIDs {
			idx, err := catalog.MustFindIndexByID(descriptor, idxID)
			if err != nil {
				return err
			}
			if idx.IsUnique() {
				enforceUniqueness = true
				break
			}
		}

		// Create a progress callback to track task completion during this iteration.
		onProgress := func(ctx context.Context, meta *execinfrapb.ProducerMetadata) error {
			if meta.BulkProcessorProgress == nil {
				return nil
			}
			var mergeProgress execinfrapb.MergeIterationProgress
			if gogotypes.Is(&meta.BulkProcessorProgress.ProgressDetails, &mergeProgress) {
				if err := gogotypes.UnmarshalAny(
					&meta.BulkProcessorProgress.ProgressDetails, &mergeProgress,
				); err != nil {
					return err
				}
				progress.MergeIterationTasksTotal = mergeProgress.TasksTotal
				progress.MergeIterationCompletedTasks = append(
					progress.MergeIterationCompletedTasks, mergeProgress.CompletedTaskID)
				return tracker.SetBackfillProgress(ctx, *progress)
			}
			return nil
		}

		ib.cleanupRedoIterationSSTs(ctx, job.ID(), iteration, maxIterations, progress.SSTStoragePrefixes)

		merged, err := invokeBulkMerge(
			ctx,
			jobExecCtx,
			currentSSTs,
			mergeSpans,
			genOutputURIAndRecordPrefix,
			iteration,
			maxIterations,
			writeTS,
			enforceUniqueness,
			onProgress,
			execinfrapb.BulkMergeSpec_BACKFILL_MONITOR,
		)
		if err != nil {
			return err
		}

		// Final iteration: data is ingested into KV, we're done.
		if iteration == maxIterations {
			// Clear manifests and merge iteration tracking to indicate completion.
			progress.SSTManifests = nil
			progress.DistributedMergePhase = int32(iteration)
			progress.MergeIterationTasksTotal = 0
			progress.MergeIterationCompletedTasks = nil
			if err := tracker.SetBackfillProgress(ctx, *progress); err != nil {
				return err
			}

			// Call testing knob for final iteration (manifests will be nil/empty).
			if fn := ib.execCfg.DistSQLSrv.TestingKnobs.AfterDistributedMergeIteration; fn != nil {
				fn(ctx, iteration, progress.SSTManifests)
			}
			return nil
		}

		if len(merged) == 0 {
			return errors.AssertionFailedf("expected merged sst output: iteration %d", iteration)
		}

		// Convert merged SSTs to manifests for checkpointing. On resume, these
		// manifests will be used as input to complete the merge in a single
		// iteration directly to KV.
		newManifests := make([]jobspb.IndexBackfillSSTManifest, 0, len(merged))
		for _, sst := range merged {
			span := roachpb.Span{
				Key:    append([]byte(nil), sst.StartKey...),
				EndKey: append([]byte(nil), sst.EndKey...),
			}
			newManifests = append(newManifests, jobspb.IndexBackfillSSTManifest{
				URI:      sst.URI,
				Span:     &span,
				KeyCount: sst.KeyCount,
				// Populate RowSample so CombineFileInfo can split schema spans on
				// resume. StartKey is already a row prefix (family suffix stripped
				// by SSTWriter.Flush), but CombineFileInfo passes samples through
				// keys.EnsureSafeSplitKey which expects a full key including the
				// column family suffix. Re-appending family 0 makes the key valid
				// for that function.
				RowSample: keys.MakeFamilyKey(append(roachpb.Key(nil), sst.StartKey...), 0),
			})
		}
		progress.SSTManifests = newManifests
		// Update the merge phase to track which iteration we completed.
		// This allows resume logic to know we're in the middle of merge iterations.
		// Clear task tracking fields so progress calculation doesn't incorrectly
		// use stale task counts from the just-completed iteration.
		progress.DistributedMergePhase = int32(iteration)
		progress.MergeIterationTasksTotal = 0
		progress.MergeIterationCompletedTasks = nil
		if err := tracker.SetBackfillProgress(ctx, *progress); err != nil {
			return err
		}

		// Call testing knob after updating progress for this iteration.
		if fn := ib.execCfg.DistSQLSrv.TestingKnobs.AfterDistributedMergeIteration; fn != nil {
			fn(ctx, iteration, progress.SSTManifests)
		}

		// Use the output of this iteration as input to the next.
		currentSSTs = merged
	}

	return nil
}

// cleanupRedoIterationSSTs removes leftover output SSTs from a previous attempt
// of a non-final merge iteration. When an iteration is interrupted (e.g., job
// paused/crashed) and restarted from scratch, the new attempt writes to the
// same output directory. This cleanup prevents orphaned files from the previous
// attempt from wasting storage.
//
// Cleanup is best-effort: errors are logged as warnings rather than failing the
// job, consistent with the existing cleanup pattern in backfiller/cleanup.go.
func (ib *IndexBackfillPlanner) cleanupRedoIterationSSTs(
	ctx context.Context,
	jobID jobspb.JobID,
	iteration int,
	maxIterations int,
	storagePrefixes []string,
) {
	if iteration >= maxIterations || len(storagePrefixes) == 0 {
		return
	}
	outputSubdir := bulkutil.NewDistMergePaths(jobID).MergeSubdir(iteration)
	cleaner := bulkutil.NewBulkJobCleaner(
		ib.execCfg.DistSQLSrv.ExternalStorageFromURI, username.NodeUserName(),
	)
	defer func() {
		if err := cleaner.Close(); err != nil {
			log.Dev.Warningf(ctx, "error closing cleaner after SST cleanup: %v", err)
		}
	}()
	if err := cleaner.CleanupJobSubdirectory(
		ctx, jobID, storagePrefixes, outputSubdir,
	); err != nil {
		log.Dev.Warningf(ctx, "failed to clean up SSTs in %s before redo: %v", outputSubdir, err)
	}
}
