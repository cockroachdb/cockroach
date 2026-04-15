// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/bulkmerge"
	"github.com/cockroachdb/cockroach/pkg/sql/bulksst"
	"github.com/cockroachdb/cockroach/pkg/sql/bulkutil"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	gogotypes "github.com/gogo/protobuf/types"
)

var replanThreshold = settings.RegisterFloatSetting(
	settings.ApplicationLevel,
	"bulkio.import.replan_flow_threshold",
	"fraction of initial flow instances that would be added or updated above which an IMPORT is restarted from its last checkpoint (0=disabled)",
	0.0,
)

var replanFrequency = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"bulkio.import.replan_flow_frequency",
	"frequency at which IMPORT checks to see if restarting would update its physical execution plan",
	time.Minute*2,
	settings.PositiveDuration,
)

// importProgressDebugName is used to mark the transaction for updating
// import job progress.
const importProgressDebugName = `import_progress`

// importSSTManifestsInfoKey is the job info key used to store SST manifests
// produced during the map phase of distributed merge import.
const importSSTManifestsInfoKey = "~import/sst-manifests.binpb"

// distImport is used by IMPORT to run a DistSQL flow to ingest data by starting
// reader processes on many nodes that each read and ingest their assigned files
// and then send back a summary of what they ingested. The combined summary is
// returned.
func distImport(
	ctx context.Context,
	execCtx sql.JobExecContext,
	job *jobs.Job,
	table *execinfrapb.ReadImportDataSpec_ImportTable,
	typeDescs []*descpb.TypeDescriptor,
	from []string,
	format roachpb.IOFileFormat,
	walltime int64,
	testingKnobs importTestingKnobs,
	procsPerNode int,
	initialSplitsPerProc int,
) (kvpb.BulkOpSummary, error) {
	ctx, sp := tracing.ChildSpan(ctx, "importer.distImport")
	defer sp.Finish()

	// When using distributed merge the processor will emit the SST's and their
	// start and end keys.
	details := job.Details().(jobspb.ImportDetails)
	useDistributedMerge := details.UseDistributedMerge

	// addStoragePrefix records a storage prefix before any SST is written to that
	// location, ensuring cleanup can occur even if the job fails mid-import.
	addStoragePrefix := func(ctx context.Context, prefix string) error {
		return job.NoTxn().Update(ctx, func(
			txn isql.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
		) error {
			prog := md.Progress.GetImport()
			if prog == nil {
				return errors.New("import progress not found")
			}
			// Check for duplicates
			for _, existing := range prog.SSTStoragePrefixes {
				if existing == prefix {
					return nil // Already recorded
				}
			}
			prog.SSTStoragePrefixes = append(prog.SSTStoragePrefixes, prefix)
			ju.UpdateProgress(md.Progress)
			return nil
		})
	}

	dsp := execCtx.DistSQLPlanner()
	makePlan := func(ctx context.Context, dsp *sql.DistSQLPlanner) (*sql.PhysicalPlan, *sql.PlanningCtx, error) {
		evalCtx := execCtx.ExtendedEvalContext()

		planCtx, sqlInstanceIDs, err := dsp.SetupAllNodesPlanning(ctx, evalCtx, execCtx.ExecCfg())
		if err != nil {
			return nil, nil, err
		}

		r := rand.New(rand.NewSource(int64(job.ID())))
		// Shuffle node order so that multiple IMPORTs done in parallel will not
		// identically schedule CSV reading. For example, if there are 3 nodes and 4
		// files, the first node will get 2 files while the other nodes will each
		// get 1 file. Shuffling will make that first node random instead of always
		// the same. Doing so seeded with the job's ID makes this deterministic
		// across re-plannings of this job.
		r.Shuffle(len(sqlInstanceIDs), func(i, j int) {
			sqlInstanceIDs[i], sqlInstanceIDs[j] = sqlInstanceIDs[j], sqlInstanceIDs[i]
		})

		inputSpecs := makeImportReaderSpecs(
			job, table, typeDescs, from, format, len(sqlInstanceIDs), /* numSQLInstances */
			walltime, execCtx.User(), procsPerNode, initialSplitsPerProc,
		)

		p := planCtx.NewPhysicalPlan()

		// Setup a one-stage plan with one proc per input spec.
		corePlacement := make([]physicalplan.ProcessorCorePlacement, len(inputSpecs))
		for i := range inputSpecs {
			corePlacement[i].SQLInstanceID = sqlInstanceIDs[i%len(sqlInstanceIDs)]
			corePlacement[i].Core.ReadImport = inputSpecs[i]
		}
		p.AddNoInputStage(
			corePlacement,
			execinfrapb.PostProcessSpec{},
			// The direct-ingest readers will emit a binary encoded BulkOpSummary.
			importProcessorOutputTypes,
			execinfrapb.Ordering{},
			nil, /* finalizeLastStageCb */
		)
		// Map the output directly back.
		colMap := make([]int, len(importProcessorOutputTypes))
		for i := range colMap {
			colMap[i] = i
		}
		p.PlanToStreamColMap = colMap
		sql.FinalizePlan(ctx, planCtx, p)
		return p, planCtx, nil
	}

	p, planCtx, err := makePlan(ctx, dsp)
	if err != nil {
		return kvpb.BulkOpSummary{}, err
	}

	importDetails := job.Progress().Details.(*jobspb.Progress_Import).Import
	if importDetails.ReadProgress == nil {
		// Initialize the progress metrics on the first attempt.
		if err := job.NoTxn().FractionProgressed(ctx, func(
			ctx context.Context, details jobspb.ProgressDetails,
		) float32 {
			prog := details.(*jobspb.Progress_Import).Import
			prog.ReadProgress = make([]float32, len(from))
			prog.ResumePos = make([]int64, len(from))
			if prog.SequenceDetails == nil {
				prog.SequenceDetails = make([]*jobspb.SequenceDetails, len(from))
				for i := range prog.SequenceDetails {
					prog.SequenceDetails[i] = &jobspb.SequenceDetails{}
				}
			}

			return 0.0
		},
		); err != nil {
			return kvpb.BulkOpSummary{}, err
		}
	}

	lastSummary := getLastImportSummary(job)
	checkpoint := newImportCheckpointTracker(
		len(from), lastSummary, nil, /* manifestBuf */
	)

	var res kvpb.BulkOpSummary

	// Check if we're resuming from a completed merge iteration. If so, skip
	// the map phase entirely — runImportMergeIterations reads its own
	// checkpointed manifests and determines the start iteration from the job
	// progress.
	importProgress := job.Progress().Details.(*jobspb.Progress_Import).Import
	mergePhase := 0
	if importProgress != nil {
		mergePhase = int(importProgress.DistributedMergePhase)
	}
	if useDistributedMerge && mergePhase >= 1 {
		execCfg := execCtx.ExecCfg()
		schemaSpans := tabledesc.NewBuilder(table.Desc).BuildImmutableTable().AllIndexSpans(execCfg.Codec)
		if err := runImportMergeIterations(
			ctx, execCtx, job, checkpoint, nil, schemaSpans, walltime, addStoragePrefix,
		); err != nil {
			return kvpb.BulkOpSummary{}, err
		}
		// Return an empty summary — the current run ingested no new rows
		// (the map phase ran in a previous attempt). Row counts for
		// validation are read from the checkpointed job progress, not
		// from this summary.
		return res, nil
	}

	// Restore SST metadata checkpointed from prior attempts. On the first
	// attempt these are empty. On retries, they contain manifests from files
	// written in previous attempts, ensuring already-written SSTs are not lost.
	processorOutput := make([]bulksst.SSTFiles, 0)
	var resumeManifests []jobspb.BulkSSTManifest
	if useDistributedMerge {
		var err error
		resumeManifests, err = readImportMergeManifests(ctx, execCtx, job)
		if err != nil {
			return kvpb.BulkOpSummary{}, err
		}
		if len(resumeManifests) > 0 {
			processorOutput = append(
				processorOutput, bulksst.ManifestsToSSTFiles(resumeManifests),
			)
		}
		checkpoint.manifestBuf = backfill.NewSSTManifestBuffer(resumeManifests)
	}

	metaFn := func(ctx context.Context, meta *execinfrapb.ProducerMetadata) error {
		if meta.BulkProcessorProgress != nil {
			// Decode map progress outside the lock since it doesn't touch
			// shared state.
			var manifests []jobspb.BulkSSTManifest
			var mapProgress execinfrapb.BulkMapProgress
			if gogotypes.Is(&meta.BulkProcessorProgress.ProgressDetails, &mapProgress) {
				if err := gogotypes.UnmarshalAny(&meta.BulkProcessorProgress.ProgressDetails, &mapProgress); err != nil {
					return err
				}
				manifests = mapProgress.SSTManifests
			}

			checkpoint.RecordProcessorUpdate(meta.BulkProcessorProgress, manifests)

			// Accumulate SST file info for the merge phase outside the
			// tracker since it's only used after the flow completes.
			if len(manifests) > 0 {
				processorOutput = append(processorOutput, bulksst.ManifestsToSSTFiles(manifests))
			}

			// For distributed merge, record storage prefix for nodes that report progress.
			if useDistributedMerge && meta.BulkProcessorProgress.NodeID != 0 {
				prefix := fmt.Sprintf("nodelocal://%d/", meta.BulkProcessorProgress.NodeID)
				if err := addStoragePrefix(ctx, prefix); err != nil {
					return err
				}
			}

			if testingKnobs.alwaysFlushJobProgress {
				return checkpoint.Persist(ctx, job)
			}
		}
		return nil
	}
	rowResultWriter := sql.NewCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
		var counts kvpb.BulkOpSummary
		if err := protoutil.Unmarshal([]byte(*row[0].(*tree.DBytes)), &counts); err != nil {
			return err
		}
		res.Add(counts)
		return nil
	})

	if planCtx.ExtendedEvalCtx.Codec.ForSystemTenant() {
		if err := presplitTableBoundaries(ctx, execCtx.ExecCfg(), table); err != nil {
			return kvpb.BulkOpSummary{}, err
		}
	}

	recv := sql.MakeDistSQLReceiver(
		ctx,
		sql.NewMetadataCallbackWriter(rowResultWriter, metaFn),
		tree.Rows,
		nil, /* rangeCache */
		nil, /* txn - the flow does not read or write the database */
		nil, /* clockUpdater */
		planCtx.ExtendedEvalCtx.Tracing,
	)
	defer recv.Release()

	replanChecker, cancelReplannerRaw := sql.PhysicalPlanChangeChecker(
		ctx, p, makePlan, execCtx,
		sql.ReplanOnChangedFraction(func() float64 { return replanThreshold.Get(&execCtx.ExecCfg().Settings.SV) }),
		func() time.Duration { return replanFrequency.Get(&execCtx.ExecCfg().Settings.SV) },
	)
	// Wrap cancelReplanner in a Once since it closes an internal channel
	// that panics on double-close. We call it both before entering the
	// merge phase and via defer.
	var cancelReplannerOnce sync.Once
	cancelReplanner := func() { cancelReplannerOnce.Do(cancelReplannerRaw) }

	stopProgress := make(chan struct{})
	var stopProgressOnce sync.Once
	// progressDone is closed when the progress ticker goroutine exits.
	// The DSP goroutine waits on this after closing stopProgress to
	// ensure no concurrent Persist calls overlap with the merge phase.
	progressDone := make(chan struct{})

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
		defer close(progressDone)
		tick := time.NewTicker(time.Second * 10)
		defer tick.Stop()
		done := ctx.Done()
		for {
			select {
			case <-stopProgress:
				return nil
			case <-done:
				return ctx.Err()
			case <-tick.C:
				if err := checkpoint.Persist(ctx, job); err != nil {
					return err
				}

			}
		}
	})

	g.GoCtx(func(ctx context.Context) error {
		defer cancelReplanner()
		defer stopProgressOnce.Do(func() { close(stopProgress) })

		if testingKnobs.beforeRunDSP != nil {
			if err := testingKnobs.beforeRunDSP(); err != nil {
				return err
			}
		}

		execCfg := execCtx.ExecCfg()
		jobsprofiler.StorePlanDiagram(ctx, execCfg.DistSQLSrv.Stopper, p, execCfg.InternalDB, job.ID())

		// Copy the evalCtx, as dsp.Run() might change it.
		evalCtxCopy := planCtx.ExtendedEvalCtx.Context.Copy()
		dsp.Run(ctx, planCtx, nil, p, recv, evalCtxCopy, testingKnobs.onSetupFinish)
		if err := rowResultWriter.Err(); err != nil {
			return err
		}
		if !useDistributedMerge {
			// For non-distributed-merge imports, correct the checkpoint's
			// accumulated EntryCounts with the actual deduplicated counts
			// from the processors. This prevents double-counting when rows
			// are reprocessed (e.g., if resumePos tracking fails).
			//
			// The processors return actual KV-ingested counts (via
			// fetchActualIngestedSummary) which are accumulated in `res`.
			// We replace the checkpoint's inflated counts with these actual
			// counts, analogous to how distributed merge corrects counts at
			// import_processor_planning.go:527-528.
			if len(res.EntryCounts) > 0 {
				checkpoint.CorrectEntryCounts(res.EntryCounts)
			}
			return nil
		}

		// Stop the map-phase checkpoint ticker and replanner before entering
		// the merge phase. The ticker writes map-phase manifests to the same
		// job info key that merge iteration checkpoints use; letting it
		// continue would risk overwriting merge output manifests with stale
		// map-phase data, causing data loss on resume.
		stopProgressOnce.Do(func() { close(stopProgress) })
		// Wait for the ticker goroutine to fully exit so there is no
		// concurrent Persist call in flight when we flush below.
		<-progressDone
		cancelReplanner()

		// Flush any remaining map-phase progress so manifests accumulated
		// since the last tick are persisted before we overwrite the key.
		if err := checkpoint.Persist(ctx, job); err != nil {
			return err
		}

		// TODO(jeffswenson): this isn't complete. We don't actually want to
		// generate splits for each index. What we want to do is generate splits
		// for each span config produced by the table that does not coalesce. For
		// example, a single RBR index would get split points between each of the
		// ranges.
		//
		// We should also consider making bulkingest tolerate ingesting an SST
		// that has data for multiple ranges. At the very least that will handle
		// the case where KV decides to run splits we were not expecting.
		schemaSpans := tabledesc.NewBuilder(table.Desc).BuildImmutableTable().AllIndexSpans(execCfg.Codec)
		return runImportMergeIterations(
			ctx, execCtx, job, checkpoint, processorOutput, schemaSpans, walltime, addStoragePrefix,
		)
	})

	g.GoCtx(replanChecker)

	if err := g.Wait(); err != nil {
		return kvpb.BulkOpSummary{}, err
	}

	// Persist the final checkpoint so the job progress summary reflects
	// all rows ingested in this distImport call. The periodic ticker may
	// not have fired since the last batch completed, so without this
	// explicit flush the summary could be stale.
	if err := checkpoint.Persist(ctx, job); err != nil {
		return kvpb.BulkOpSummary{}, err
	}

	return res, nil
}

// runImportMergeIterations runs the distributed merge loop for import. It reads
// DistributedMergePhase from the job progress to determine which iteration to
// start from. When resuming from a completed iteration (phase >= 1), it reads
// the checkpointed manifests from the job info key instead of using
// processorOutput.
//
// The merge runs 2 iterations: a local merge (iteration 1) has each node merge
// its local SSTs first, reducing network traffic. The final merge (iteration 2)
// merges cross-node and writes directly to KV.
//
// After non-final iterations, the output manifests are checkpointed to the job
// info key along with the completed phase number. This allows the job to resume
// from a completed iteration without re-running the map phase or earlier
// iterations.
func runImportMergeIterations(
	ctx context.Context,
	execCtx sql.JobExecContext,
	job *jobs.Job,
	checkpoint *importCheckpointTracker,
	processorOutput []bulksst.SSTFiles,
	schemaSpans []roachpb.Span,
	walltime int64,
	addStoragePrefix func(ctx context.Context, prefix string) error,
) error {
	progress := job.Progress()
	importProg := progress.GetImport()
	mergePhase := 0
	if importProg != nil {
		mergePhase = int(importProg.DistributedMergePhase)
	}
	const maxIterations = 2
	startIteration := mergePhase + 1

	// All merge iterations already completed in a previous run. This
	// happens when the final iteration finished and its checkpoint was
	// persisted (clearing manifests) but the job failed before being
	// marked successful. Nothing left to do.
	if startIteration > maxIterations {
		return nil
	}

	// On resume from a completed merge iteration, read the checkpointed
	// manifests from the job info key instead of using processor output.
	sstFiles := processorOutput
	if mergePhase >= 1 {
		manifests, err := readImportMergeManifests(ctx, execCtx, job)
		if err != nil {
			return err
		}
		if len(manifests) == 0 {
			return errors.AssertionFailedf(
				"distributed merge phase %d but no checkpointed manifests", mergePhase)
		}
		sstFiles = []bulksst.SSTFiles{bulksst.ManifestsToSSTFiles(manifests)}
	}

	inputSSTs, spans, err := bulksst.CombineFileInfo(sstFiles, schemaSpans)
	if err != nil {
		return err
	}

	// Run 2 merge iterations: a local merge (iteration 1) followed by a final
	// cross-node merge (iteration 2). The local merge reduces network traffic by
	// having each node merge its local SSTs first.
	currentSSTs := inputSSTs
	for iteration := startIteration; iteration <= maxIterations; iteration++ {
		// Clean up leftover SSTs from a previous interrupted attempt of this
		// non-final iteration, preventing orphaned files from wasting storage.
		progress := job.Progress()
		prog := progress.GetImport()
		var storagePrefixes []string
		if prog != nil {
			storagePrefixes = prog.SSTStoragePrefixes
		}
		cleanupImportRedoIterationSSTs(ctx, execCtx, job.ID(), iteration, maxIterations, storagePrefixes)

		var writeTS *hlc.Timestamp
		if iteration == maxIterations {
			writeTS = &hlc.Timestamp{WallTime: walltime}
		}

		merged, err := bulkmerge.Merge(
			ctx, execCtx, currentSSTs, spans,
			func(instanceID base.SQLInstanceID) (string, error) {
				// Record the storage prefix before any SSTs are written.
				prefix := fmt.Sprintf("nodelocal://%d/", instanceID)
				if err := addStoragePrefix(ctx, prefix); err != nil {
					return "", err
				}
				return prefix + bulkutil.NewDistMergePaths(job.ID()).MergePath(iteration), nil
			},
			bulkmerge.MergeOptions{
				Iteration:         iteration,
				MaxIterations:     maxIterations,
				WriteTimestamp:    writeTS,
				EnforceUniqueness: true,
				MemoryMonitor:     execinfrapb.BulkMergeSpec_BULK_MONITOR,
			},
		)
		if err != nil {
			return err
		}

		// Checkpoint via the tracker so manifest serialization uses the
		// same codepath as map-phase checkpoints.
		if iteration < maxIterations {
			manifests := bulksst.MergeOutputToManifests(merged.SSTs)
			checkpoint.SetMergeIterationResult(manifests, int32(iteration))
		} else {
			// Final iteration: correct the entry counts to reflect actual
			// KV-ingested totals. The map-phase counts include duplicates
			// from checkpoint-and-resume overlap; the merge's ingest
			// summary has the true count after skipping identical dupes.
			if len(merged.IngestSummary.EntryCounts) > 0 {
				checkpoint.CorrectEntryCounts(merged.IngestSummary.EntryCounts)
			}
			checkpoint.SetMergeComplete(int32(iteration))
		}
		if err := checkpoint.Persist(ctx, job); err != nil {
			return err
		}

		// Pausepoint after checkpointing a non-final iteration so tests
		// can verify resume from a completed merge iteration.
		if iteration < maxIterations {
			if err := execCtx.ExecCfg().JobRegistry.CheckPausepoint(
				"import.after_merge_iteration",
			); err != nil {
				return err
			}
		}

		currentSSTs = merged.SSTs
	}

	return nil
}

// readImportMergeManifests reads SST manifests from the job info key. Returns
// nil if no manifests are stored.
func readImportMergeManifests(
	ctx context.Context, execCtx sql.JobExecContext, job *jobs.Job,
) ([]jobspb.BulkSSTManifest, error) {
	var manifests []jobspb.BulkSSTManifest
	if err := execCtx.ExecCfg().InternalDB.Txn(ctx, func(
		ctx context.Context, txn isql.Txn,
	) error {
		data, err := jobs.ReadChunkedFileToJobInfo(
			ctx, importSSTManifestsInfoKey, txn, job.ID(),
		)
		if err != nil {
			return err
		}
		if len(data) > 0 {
			var stored jobspb.BulkSSTManifests
			if err := protoutil.Unmarshal(data, &stored); err != nil {
				return errors.Wrap(err, "unmarshaling SST manifests from job info")
			}
			manifests = stored.Manifests
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return manifests, nil
}

// cleanupImportRedoIterationSSTs removes leftover output SSTs from a previous
// attempt of a non-final merge iteration. Best-effort: errors are logged as
// warnings rather than failing the job.
func cleanupImportRedoIterationSSTs(
	ctx context.Context,
	execCtx sql.JobExecContext,
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
		execCtx.ExecCfg().DistSQLSrv.ExternalStorageFromURI, username.NodeUserName(),
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

func getLastImportSummary(job *jobs.Job) kvpb.BulkOpSummary {
	progress := job.Progress()
	importProgress := progress.GetImport()
	return importProgress.Summary
}

func makeImportReaderSpecs(
	job *jobs.Job,
	table *execinfrapb.ReadImportDataSpec_ImportTable,
	typeDescs []*descpb.TypeDescriptor,
	from []string,
	format roachpb.IOFileFormat,
	numSQLInstances int,
	walltime int64,
	user username.SQLUsername,
	procsPerNode int,
	initialSplitsPerProc int,
) []*execinfrapb.ReadImportDataSpec {
	details := job.Details().(jobspb.ImportDetails)
	// For each input file, assign it to a node.
	inputSpecs := make([]*execinfrapb.ReadImportDataSpec, 0, numSQLInstances*procsPerNode)
	progress := job.Progress()
	importProgress := progress.GetImport()
	var distributedMergeFilePrefix string
	if details.UseDistributedMerge {
		distributedMergeFilePrefix = bulkutil.NewDistMergePaths(job.ID()).MapPath()
	}
	for i, input := range from {
		// Round robin assign CSV files to processors. Files 0 through len(specs)-1
		// creates the spec. Future files just add themselves to the Uris.
		if i < cap(inputSpecs) {
			spec := &execinfrapb.ReadImportDataSpec{
				JobID:  int64(job.ID()),
				Table:  table,
				Tables: map[string]*execinfrapb.ReadImportDataSpec_ImportTable{"": table},
				Types:  typeDescs,
				Format: format,
				Progress: execinfrapb.JobProgress{
					JobID: job.ID(),
					Slot:  int32(i),
				},
				WalltimeNanos:              walltime,
				Uri:                        make(map[int32]string),
				ResumePos:                  make(map[int32]int64),
				UserProto:                  user.EncodeProto(),
				DatabasePrimaryRegion:      details.DatabasePrimaryRegion,
				InitialSplits:              int32(initialSplitsPerProc),
				UseDistributedMerge:        details.UseDistributedMerge,
				DistributedMergeFilePrefix: distributedMergeFilePrefix,
			}
			inputSpecs = append(inputSpecs, spec)
		}
		n := i % len(inputSpecs)
		inputSpecs[n].Uri[int32(i)] = input
		if importProgress.ResumePos != nil {
			inputSpecs[n].ResumePos[int32(i)] = importProgress.ResumePos[int32(i)]
		}
	}

	for i := range inputSpecs {
		// TODO(mjibson): using the actual file sizes here would improve progress
		// accuracy.
		inputSpecs[i].Progress.Contribution = float32(len(inputSpecs[i].Uri)) / float32(len(from))
	}
	return inputSpecs
}

func presplitTableBoundaries(
	ctx context.Context, cfg *sql.ExecutorConfig, table *execinfrapb.ReadImportDataSpec_ImportTable,
) error {
	var span *tracing.Span
	ctx, span = tracing.ChildSpan(ctx, "import-pre-splitting-table-boundaries")
	defer span.Finish()
	expirationTime := cfg.DB.Clock().Now().Add(time.Hour.Nanoseconds(), 0)
	// TODO(ajwerner): Consider passing in the wrapped descriptors.
	tblDesc := tabledesc.NewBuilder(table.Desc).BuildImmutableTable()
	for _, span := range tblDesc.AllIndexSpans(cfg.Codec) {
		if err := cfg.DB.AdminSplit(ctx, span.Key, expirationTime); err != nil {
			return err
		}
	}
	return nil
}
