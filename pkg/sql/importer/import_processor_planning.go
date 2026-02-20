// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync/atomic"
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
	"github.com/cockroachdb/cockroach/pkg/sql/bulkmerge"
	"github.com/cockroachdb/cockroach/pkg/sql/bulksst"
	"github.com/cockroachdb/cockroach/pkg/sql/bulkutil"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
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
		outputTypes := []*types.T{types.Bytes, types.Bytes}
		if useDistributedMerge {
			outputTypes = []*types.T{types.Bytes, types.Bytes, types.Bytes}
		}
		p.AddNoInputStage(
			corePlacement,
			execinfrapb.PostProcessSpec{},
			// The direct-ingest readers will emit a binary encoded BulkOpSummary.
			outputTypes,
			execinfrapb.Ordering{},
			nil, /* finalizeLastStageCb */
		)
		// Map the output directly back.
		colMap := make([]int, len(outputTypes))
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

	// accumulatedBulkSummary accumulates the BulkOpSummary returned from each
	// processor in their progress updates. It is used to update the job progress.
	accumulatedBulkSummary := struct {
		syncutil.Mutex
		kvpb.BulkOpSummary
	}{}
	accumulatedBulkSummary.Lock()
	accumulatedBulkSummary.BulkOpSummary = getLastImportSummary(job)
	accumulatedBulkSummary.Unlock()

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

	rowProgress := make([]int64, len(from))
	fractionProgress := make([]uint32, len(from))

	updateJobProgress := func() error {
		return job.DebugNameNoTxn(importProgressDebugName).FractionProgressed(ctx, func(
			ctx context.Context, details jobspb.ProgressDetails,
		) float32 {
			var overall float32
			prog := details.(*jobspb.Progress_Import).Import
			for i := range rowProgress {
				prog.ResumePos[i] = atomic.LoadInt64(&rowProgress[i])
			}
			for i := range fractionProgress {
				fileProgress := math.Float32frombits(atomic.LoadUint32(&fractionProgress[i]))
				prog.ReadProgress[i] = fileProgress
				overall += fileProgress
			}

			accumulatedBulkSummary.Lock()
			prog.Summary = accumulatedBulkSummary.BulkOpSummary.DeepCopy()
			accumulatedBulkSummary.Unlock()
			return overall / float32(len(from))
		},
		)
	}

	metaFn := func(ctx context.Context, meta *execinfrapb.ProducerMetadata) error {
		if meta.BulkProcessorProgress != nil {
			for i, v := range meta.BulkProcessorProgress.ResumePos {
				atomic.StoreInt64(&rowProgress[i], v)
			}
			for i, v := range meta.BulkProcessorProgress.CompletedFraction {
				atomic.StoreUint32(&fractionProgress[i], math.Float32bits(v))
			}

			accumulatedBulkSummary.Lock()
			accumulatedBulkSummary.Add(meta.BulkProcessorProgress.BulkSummary)
			accumulatedBulkSummary.Unlock()

			// For distributed merge, record storage prefix for nodes that report progress
			if useDistributedMerge && meta.BulkProcessorProgress.NodeID != 0 {
				prefix := fmt.Sprintf("nodelocal://%d/", meta.BulkProcessorProgress.NodeID)
				if err := addStoragePrefix(ctx, prefix); err != nil {
					return err
				}
			}

			if testingKnobs.alwaysFlushJobProgress {
				return updateJobProgress()
			}
		}
		return nil
	}

	var res kvpb.BulkOpSummary
	var processorOutput []bulksst.SSTFiles
	rowResultWriter := sql.NewCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
		var counts kvpb.BulkOpSummary
		if err := protoutil.Unmarshal([]byte(*row[0].(*tree.DBytes)), &counts); err != nil {
			return err
		}
		res.Add(counts)
		if len(row) == 3 {
			var sstFiles bulksst.SSTFiles
			if err := protoutil.Unmarshal([]byte(*row[2].(*tree.DBytes)), &sstFiles); err != nil {
				return err
			}
			processorOutput = append(processorOutput, sstFiles)
		}
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

	replanChecker, cancelReplanner := sql.PhysicalPlanChangeChecker(
		ctx, p, makePlan, execCtx,
		sql.ReplanOnChangedFraction(func() float64 { return replanThreshold.Get(&execCtx.ExecCfg().Settings.SV) }),
		func() time.Duration { return replanFrequency.Get(&execCtx.ExecCfg().Settings.SV) },
	)

	stopProgress := make(chan struct{})

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(func(ctx context.Context) error {
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
				if err := updateJobProgress(); err != nil {
					return err
				}

			}
		}
	})

	g.GoCtx(func(ctx context.Context) error {
		defer cancelReplanner()
		defer close(stopProgress)

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
			return nil
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
		inputSSTs, spans, err := bulksst.CombineFileInfo(processorOutput, schemaSpans)
		if err != nil {
			return err
		}

		writeTS := &hlc.Timestamp{WallTime: walltime}
		_, err = bulkmerge.Merge(ctx, execCtx, inputSSTs, spans, func(instanceID base.SQLInstanceID) (string, error) {
			// Record the storage prefix before any SSTs are written
			prefix := fmt.Sprintf("nodelocal://%d/", instanceID)
			if err := addStoragePrefix(ctx, prefix); err != nil {
				return "", err
			}
			return prefix + bulkutil.NewDistMergePaths(job.ID()).MergePath(1), nil
		}, bulkmerge.MergeOptions{
			Iteration:      1,
			MaxIterations:  1,
			WriteTimestamp: writeTS,
			// TODO(161447): uniqueness is currently not enforced
			EnforceUniqueness: false,
			MemoryMonitor:     execinfrapb.BulkMergeSpec_BULK_MONITOR,
		})

		return err
	})

	g.GoCtx(replanChecker)

	if err := g.Wait(); err != nil {
		return kvpb.BulkOpSummary{}, err
	}

	return res, nil
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
				WalltimeNanos:         walltime,
				Uri:                   make(map[int32]string),
				ResumePos:             make(map[int32]int64),
				UserProto:             user.EncodeProto(),
				DatabasePrimaryRegion: details.DatabasePrimaryRegion,
				InitialSplits:         int32(initialSplitsPerProc),
				UseDistributedMerge:   details.UseDistributedMerge,
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
