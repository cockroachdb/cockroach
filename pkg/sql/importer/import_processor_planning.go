// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"context"
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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

// distImport is used by IMPORT to run a DistSQL flow to ingest data by starting
// reader processes on many nodes that each read and ingest their assigned files
// and then send back a summary of what they ingested. The combined summary is
// returned.
func distImport(
	ctx context.Context,
	execCtx sql.JobExecContext,
	job *jobs.Job,
	tables map[string]*execinfrapb.ReadImportDataSpec_ImportTable,
	typeDescs []*descpb.TypeDescriptor,
	from []string,
	format roachpb.IOFileFormat,
	walltime int64,
	testingKnobs importTestingKnobs,
	procsPerNode int,
) (kvpb.BulkOpSummary, error) {
	ctx, sp := tracing.ChildSpan(ctx, "importer.distImport")
	defer sp.Finish()

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

		inputSpecs := makeImportReaderSpecs(job, tables, typeDescs, from, format, sqlInstanceIDs, walltime,
			execCtx.User(), procsPerNode)

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
			[]*types.T{types.Bytes, types.Bytes},
			execinfrapb.Ordering{},
		)

		p.PlanToStreamColMap = []int{0, 1}

		sql.FinalizePlan(ctx, planCtx, p)
		return p, planCtx, nil
	}

	p, planCtx, err := makePlan(ctx, dsp)
	if err != nil {
		return kvpb.BulkOpSummary{}, err
	}
	evalCtx := planCtx.ExtendedEvalCtx

	// accumulatedBulkSummary accumulates the BulkOpSummary returned from each
	// processor in their progress updates. It stores stats about the amount of
	// data written since the last time we update the job progress.
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
		return job.NoTxn().FractionProgressed(ctx, func(
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
			prog.Summary.Add(accumulatedBulkSummary.BulkOpSummary)
			accumulatedBulkSummary.Reset()
			accumulatedBulkSummary.Unlock()
			return overall / float32(len(from))
		},
		)
	}

	metaFn := func(_ context.Context, meta *execinfrapb.ProducerMetadata) error {
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

			if testingKnobs.alwaysFlushJobProgress {
				return updateJobProgress()
			}
		}
		return nil
	}

	var res kvpb.BulkOpSummary
	rowResultWriter := sql.NewCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
		var counts kvpb.BulkOpSummary
		if err := protoutil.Unmarshal([]byte(*row[0].(*tree.DBytes)), &counts); err != nil {
			return err
		}
		res.Add(counts)
		return nil
	})

	if evalCtx.Codec.ForSystemTenant() {
		if err := presplitTableBoundaries(ctx, execCtx.ExecCfg(), tables); err != nil {
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
		evalCtx.Tracing,
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
		evalCtxCopy := *evalCtx
		dsp.Run(ctx, planCtx, nil, p, recv, &evalCtxCopy, testingKnobs.onSetupFinish)
		return rowResultWriter.Err()
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
	tables map[string]*execinfrapb.ReadImportDataSpec_ImportTable,
	typeDescs []*descpb.TypeDescriptor,
	from []string,
	format roachpb.IOFileFormat,
	sqlInstanceIDs []base.SQLInstanceID,
	walltime int64,
	user username.SQLUsername,
	procsPerNode int,
) []*execinfrapb.ReadImportDataSpec {
	details := job.Details().(jobspb.ImportDetails)
	// For each input file, assign it to a node.
	inputSpecs := make([]*execinfrapb.ReadImportDataSpec, 0, len(sqlInstanceIDs)*procsPerNode)
	progress := job.Progress()
	importProgress := progress.GetImport()
	for i, input := range from {
		// Round robin assign CSV files to processors. Files 0 through len(specs)-1
		// creates the spec. Future files just add themselves to the Uris.
		if i < cap(inputSpecs) {
			spec := &execinfrapb.ReadImportDataSpec{
				JobID:  int64(job.ID()),
				Tables: tables,
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
				InitialSplits:         int32(len(sqlInstanceIDs)),
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
	ctx context.Context,
	cfg *sql.ExecutorConfig,
	tables map[string]*execinfrapb.ReadImportDataSpec_ImportTable,
) error {
	var span *tracing.Span
	ctx, span = tracing.ChildSpan(ctx, "import-pre-splitting-table-boundaries")
	defer span.Finish()
	expirationTime := cfg.DB.Clock().Now().Add(time.Hour.Nanoseconds(), 0)
	for _, tbl := range tables {
		// TODO(ajwerner): Consider passing in the wrapped descriptors.
		tblDesc := tabledesc.NewBuilder(tbl.Desc).BuildImmutableTable()
		for _, span := range tblDesc.AllIndexSpans(cfg.Codec) {
			if err := cfg.DB.AdminSplit(ctx, span.Key, expirationTime); err != nil {
				return err
			}
		}
	}
	return nil
}
