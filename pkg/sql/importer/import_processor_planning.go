// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/oppurpose"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/logtags"
)

var replanThreshold = settings.RegisterFloatSetting(
	settings.TenantWritable,
	"bulkio.import.replan_flow_threshold",
	"fraction of initial flow instances that would be added or updated above which an IMPORT is restarted from its last checkpoint (0=disabled)",
	0.0,
)

var replanFrequency = settings.RegisterDurationSetting(
	settings.TenantWritable,
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
) (roachpb.BulkOpSummary, error) {
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

		dsp.FinalizePlan(planCtx, p)
		return p, planCtx, nil
	}

	p, planCtx, err := makePlan(ctx, dsp)
	if err != nil {
		return roachpb.BulkOpSummary{}, err
	}
	evalCtx := planCtx.ExtendedEvalCtx

	// accumulatedBulkSummary accumulates the BulkOpSummary returned from each
	// processor in their progress updates. It stores stats about the amount of
	// data written since the last time we update the job progress.
	accumulatedBulkSummary := struct {
		syncutil.Mutex
		roachpb.BulkOpSummary
	}{}
	accumulatedBulkSummary.Lock()
	accumulatedBulkSummary.BulkOpSummary = getLastImportSummary(job)
	accumulatedBulkSummary.Unlock()

	importDetails := job.Progress().Details.(*jobspb.Progress_Import).Import
	if importDetails.ReadProgress == nil {
		// Initialize the progress metrics on the first attempt.
		if err := job.FractionProgressed(ctx, nil, /* txn */
			func(ctx context.Context, details jobspb.ProgressDetails) float32 {
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
			return roachpb.BulkOpSummary{}, err
		}
	}

	rowProgress := make([]int64, len(from))
	fractionProgress := make([]uint32, len(from))

	updateJobProgress := func() error {
		return job.FractionProgressed(ctx, nil, /* txn */
			func(ctx context.Context, details jobspb.ProgressDetails) float32 {
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

	var res roachpb.BulkOpSummary
	rowResultWriter := sql.NewCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
		var counts roachpb.BulkOpSummary
		if err := protoutil.Unmarshal([]byte(*row[0].(*tree.DBytes)), &counts); err != nil {
			return err
		}
		res.Add(counts)
		return nil
	})

	if evalCtx.Codec.ForSystemTenant() {
		if err := presplitTableBoundaries(ctx, execCtx.ExecCfg(), tables); err != nil {
			return roachpb.BulkOpSummary{}, err
		}
	}

	flowCtx, watcher := newCancelWatcher(ctx, 3*progressUpdateInterval)
	recv := sql.MakeDistSQLReceiver(
		flowCtx,
		sql.NewMetadataCallbackWriter(rowResultWriter, metaFn),
		tree.Rows,
		nil, /* rangeCache */
		nil, /* txn - the flow does not read or write the database */
		nil, /* clockUpdater */
		evalCtx.Tracing,
		evalCtx.ExecCfg.ContentionRegistry,
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
		watcher.watch(ctx, recv)
		return nil
	})
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
		defer watcher.stop()

		if testingKnobs.beforeRunDSP != nil {
			if err := testingKnobs.beforeRunDSP(); err != nil {
				return err
			}
		}

		if testingKnobs.beforeRunDSP != nil {
			if err := testingKnobs.beforeRunDSP(); err != nil {
				return err
			}
		}

		// Copy the evalCtx, as dsp.Run() might change it.
		evalCtxCopy := *evalCtx
		dsp.Run(flowCtx, planCtx, nil, p, recv, &evalCtxCopy, testingKnobs.onSetupFinish)
		return rowResultWriter.Err()
	})

	g.GoCtx(replanChecker)

	if err := g.Wait(); err != nil {
		return roachpb.BulkOpSummary{}, err
	}

	return res, nil
}

// cancelWatcher is used to handle job PAUSE and CANCEL
// gracefully.
//
// When the a job is canceled or paused, the context passed to the
// Resumer is canceled by the job system. Rather than passing the
// Resumer's context directly to our DistSQL receiver and planner, we
// instead construct a new context for the distsql machinery and watch
// the original context for cancelation using this cancelWatcher.
//
// When the cancelWatcher sees a cancelation on the watched context,
// it informs the DistSQL receiver passed to watch by calling
// SetError. This gives the DistSQL flow a chance to drain gracefully.
//
// However, we do not want to wait forever. After SetError is called,
// we wait up to `timeout` before canceling the context returned by
// newCancelWatcher.
type cancelWatcher struct {
	watchedCtx context.Context
	timeout    time.Duration

	done   chan struct{}
	cancel context.CancelFunc
}

// newCancelWatcher constructs a cancelWatcher. To start the watcher
// call watch. The context passed to newCancelWatcher will be watched
// for cancelation. The returned context should be used for the
// DistSQL receiver and DistSQLPlanner.
func newCancelWatcher(
	ctxToWatch context.Context, timeout time.Duration,
) (context.Context, *cancelWatcher) {
	ctx, cancel := context.WithCancel(
		logtags.AddTags(
			context.Background(),
			logtags.FromContext(ctxToWatch)))
	return ctx, &cancelWatcher{
		watchedCtx: ctxToWatch,
		timeout:    timeout,

		done:   make(chan struct{}),
		cancel: cancel,
	}
}

// watch starts watching the context passed to newCancelWatcher for
// cancellation and notifies the given DistSQLReceiver when a
// cancellation occurs.
//
// After cancellation, if the watcher is not stopped before the
// configured timeout, the context returned from the constructor is
// cancelled.
func (c *cancelWatcher) watch(ctx context.Context, recv *sql.DistSQLReceiver) {
	select {
	case <-c.watchedCtx.Done():
		recv.SetError(c.watchedCtx.Err())
		timer := timeutil.NewTimer()
		defer timer.Stop()
		timer.Reset(c.timeout)
		select {
		case <-c.done:
		case <-timer.C:
			timer.Read = true
			log.Warningf(ctx, "watcher not stopped after %s, canceling flow context", c.timeout)
			c.cancel()
		}
	case <-c.done:
	}
}

func (c *cancelWatcher) stop() {
	close(c.done)
}

func getLastImportSummary(job *jobs.Job) roachpb.BulkOpSummary {
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
			if err := cfg.DB.AdminSplit(ctx, span.Key, expirationTime, oppurpose.SplitImport); err != nil {
				return err
			}
		}
	}
	return nil
}
