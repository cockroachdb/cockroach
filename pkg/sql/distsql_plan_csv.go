// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/logtags"
)

// RowResultWriter is a thin wrapper around a RowContainer.
type RowResultWriter struct {
	rowContainer *rowContainerHelper
	rowsAffected int
	err          error
}

var _ rowResultWriter = &RowResultWriter{}

// NewRowResultWriter creates a new RowResultWriter.
func NewRowResultWriter(rowContainer *rowContainerHelper) *RowResultWriter {
	return &RowResultWriter{rowContainer: rowContainer}
}

// IncrementRowsAffected implements the rowResultWriter interface.
func (b *RowResultWriter) IncrementRowsAffected(ctx context.Context, n int) {
	b.rowsAffected += n
}

// AddRow implements the rowResultWriter interface.
func (b *RowResultWriter) AddRow(ctx context.Context, row tree.Datums) error {
	if b.rowContainer != nil {
		return b.rowContainer.addRow(ctx, row)
	}
	return nil
}

// SetError is part of the rowResultWriter interface.
func (b *RowResultWriter) SetError(err error) {
	b.err = err
}

// Err is part of the rowResultWriter interface.
func (b *RowResultWriter) Err() error {
	return b.err
}

// callbackResultWriter is a rowResultWriter that runs a callback function
// on AddRow.
type callbackResultWriter struct {
	fn           func(ctx context.Context, row tree.Datums) error
	rowsAffected int
	err          error
}

var _ rowResultWriter = &callbackResultWriter{}

// newCallbackResultWriter creates a new callbackResultWriter.
func newCallbackResultWriter(
	fn func(ctx context.Context, row tree.Datums) error,
) *callbackResultWriter {
	return &callbackResultWriter{fn: fn}
}

func (c *callbackResultWriter) IncrementRowsAffected(ctx context.Context, n int) {
	c.rowsAffected += n
}

func (c *callbackResultWriter) AddRow(ctx context.Context, row tree.Datums) error {
	return c.fn(ctx, row)
}

func (c *callbackResultWriter) SetError(err error) {
	c.err = err
}

func (c *callbackResultWriter) Err() error {
	return c.err
}

func makeImportReaderSpecs(
	job *jobs.Job,
	tables map[string]*execinfrapb.ReadImportDataSpec_ImportTable,
	from []string,
	format roachpb.IOFileFormat,
	nodes []roachpb.NodeID,
	walltime int64,
	user security.SQLUsername,
) []*execinfrapb.ReadImportDataSpec {

	// For each input file, assign it to a node.
	inputSpecs := make([]*execinfrapb.ReadImportDataSpec, 0, len(nodes))
	progress := job.Progress()
	importProgress := progress.GetImport()
	for i, input := range from {
		// Round robin assign CSV files to nodes. Files 0 through len(nodes)-1
		// creates the spec. Future files just add themselves to the Uris.
		if i < len(nodes) {
			spec := &execinfrapb.ReadImportDataSpec{
				Tables: tables,
				Format: format,
				Progress: execinfrapb.JobProgress{
					JobID: job.ID(),
					Slot:  int32(i),
				},
				WalltimeNanos: walltime,
				Uri:           make(map[int32]string),
				ResumePos:     make(map[int32]int64),
				UserProto:     user.EncodeProto(),
			}
			inputSpecs = append(inputSpecs, spec)
		}
		n := i % len(nodes)
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
	cfg *ExecutorConfig,
	tables map[string]*execinfrapb.ReadImportDataSpec_ImportTable,
) error {
	expirationTime := cfg.DB.Clock().Now().Add(time.Hour.Nanoseconds(), 0)
	for _, tbl := range tables {
		// TODO(ajwerner): Consider passing in the wrapped descriptors.
		tblDesc := tabledesc.NewBuilder(tbl.Desc).BuildImmutableTable()
		for _, span := range tblDesc.AllIndexSpans(cfg.Codec) {
			if err := cfg.DB.AdminSplit(ctx, span.Key, expirationTime); err != nil {
				return err
			}

			log.VEventf(ctx, 1, "scattering index range %s", span.Key)
			scatterReq := &roachpb.AdminScatterRequest{
				RequestHeader: roachpb.RequestHeaderFromSpan(span),
			}
			if _, pErr := kv.SendWrapped(ctx, cfg.DB.NonTransactionalSender(), scatterReq); pErr != nil {
				log.Errorf(ctx, "failed to scatter span %s: %s", span.Key, pErr)
			}
		}
	}
	return nil
}

// DistIngest is used by IMPORT to run a DistSQL flow to ingest data by starting
// reader processes on many nodes that each read and ingest their assigned files
// and then send back a summary of what they ingested. The combined summary is
// returned.
func DistIngest(
	ctx context.Context,
	execCtx JobExecContext,
	job *jobs.Job,
	tables map[string]*execinfrapb.ReadImportDataSpec_ImportTable,
	from []string,
	format roachpb.IOFileFormat,
	walltime int64,
	alwaysFlushProgress bool,
) (roachpb.BulkOpSummary, error) {
	ctx = logtags.AddTag(ctx, "import-distsql-ingest", nil)

	dsp := execCtx.DistSQLPlanner()
	evalCtx := execCtx.ExtendedEvalContext()

	planCtx, nodes, err := dsp.SetupAllNodesPlanning(ctx, evalCtx, execCtx.ExecCfg())
	if err != nil {
		return roachpb.BulkOpSummary{}, err
	}

	inputSpecs := makeImportReaderSpecs(job, tables, from, format, nodes, walltime, execCtx.User())

	p := planCtx.NewPhysicalPlan()

	// Setup a one-stage plan with one proc per input spec.
	corePlacement := make([]physicalplan.ProcessorCorePlacement, len(inputSpecs))
	for i := range inputSpecs {
		corePlacement[i].NodeID = nodes[i]
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

			if alwaysFlushProgress {
				return updateJobProgress()
			}
		}
		return nil
	}

	var res roachpb.BulkOpSummary
	rowResultWriter := newCallbackResultWriter(func(ctx context.Context, row tree.Datums) error {
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

	recv := MakeDistSQLReceiver(
		ctx,
		&MetadataCallbackWriter{rowResultWriter: rowResultWriter, fn: metaFn},
		tree.Rows,
		nil, /* rangeCache */
		nil, /* txn - the flow does not read or write the database */
		nil, /* clockUpdater */
		evalCtx.Tracing,
		evalCtx.ExecCfg.ContentionRegistry,
		nil, /* testingPushCallback */
	)
	defer recv.Release()

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
		defer close(stopProgress)
		// Copy the evalCtx, as dsp.Run() might change it.
		evalCtxCopy := *evalCtx
		dsp.Run(planCtx, nil, p, recv, &evalCtxCopy, nil /* finishedSetupFn */)()
		return rowResultWriter.Err()
	})

	if err := g.Wait(); err != nil {
		return roachpb.BulkOpSummary{}, err
	}

	return res, nil
}
