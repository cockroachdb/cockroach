// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeeddist

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// ChangefeedResultTypes is the types returned by changefeed stream.
var ChangefeedResultTypes = []*types.T{
	types.Bytes,  // resolved span
	types.String, // topic
	types.Bytes,  // key
	types.Bytes,  // value
}

// StartDistChangefeed starts distributed changefeed execution.
func StartDistChangefeed(
	ctx context.Context,
	execCtx sql.JobExecContext,
	jobID jobspb.JobID,
	details jobspb.ChangefeedDetails,
	trackedSpans []roachpb.Span,
	initialHighWater hlc.Timestamp,
	checkpoint jobspb.ChangefeedProgress_Checkpoint,
	resultsCh chan<- tree.Datums,
) error {
	// Changefeed flows handle transactional consistency themselves.
	var noTxn *kv.Txn

	dsp := execCtx.DistSQLPlanner()
	evalCtx := execCtx.ExtendedEvalContext()
	planCtx := dsp.NewPlanningCtx(ctx, evalCtx, nil /* planner */, noTxn,
		execCtx.ExecCfg().Codec.ForSystemTenant() /* distribute */)

	var spanPartitions []sql.SpanPartition
	if details.SinkURI == `` {
		// Sinkless feeds get one ChangeAggregator on the gateway.
		spanPartitions = []sql.SpanPartition{{Node: dsp.GatewayID(), Spans: trackedSpans}}
	} else {
		// All other feeds get a ChangeAggregator local on the leaseholder.
		var err error
		spanPartitions, err = dsp.PartitionSpans(planCtx, trackedSpans)
		if err != nil {
			return err
		}
	}

	// Use the same checkpoint for all aggregators; each aggregator will only look at
	// spans that are assigned to it.
	// We could compute per-aggregator checkpoint, but that's probably an overkill.
	aggregatorCheckpoint := execinfrapb.ChangeAggregatorSpec_Checkpoint{
		Spans: checkpoint.Spans,
	}

	corePlacement := make([]physicalplan.ProcessorCorePlacement, len(spanPartitions))
	for i, sp := range spanPartitions {
		watches := make([]execinfrapb.ChangeAggregatorSpec_Watch, len(sp.Spans))
		for watchIdx, nodeSpan := range sp.Spans {
			watches[watchIdx] = execinfrapb.ChangeAggregatorSpec_Watch{
				Span:            nodeSpan,
				InitialResolved: initialHighWater,
			}
		}

		spec := &execinfrapb.ChangeAggregatorSpec{
			Watches:    watches,
			Checkpoint: aggregatorCheckpoint,
			Feed:       details,
			UserProto:  execCtx.User().EncodeProto(),
			JobID:      jobID,
		}
		corePlacement[i].NodeID = sp.Node
		corePlacement[i].Core.ChangeAggregator = spec
	}

	// NB: This SpanFrontier processor depends on the set of tracked spans being
	// static. Currently there is no way for them to change after the changefeed
	// is created, even if it is paused and unpaused, but #28982 describes some
	// ways that this might happen in the future.
	changeFrontierSpec := execinfrapb.ChangeFrontierSpec{
		TrackedSpans: trackedSpans,
		Feed:         details,
		JobID:        jobID,
		UserProto:    execCtx.User().EncodeProto(),
	}

	p := planCtx.NewPhysicalPlan()
	p.AddNoInputStage(corePlacement, execinfrapb.PostProcessSpec{}, ChangefeedResultTypes, execinfrapb.Ordering{})
	p.AddSingleGroupStage(
		dsp.GatewayID(),
		execinfrapb.ProcessorCoreUnion{ChangeFrontier: &changeFrontierSpec},
		execinfrapb.PostProcessSpec{},
		ChangefeedResultTypes,
	)

	p.PlanToStreamColMap = []int{1, 2, 3}
	dsp.FinalizePlan(planCtx, p)

	resultRows := makeChangefeedResultWriter(resultsCh)
	recv := sql.MakeDistSQLReceiver(
		ctx,
		resultRows,
		tree.Rows,
		execCtx.ExecCfg().RangeDescriptorCache,
		noTxn,
		nil, /* clockUpdater */
		evalCtx.Tracing,
		execCtx.ExecCfg().ContentionRegistry,
		nil, /* testingPushCallback */
	)
	defer recv.Release()

	var finishedSetupFn func()
	if details.SinkURI != `` {
		// We abuse the job's results channel to make CREATE CHANGEFEED wait for
		// this before returning to the user to ensure the setup went okay. Job
		// resumption doesn't have the same hack, but at the moment ignores
		// results and so is currently okay. Return nil instead of anything
		// meaningful so that if we start doing anything with the results
		// returned by resumed jobs, then it breaks instead of returning
		// nonsense.
		finishedSetupFn = func() { resultsCh <- tree.Datums(nil) }
	}

	// Copy the evalCtx, as dsp.Run() might change it.
	evalCtxCopy := *evalCtx
	dsp.Run(planCtx, noTxn, p, recv, &evalCtxCopy, finishedSetupFn)()
	return resultRows.Err()
}

// changefeedResultWriter implements the `sql.rowResultWriter` that sends
// the received rows back over the given channel.
type changefeedResultWriter struct {
	rowsCh       chan<- tree.Datums
	rowsAffected int
	err          error
}

func makeChangefeedResultWriter(rowsCh chan<- tree.Datums) *changefeedResultWriter {
	return &changefeedResultWriter{rowsCh: rowsCh}
}

func (w *changefeedResultWriter) AddRow(ctx context.Context, row tree.Datums) error {
	// Copy the row because it's not guaranteed to exist after this function
	// returns.
	row = append(tree.Datums(nil), row...)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case w.rowsCh <- row:
		return nil
	}
}
func (w *changefeedResultWriter) IncrementRowsAffected(ctx context.Context, n int) {
	w.rowsAffected += n
}
func (w *changefeedResultWriter) SetError(err error) {
	w.err = err
}
func (w *changefeedResultWriter) Err() error {
	return w.err
}
