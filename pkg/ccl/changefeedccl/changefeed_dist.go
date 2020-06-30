// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/rowexec"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func init() {
	rowexec.NewChangeAggregatorProcessor = newChangeAggregatorProcessor
	rowexec.NewChangeFrontierProcessor = newChangeFrontierProcessor
}

const (
	changeAggregatorProcName = `changeagg`
	changeFrontierProcName   = `changefntr`
)

var changefeedResultTypes = []*types.T{
	types.Bytes,  // resolved span
	types.String, // topic
	types.Bytes,  // key
	types.Bytes,  // value
}

// distChangefeedFlow plans and runs a distributed changefeed.
//
// One or more ChangeAggregator processors watch table data for changes. These
// transform the changed kvs into changed rows and either emit them to a sink
// (such as kafka) or, if there is no sink, forward them in columns 1,2,3 (where
// they will be eventually returned directly via pgwire). In either case,
// periodically a span will become resolved as of some timestamp, meaning that
// no new rows will ever be emitted at or below that timestamp. These span-level
// resolved timestamps are emitted as a marshaled `jobspb.ResolvedSpan` proto in
// column 0.
//
// The flow will always have exactly one ChangeFrontier processor which all the
// ChangeAggregators feed into. It collects all span-level resolved timestamps
// and aggregates them into a changefeed-level resolved timestamp, which is the
// minimum of the span-level resolved timestamps. This changefeed-level resolved
// timestamp is emitted into the changefeed sink (or returned to the gateway if
// there is no sink) whenever it advances. ChangeFrontier also updates the
// progress of the changefeed's corresponding system job.
func distChangefeedFlow(
	ctx context.Context,
	phs sql.PlanHookState,
	jobID int64,
	details jobspb.ChangefeedDetails,
	progress jobspb.Progress,
	resultsCh chan<- tree.Datums,
) error {
	var err error
	details, err = validateDetails(details)
	if err != nil {
		return err
	}

	// NB: A non-empty high water indicates that we have checkpointed a resolved
	// timestamp. Skipping the initial scan is equivalent to starting the
	// changefeed from a checkpoint at its start time. Initialize the progress
	// based on whether we should perform an initial scan.
	{
		h := progress.GetHighWater()
		noHighWater := (h == nil || *h == (hlc.Timestamp{}))
		// We want to set the highWater and thus avoid an initial scan if either
		// this is a cursor and there was no request for one, or we don't have a
		// cursor but we have a request to not have an initial scan.
		if noHighWater && !initialScanFromOptions(details.Opts) {
			// If there is a cursor, the statement time has already been set to it.
			progress.Progress = &jobspb.Progress_HighWater{HighWater: &details.StatementTime}
		}
	}

	spansTS := details.StatementTime
	var initialHighWater hlc.Timestamp
	if h := progress.GetHighWater(); h != nil && *h != (hlc.Timestamp{}) {
		initialHighWater = *h
		// If we have a high-water set, use it to compute the spans, since the
		// ones at the statement time may have been garbage collected by now.
		spansTS = initialHighWater
	}

	execCfg := phs.ExecCfg()
	trackedSpans, err := fetchSpansForTargets(ctx, execCfg.DB, execCfg.Codec, details.Targets, spansTS)
	if err != nil {
		return err
	}

	// Changefeed flows handle transactional consistency themselves.
	var noTxn *kv.Txn
	gatewayNodeID, err := execCfg.NodeID.OptionalNodeIDErr(48274)
	if err != nil {
		return err
	}
	dsp := phs.DistSQLPlanner()
	evalCtx := phs.ExtendedEvalContext()
	planCtx := dsp.NewPlanningCtx(ctx, evalCtx, noTxn, true /* distribute */)

	var spanPartitions []sql.SpanPartition
	if details.SinkURI == `` {
		// Sinkless feeds get one ChangeAggregator on the gateway.
		spanPartitions = []sql.SpanPartition{{Node: gatewayNodeID, Spans: trackedSpans}}
	} else {
		// All other feeds get a ChangeAggregator local on the leaseholder.
		spanPartitions, err = dsp.PartitionSpans(planCtx, trackedSpans)
		if err != nil {
			return err
		}
	}

	corePlacement := make([]physicalplan.ProcessorCorePlacement, len(spanPartitions))
	for i, sp := range spanPartitions {
		// TODO(dan): Merge these watches with the span-level resolved
		// timestamps from the job progress.
		watches := make([]execinfrapb.ChangeAggregatorSpec_Watch, len(sp.Spans))
		for watchIdx, nodeSpan := range sp.Spans {
			watches[watchIdx] = execinfrapb.ChangeAggregatorSpec_Watch{
				Span:            nodeSpan,
				InitialResolved: initialHighWater,
			}
		}

		corePlacement[i].NodeID = sp.Node
		corePlacement[i].Core.ChangeAggregator = &execinfrapb.ChangeAggregatorSpec{
			Watches: watches,
			Feed:    details,
			User:    phs.User(),
		}
	}
	// NB: This SpanFrontier processor depends on the set of tracked spans being
	// static. Currently there is no way for them to change after the changefeed
	// is created, even if it is paused and unpaused, but #28982 describes some
	// ways that this might happen in the future.
	changeFrontierSpec := execinfrapb.ChangeFrontierSpec{
		TrackedSpans: trackedSpans,
		Feed:         details,
		JobID:        jobID,
		User:         phs.User(),
	}

	p := sql.MakePhysicalPlan(gatewayNodeID)
	p.AddNoInputStage(corePlacement, execinfrapb.PostProcessSpec{}, []*types.T{}, execinfrapb.Ordering{})
	p.AddSingleGroupStage(
		gatewayNodeID,
		execinfrapb.ProcessorCoreUnion{ChangeFrontier: &changeFrontierSpec},
		execinfrapb.PostProcessSpec{},
		changefeedResultTypes,
	)

	p.PlanToStreamColMap = []int{1, 2, 3}
	dsp.FinalizePlan(planCtx, &p)

	resultRows := makeChangefeedResultWriter(resultsCh)
	recv := sql.MakeDistSQLReceiver(
		ctx,
		resultRows,
		tree.Rows,
		execCfg.RangeDescriptorCache,
		noTxn,
		func(ts hlc.Timestamp) {},
		evalCtx.Tracing,
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
	dsp.Run(planCtx, noTxn, &p, recv, &evalCtxCopy, finishedSetupFn)()
	return resultRows.Err()
}

func fetchSpansForTargets(
	ctx context.Context,
	db *kv.DB,
	codec keys.SQLCodec,
	targets jobspb.ChangefeedTargets,
	ts hlc.Timestamp,
) ([]roachpb.Span, error) {
	var spans []roachpb.Span
	err := db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		spans = nil
		txn.SetFixedTimestamp(ctx, ts)
		// Note that all targets are currently guaranteed to be tables.
		for tableID := range targets {
			tableDesc, err := sqlbase.GetTableDescFromID(ctx, txn, codec, tableID)
			if err != nil {
				return err
			}
			spans = append(spans, tableDesc.PrimaryIndexSpan(codec))
		}
		return nil
	})
	return spans, err
}

// changefeedResultWriter implements the `rowexec.resultWriter` that sends
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
func (w *changefeedResultWriter) IncrementRowsAffected(n int) {
	w.rowsAffected += n
}
func (w *changefeedResultWriter) SetError(err error) {
	w.err = err
}
func (w *changefeedResultWriter) Err() error {
	return w.err
}
