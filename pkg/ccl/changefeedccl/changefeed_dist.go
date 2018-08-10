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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlplan"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/pkg/errors"
)

func init() {
	distsqlrun.NewChangeAggregatorProcessor = newChangeAggregatorProcessor
	distsqlrun.NewChangeFrontierProcessor = newChangeFrontierProcessor
}

const (
	changeAggregatorProcName = `changeagg`
	changeFrontierProcName   = `changefntr`
)

var changefeedResultTypes = []sqlbase.ColumnType{
	{SemanticType: sqlbase.ColumnType_BYTES},  // resolved span
	{SemanticType: sqlbase.ColumnType_STRING}, // topic
	{SemanticType: sqlbase.ColumnType_BYTES},  // key
	{SemanticType: sqlbase.ColumnType_BYTES},  // value
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
//
// TODO(dan): The logic here is the planning for the non-enterprise version of
// changefeeds, which is one ChangeAggregator processor feeding into one
// ChangeFrontier processor with both on the gateway node. Also implement the
// planning logic for the enterprise version, which places ChangeAggregator
// processors on the leaseholder for the spans they're watching.
func distChangefeedFlow(
	ctx context.Context,
	phs sql.PlanHookState,
	details jobspb.ChangefeedDetails,
	progress jobspb.Progress,
	resultsCh chan<- tree.Datums,
	progressedFn func(context.Context, jobs.HighWaterProgressedFn) error,
) error {
	var err error
	details, err = validateDetails(details)
	if err != nil {
		return err
	}

	var highWater hlc.Timestamp
	if h := progress.GetHighWater(); h != nil {
		highWater = *h
	}

	execCfg := phs.ExecCfg()
	trackedSpans, err := fetchSpansForTargets(ctx, execCfg.DB, details.Targets, highWater)
	if err != nil {
		return err
	}

	// TODO(dan): Merge these with the span-level resolved timestamps from the
	// job progress.
	var watches []distsqlrun.ChangeAggregatorSpec_Watch
	for _, span := range trackedSpans {
		watches = append(watches, distsqlrun.ChangeAggregatorSpec_Watch{
			Span:            span,
			InitialResolved: highWater,
		})
	}

	gatewayNodeID := execCfg.NodeID.Get()
	changeAggregatorProcs := []distsqlplan.Processor{{
		Node: gatewayNodeID,
		Spec: distsqlrun.ProcessorSpec{
			Core: distsqlrun.ProcessorCoreUnion{
				ChangeAggregator: &distsqlrun.ChangeAggregatorSpec{
					Watches: watches,
					Feed:    details,
				},
			},
			Output: []distsqlrun.OutputRouterSpec{{Type: distsqlrun.OutputRouterSpec_PASS_THROUGH}},
		},
	}}
	changeFrontierSpec := distsqlrun.ChangeFrontierSpec{
		TrackedSpans: trackedSpans,
		Feed:         details,
	}

	var p sql.PhysicalPlan

	stageID := p.NewStageID()
	p.ResultRouters = make([]distsqlplan.ProcessorIdx, len(changeAggregatorProcs))
	for i, proc := range changeAggregatorProcs {
		proc.Spec.StageID = stageID
		pIdx := p.AddProcessor(proc)
		p.ResultRouters[i] = pIdx
	}

	p.AddSingleGroupStage(
		gatewayNodeID,
		distsqlrun.ProcessorCoreUnion{ChangeFrontier: &changeFrontierSpec},
		distsqlrun.PostProcessSpec{},
		changefeedResultTypes,
	)

	p.ResultTypes = changefeedResultTypes
	p.PlanToStreamColMap = []int{1, 2, 3}

	// Changefeed flows handle transactional consistency themselves.
	var noTxn *client.Txn

	dsp := phs.DistSQLPlanner()
	evalCtx := phs.ExtendedEvalContext()
	planCtx := dsp.NewPlanningCtx(ctx, evalCtx, noTxn)
	dsp.FinalizePlan(&planCtx, &p)

	resultRows := makeChangefeedResultWriter(resultsCh)
	recv := sql.MakeDistSQLReceiver(
		ctx,
		resultRows,
		tree.Rows,
		execCfg.RangeDescriptorCache,
		execCfg.LeaseHolderCache,
		noTxn,
		func(ts hlc.Timestamp) {},
		evalCtx.Tracing,
	)

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

	dsp.Run(&planCtx, noTxn, &p, recv, evalCtx, finishedSetupFn)
	return resultRows.Err()
}

type changeAggregator struct {
	distsqlrun.ProcessorBase

	flowCtx *distsqlrun.FlowCtx
	spec    distsqlrun.ChangeAggregatorSpec
	memAcc  mon.BoundAccount

	// cancel shuts down the processor, both the `Next()` flow and the poller.
	cancel func()
	// poller runs in the background and puts kv changes and resolved spans into
	// a buffer, which is used by `Next()`.
	poller *poller
	// pollerErrCh is written once with the poller error (or nil).
	pollerErrCh chan error

	// sink is the Sink to write rows to. Resolved timestamps are never written
	// by changeAggregator.
	sink Sink
	// tickFn is the workhorse behind Next(). It pulls kv changes from the
	// buffer that poller fills, handles table leasing, converts them to rows,
	// and writes them to the sink.
	tickFn func(context.Context) ([]jobspb.ResolvedSpan, error)
	// resolvedSpanBuf contains resolved span updates to send to changeFrontier.
	// If sink is a bufferSink, it must be emptied before these are sent.
	resolvedSpanBuf encDatumRowBuffer
}

func newChangeAggregatorProcessor(
	flowCtx *distsqlrun.FlowCtx,
	processorID int32,
	spec distsqlrun.ChangeAggregatorSpec,
	output distsqlrun.RowReceiver,
) (distsqlrun.Processor, error) {
	ctx := flowCtx.EvalCtx.Ctx()
	memMonitor := distsqlrun.NewMonitor(ctx, flowCtx.EvalCtx.Mon, "changeagg-mem")
	ca := &changeAggregator{
		flowCtx: flowCtx,
		spec:    spec,
		memAcc:  memMonitor.MakeBoundAccount(),
	}
	if err := ca.Init(
		ca,
		&distsqlrun.PostProcessSpec{},
		nil, /* types */
		flowCtx,
		processorID,
		output,
		memMonitor,
		distsqlrun.ProcStateOpts{
			TrailingMetaCallback: func() []distsqlrun.ProducerMetadata {
				ca.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}

	initialHighWater := hlc.Timestamp{WallTime: -1}
	var spans []roachpb.Span
	for _, watch := range spec.Watches {
		spans = append(spans, watch.Span)
		if initialHighWater.WallTime == -1 || watch.InitialResolved.Less(initialHighWater) {
			initialHighWater = watch.InitialResolved
		}
	}

	var err error
	if ca.sink, err = getSink(spec.Feed.SinkURI, spec.Feed.Targets); err != nil {
		return nil, err
	}
	buf := makeBuffer()
	ca.poller = makePoller(
		flowCtx.Settings, flowCtx.ClientDB, flowCtx.ClientDB.Clock(), flowCtx.Gossip, spans,
		spec.Feed.Targets, initialHighWater, buf)
	rowsFn := kvsToRows(flowCtx.LeaseManager.(*sql.LeaseManager), spec.Feed, buf.Get)
	ca.tickFn = emitEntries(spec.Feed, ca.sink, rowsFn)

	return ca, nil
}

var _ distsqlrun.Processor = &changeAggregator{}
var _ distsqlrun.RowSource = &changeAggregator{}

func (ca *changeAggregator) OutputTypes() []sqlbase.ColumnType {
	return changefeedResultTypes
}

// Start is part of the RowSource interface.
func (ca *changeAggregator) Start(ctx context.Context) context.Context {
	ctx, ca.cancel = context.WithCancel(ctx)

	ca.pollerErrCh = make(chan error, 1)
	go func(ctx context.Context) {
		err := ca.poller.Run(ctx)
		// Trying to call MoveToDraining here is racy (`MoveToDraining called in
		// state stateTrailingMeta`), so return the error via a channel.
		ca.pollerErrCh <- err
		close(ca.pollerErrCh)
		ca.cancel()
	}(ctx)

	ctx = ca.StartInternal(ctx, changeAggregatorProcName)
	return ctx
}

func (ca *changeAggregator) close() {
	// Wait for the poller to finish shutting down.
	<-ca.pollerErrCh
	if err := ca.sink.Close(); err != nil {
		log.Warningf(ca.Ctx, `error closing sink. goroutines may have leaked: %v`, err)
	}
	// Need to close the mem accounting while the context is still valid.
	ca.memAcc.Close(ca.Ctx)
	ca.InternalClose()
	ca.MemMonitor.Stop(ca.Ctx)
}

// Next is part of the RowSource interface.
func (ca *changeAggregator) Next() (sqlbase.EncDatumRow, *distsqlrun.ProducerMetadata) {
	for ca.State == distsqlrun.StateRunning {
		if s, ok := ca.sink.(*bufferSink); ok && !s.buf.IsEmpty() {
			ret := s.buf.Pop()
			return ret, nil
		} else if !ca.resolvedSpanBuf.IsEmpty() {
			ret := ca.resolvedSpanBuf.Pop()
			return ret, nil
		}

		if err := ca.tick(); err != nil {
			select {
			// If the poller errored first, that's the interesting one, so
			// overwrite `err`.
			case err = <-ca.pollerErrCh:
			default:
			}
			// Shut down the poller if it wasn't already.
			ca.cancel()

			ca.MoveToDraining(err)
			break
		}
	}
	return nil, ca.DrainHelper()
}

func (ca *changeAggregator) tick() error {
	resolvedSpans, err := ca.tickFn(ca.Ctx)
	if err != nil {
		return err
	}

	for _, resolvedSpan := range resolvedSpans {
		resolvedBytes, err := protoutil.Marshal(&resolvedSpan)
		if err != nil {
			return err
		}
		ca.resolvedSpanBuf.Push(sqlbase.EncDatumRow{
			sqlbase.EncDatum{Datum: tree.NewDBytes(tree.DBytes(resolvedBytes))},
			sqlbase.EncDatum{Datum: tree.DNull}, // topic
			sqlbase.EncDatum{Datum: tree.DNull}, // key
			sqlbase.EncDatum{Datum: tree.DNull}, // value
		})
	}
	return nil
}

// ConsumerDone is part of the RowSource interface.
func (ca *changeAggregator) ConsumerDone() {
	ca.MoveToDraining(nil /* err */)
}

// ConsumerClosed is part of the RowSource interface.
func (ca *changeAggregator) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	ca.InternalClose()
}

type changeFrontier struct {
	distsqlrun.ProcessorBase

	flowCtx *distsqlrun.FlowCtx
	spec    distsqlrun.ChangeFrontierSpec
	memAcc  mon.BoundAccount
	a       sqlbase.DatumAlloc

	// input returns rows from one or more changeAggregator processors
	input distsqlrun.RowSource

	// sf contains the current resolved timestamp high-water for the tracked
	// span set.
	sf *spanFrontier
	// sink is the Sink to write resolved timestamps to. Rows are never written
	// by changeFrontier.
	sink Sink
	// jobProgressedFn, if non-nil, is called to checkpoint the changefeed's
	// progress in the corresponding system job entry.
	jobProgressedFn func(context.Context, jobs.HighWaterProgressedFn) error
	// passthroughBuf, in some but not all flows, contains changed row data to
	// pass through unchanged to the gateway node.
	passthroughBuf encDatumRowBuffer
}

func newChangeFrontierProcessor(
	flowCtx *distsqlrun.FlowCtx,
	processorID int32,
	spec distsqlrun.ChangeFrontierSpec,
	input distsqlrun.RowSource,
	output distsqlrun.RowReceiver,
) (distsqlrun.Processor, error) {
	ctx := flowCtx.EvalCtx.Ctx()
	memMonitor := distsqlrun.NewMonitor(ctx, flowCtx.EvalCtx.Mon, "changefntr-mem")
	cf := &changeFrontier{
		flowCtx: flowCtx,
		spec:    spec,
		memAcc:  memMonitor.MakeBoundAccount(),
		input:   input,
		sf:      makeSpanFrontier(spec.TrackedSpans...),
	}
	if err := cf.Init(
		cf, &distsqlrun.PostProcessSpec{},
		input.OutputTypes(),
		flowCtx,
		processorID,
		output,
		memMonitor,
		distsqlrun.ProcStateOpts{
			TrailingMetaCallback: func() []distsqlrun.ProducerMetadata {
				cf.close()
				return nil
			},
			InputsToDrain: []distsqlrun.RowSource{cf.input},
		},
	); err != nil {
		return nil, err
	}

	var err error
	if cf.sink, err = getSink(spec.Feed.SinkURI, spec.Feed.Targets); err != nil {
		return nil, err
	}

	if spec.JobID != 0 {
		job, err := flowCtx.JobRegistry.LoadJob(ctx, spec.JobID)
		if err != nil {
			return nil, err
		}
		cf.jobProgressedFn = job.HighWaterProgressed
	}

	return cf, nil
}

var _ distsqlrun.Processor = &changeAggregator{}
var _ distsqlrun.RowSource = &changeAggregator{}

func (cf *changeFrontier) OutputTypes() []sqlbase.ColumnType {
	return changefeedResultTypes
}

// Start is part of the RowSource interface.
func (cf *changeFrontier) Start(ctx context.Context) context.Context {
	cf.input.Start(ctx)
	return cf.StartInternal(ctx, changeFrontierProcName)
}

func (cf *changeFrontier) close() {
	if err := cf.sink.Close(); err != nil {
		log.Warningf(cf.Ctx, `error closing sink. goroutines may have leaked: %v`, err)
	}
	// Need to close the mem accounting while the context is still valid.
	cf.memAcc.Close(cf.Ctx)
	cf.InternalClose()
	cf.MemMonitor.Stop(cf.Ctx)
}

// Next is part of the RowSource interface.
func (cf *changeFrontier) Next() (sqlbase.EncDatumRow, *distsqlrun.ProducerMetadata) {
	for cf.State == distsqlrun.StateRunning {
		if !cf.passthroughBuf.IsEmpty() {
			return cf.passthroughBuf.Pop(), nil
		} else if s, ok := cf.sink.(*bufferSink); ok && !s.buf.IsEmpty() {
			ret := s.buf.Pop()
			return ret, nil
		}

		row, meta := cf.input.Next()
		if meta != nil {
			if meta.Err != nil {
				cf.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			cf.MoveToDraining(nil /* err */)
			break
		}

		if row[0].IsNull() {
			// In changefeeds with a sink, this will never happen. But in the
			// core changefeed, which returns changed rows directly via pgwire,
			// a row with a null resolved_span field is a changed row that needs
			// to be forwared to the gateway.
			cf.passthroughBuf.Push(row)
			continue
		}

		if err := cf.noteResolvedSpan(row[0]); err != nil {
			cf.MoveToDraining(err)
			break
		}
	}
	return nil, cf.DrainHelper()
}

func (cf *changeFrontier) noteResolvedSpan(d sqlbase.EncDatum) error {
	if err := d.EnsureDecoded(&changefeedResultTypes[0], &cf.a); err != nil {
		return err
	}
	raw, ok := d.Datum.(*tree.DBytes)
	if !ok {
		return errors.Errorf(`unexpected datum type %T: %s`, d.Datum, d.Datum)
	}
	var resolved jobspb.ResolvedSpan
	if err := protoutil.Unmarshal([]byte(*raw), &resolved); err != nil {
		return errors.Wrapf(err, `unmarshalling resolved span: %x`, raw)
	}
	if cf.sf.Forward(resolved.Span, resolved.Timestamp) {
		if err := emitResolvedTimestamp(
			cf.Ctx, cf.spec.Feed, cf.sink, cf.jobProgressedFn, cf.sf,
		); err != nil {
			return err
		}
	}
	return nil
}

// ConsumerDone is part of the RowSource interface.
func (cf *changeFrontier) ConsumerDone() {
	cf.MoveToDraining(nil /* err */)
}

// ConsumerClosed is part of the RowSource interface.
func (cf *changeFrontier) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	cf.InternalClose()
}

// changefeedResultWriter implements the `distsqlrun.resultWriter` that sends
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
