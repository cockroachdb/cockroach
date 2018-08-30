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

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/pkg/errors"
)

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
	// changedRowBuf, if non-nil, contains changed rows to be emitted. Anything
	// queued in `resolvedSpanBuf` is dependent on these having been emitted, so
	// this one must be empty before moving on to that one.
	changedRowBuf *encDatumRowBuffer
	// resolvedSpanBuf contains resolved span updates to send to changeFrontier.
	// If sink is a bufferSink, it must be emptied before these are sent.
	resolvedSpanBuf encDatumRowBuffer
}

var _ distsqlrun.Processor = &changeAggregator{}
var _ distsqlrun.RowSource = &changeAggregator{}

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
	if b, ok := ca.sink.(*bufferSink); ok {
		ca.changedRowBuf = &b.buf
	}
	// The job registry has a set of metrics used to monitor the various jobs it
	// runs. They're all stored as the `metric.Struct` interface because of
	// dependency cycles.
	metrics := flowCtx.JobRegistry.MetricsStruct().Changefeed.(*Metrics)
	ca.sink = makeMetricsSink(metrics, ca.sink)

	buf := makeBuffer()
	ca.poller = makePoller(
		flowCtx.Settings, flowCtx.ClientDB, flowCtx.ClientDB.Clock(), flowCtx.Gossip, spans,
		spec.Feed, initialHighWater, buf)
	rowsFn := kvsToRows(flowCtx.LeaseManager.(*sql.LeaseManager), spec.Feed, buf.Get)
	ca.tickFn = emitEntries(spec.Feed, ca.sink, rowsFn)

	return ca, nil
}

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
	// Wait for the poller to finish shutting down. If the poller errored first,
	// then Next will have passed its error to MoveToDraining. Otherwise, the
	// error will be related to the forced shutdown of the poller (probably
	// context canceled) and we don't care what it is, so throw it away.
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
		if !ca.changedRowBuf.IsEmpty() {
			return ca.changedRowBuf.Pop(), nil
		} else if !ca.resolvedSpanBuf.IsEmpty() {
			return ca.resolvedSpanBuf.Pop(), nil
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
		// Enqueue a row to be returned that indicates some span-level resolved
		// timestamp has advanced. If any rows were queued in `sink`, they must
		// be emitted first.
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
	// resolvedBuf, if non-nil, contains rows indicating a changefeed-level
	// resolved timestamp to be returned. It depends on everything in
	// `passthroughBuf` being sent, so that one needs to be emptied first.
	resolvedBuf *encDatumRowBuffer
	// metrics are monitoring counters shared between all changefeeds.
	metrics *Metrics
	// metricsID is used as the unique id of this changefeed in the
	// metrics.MinHighWater map.
	metricsID int
}

var _ distsqlrun.Processor = &changeFrontier{}
var _ distsqlrun.RowSource = &changeFrontier{}

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
	if b, ok := cf.sink.(*bufferSink); ok {
		cf.resolvedBuf = &b.buf
	}
	// The job registry has a set of metrics used to monitor the various jobs it
	// runs. They're all stored as the `metric.Struct` interface because of
	// dependency cycles.
	cf.metrics = flowCtx.JobRegistry.MetricsStruct().Changefeed.(*Metrics)
	cf.sink = makeMetricsSink(cf.metrics, cf.sink)

	if spec.JobID != 0 {
		job, err := flowCtx.JobRegistry.LoadJob(ctx, spec.JobID)
		if err != nil {
			return nil, err
		}
		cf.jobProgressedFn = job.HighWaterProgressed
	}

	return cf, nil
}

func (cf *changeFrontier) OutputTypes() []sqlbase.ColumnType {
	return changefeedResultTypes
}

// Start is part of the RowSource interface.
func (cf *changeFrontier) Start(ctx context.Context) context.Context {
	cf.input.Start(ctx)
	ctx = cf.StartInternal(ctx, changeFrontierProcName)

	cf.metrics.mu.Lock()
	cf.metricsID = cf.metrics.mu.id
	cf.metrics.mu.id++
	cf.metrics.mu.Unlock()
	go func() {
		// Delete this feed from the MinHighwater metric so it's no longer
		// considered by the gauge.
		//
		// TODO(dan): Ideally this would be done in something like `close` but
		// there's nothing that's guaranteed to be called when a processor shuts
		// down.
		<-ctx.Done()
		cf.metrics.mu.Lock()
		delete(cf.metrics.mu.resolved, cf.metricsID)
		cf.metricsID = -1
		cf.metrics.mu.Unlock()
	}()

	return ctx
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
		} else if !cf.resolvedBuf.IsEmpty() {
			return cf.resolvedBuf.Pop(), nil
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
			// to be forwarded to the gateway.
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
		cf.metrics.mu.Lock()
		if cf.metricsID != -1 {
			cf.metrics.mu.resolved[cf.metricsID] = cf.sf.Frontier()
		}
		cf.metrics.mu.Unlock()
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
