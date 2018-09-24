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
	// errCh contains the return values of poller and tableHistUpdater.
	errCh chan error
	// poller runs in the background and puts kv changes and resolved spans into
	// a buffer, which is used by `Next()`.
	poller *poller
	// pollerDoneCh is closed when the poller exits.
	pollerDoneCh chan struct{}
	// tableHistUpdater runs in the background and continually advances the
	// high-water of a tableHistory.
	tableHistUpdater *tableHistoryUpdater
	// tableHistUpdaterDoneCh is closed when the tableHistUpdater exits.
	tableHistUpdaterDoneCh chan struct{}

	// encoder is the Encoder to use for key and value serialization.
	encoder Encoder
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
			TrailingMetaCallback: func(context.Context) []distsqlrun.ProducerMetadata {
				ca.close()
				return nil
			},
		},
	); err != nil {
		return nil, err
	}

	var err error
	if ca.encoder, err = getEncoder(ca.spec.Feed.Opts); err != nil {
		return nil, err
	}

	// Due to the possibility of leaked goroutines, it is not safe to start a sink
	// in this method because there is no guarantee that the TrailingMetaCallback
	// method will ever be called (this can happen, for example, if an error
	// occurs during flow setup).  However, we still want to ensure that the user
	// has not made any obvious errors when specifying the sink in the CREATE
	// CHANGEFEED statement. Therefore, we create a "canary" sink, which will be
	// immediately closed, only to check for errors.
	{
		canarySink, err := getSink(spec.Feed.SinkURI, spec.Feed.Opts, spec.Feed.Targets)
		if err != nil {
			return nil, err
		}
		if err := canarySink.Close(); err != nil {
			return nil, err
		}
	}

	return ca, nil
}

func (ca *changeAggregator) OutputTypes() []sqlbase.ColumnType {
	return changefeedResultTypes
}

// Start is part of the RowSource interface.
func (ca *changeAggregator) Start(ctx context.Context) context.Context {
	ctx, ca.cancel = context.WithCancel(ctx)
	// StartInternal called at the beginning of the function because there are
	// early returns if errors are detected.
	ctx = ca.StartInternal(ctx, changeAggregatorProcName)

	var err error
	if ca.sink, err = getSink(
		ca.spec.Feed.SinkURI, ca.spec.Feed.Opts, ca.spec.Feed.Targets,
	); err != nil {
		// Early abort in the case that there is an error creating the sink.
		ca.MoveToDraining(err)
		ca.cancel()
		return ctx
	}

	// This is the correct point to set up certain hooks depending on the sink
	// type.
	if b, ok := ca.sink.(*bufferSink); ok {
		ca.changedRowBuf = &b.buf
	}

	initialHighWater := hlc.Timestamp{WallTime: -1}
	var spans []roachpb.Span
	for _, watch := range ca.spec.Watches {
		spans = append(spans, watch.Span)
		if initialHighWater.WallTime == -1 || watch.InitialResolved.Less(initialHighWater) {
			initialHighWater = watch.InitialResolved
		}
	}

	// The job registry has a set of metrics used to monitor the various jobs it
	// runs. They're all stored as the `metric.Struct` interface because of
	// dependency cycles.
	metrics := ca.flowCtx.JobRegistry.MetricsStruct().Changefeed.(*Metrics)
	ca.sink = makeMetricsSink(metrics, ca.sink)

	buf := makeBuffer()
	ca.poller = makePoller(
		ca.flowCtx.Settings, ca.flowCtx.ClientDB, ca.flowCtx.ClientDB.Clock(), ca.flowCtx.Gossip,
		spans, ca.spec.Feed, initialHighWater, buf,
	)

	leaseMgr := ca.flowCtx.LeaseManager.(*sql.LeaseManager)
	tableHist := makeTableHistory(func(desc *sqlbase.TableDescriptor) error {
		// NB: Each new `tableDesc.Version` is initially written with an mvcc
		// timestamp equal to its `ModificationTime`. It might later update that
		// `Version` with backfill progress, but we only validate a table
		// descriptor through its `ModificationTime` before using it, so this
		// validation function can't depend on anything that changes after a new
		// `Version` of a table desc is written.
		return validateChangefeedTable(ca.spec.Feed.Targets, desc)
	}, initialHighWater)
	ca.tableHistUpdater = &tableHistoryUpdater{
		settings: ca.flowCtx.Settings,
		db:       ca.flowCtx.ClientDB,
		targets:  ca.spec.Feed.Targets,
		m:        tableHist,
	}
	rowsFn := kvsToRows(leaseMgr, tableHist, ca.spec.Feed, buf.Get)

	var knobs TestingKnobs
	if cfKnobs, ok := ca.flowCtx.TestingKnobs().Changefeed.(*TestingKnobs); ok {
		knobs = *cfKnobs
	}
	ca.tickFn = emitEntries(ca.spec.Feed, ca.encoder, ca.sink, rowsFn, knobs)

	// Give errCh enough buffer both possible errors from supporting goroutines,
	// but only the first one is ever used.
	ca.errCh = make(chan error, 2)
	ca.pollerDoneCh = make(chan struct{})
	go func(ctx context.Context) {
		defer close(ca.pollerDoneCh)
		err := ca.poller.Run(ctx)
		// Trying to call MoveToDraining here is racy (`MoveToDraining called in
		// state stateTrailingMeta`), so return the error via a channel.
		ca.errCh <- err
		ca.cancel()
	}(ctx)
	ca.tableHistUpdaterDoneCh = make(chan struct{})
	go func(ctx context.Context) {
		defer close(ca.tableHistUpdaterDoneCh)
		err := ca.tableHistUpdater.PollTableDescs(ctx)
		// Trying to call MoveToDraining here is racy (`MoveToDraining called in
		// state stateTrailingMeta`), so return the error via a channel.
		ca.errCh <- err
		ca.cancel()
	}(ctx)

	return ctx
}

// close has two purposes: to synchronize on the completion of the helper
// goroutines created by the Start method, and to clean up any resources used by
// the processor. Due to the fact that this method may be called even if the
// processor did not finish completion, there is an excessive amount of nil
// checking.
func (ca *changeAggregator) close() {
	// Shut down the poller and tableHistUpdater if they weren't already.
	if ca.cancel != nil {
		ca.cancel()
	}
	// Wait for the poller and tableHistUpdater to finish shutting down.
	if ca.pollerDoneCh != nil {
		<-ca.pollerDoneCh
	}
	if ca.tableHistUpdaterDoneCh != nil {
		<-ca.tableHistUpdaterDoneCh
	}
	if ca.sink != nil {
		if err := ca.sink.Close(); err != nil {
			log.Warningf(ca.Ctx, `error closing sink. goroutines may have leaked: %v`, err)
		}
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
			// If the poller or tableHistUpdater errored first, that's the
			// interesting one, so overwrite `err`.
			case err = <-ca.errCh:
			default:
			}
			// Shut down the poller and tableHistUpdater if they weren't
			// already.
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
	// encoder is the Encoder to use for resolved timestamp serialization.
	encoder Encoder
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
			TrailingMetaCallback: func(context.Context) []distsqlrun.ProducerMetadata {
				cf.close()
				return nil
			},
			InputsToDrain: []distsqlrun.RowSource{cf.input},
		},
	); err != nil {
		return nil, err
	}

	var err error
	if cf.encoder, err = getEncoder(spec.Feed.Opts); err != nil {
		return nil, err
	}

	// See comment in newChangeAggregatorProcessor for details on the use of canary
	// sinks.
	{
		canarySink, err := getSink(spec.Feed.SinkURI, spec.Feed.Opts, spec.Feed.Targets)
		if err != nil {
			return nil, err
		}
		if err := canarySink.Close(); err != nil {
			return nil, err
		}
	}

	return cf, nil
}

func (cf *changeFrontier) OutputTypes() []sqlbase.ColumnType {
	return changefeedResultTypes
}

// Start is part of the RowSource interface.
func (cf *changeFrontier) Start(ctx context.Context) context.Context {
	cf.input.Start(ctx)

	// StartInternal called at the beginning of the function because there are
	// early returns if errors are detected.
	ctx = cf.StartInternal(ctx, changeFrontierProcName)

	var err error
	if cf.sink, err = getSink(
		cf.spec.Feed.SinkURI, cf.spec.Feed.Opts, cf.spec.Feed.Targets,
	); err != nil {
		cf.MoveToDraining(err)
		return ctx
	}

	if b, ok := cf.sink.(*bufferSink); ok {
		cf.resolvedBuf = &b.buf
	}

	// The job registry has a set of metrics used to monitor the various jobs it
	// runs. They're all stored as the `metric.Struct` interface because of
	// dependency cycles.
	cf.metrics = cf.flowCtx.JobRegistry.MetricsStruct().Changefeed.(*Metrics)
	cf.sink = makeMetricsSink(cf.metrics, cf.sink)

	if cf.spec.JobID != 0 {
		job, err := cf.flowCtx.JobRegistry.LoadJob(ctx, cf.spec.JobID)
		if err != nil {
			cf.MoveToDraining(err)
			return ctx
		}
		cf.jobProgressedFn = job.HighWaterProgressed
	}

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
	if cf.sink != nil {
		if err := cf.sink.Close(); err != nil {
			log.Warningf(cf.Ctx, `error closing sink. goroutines may have leaked: %v`, err)
		}
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
			cf.Ctx, cf.spec.Feed, cf.encoder, cf.sink, cf.jobProgressedFn, cf.sf,
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
