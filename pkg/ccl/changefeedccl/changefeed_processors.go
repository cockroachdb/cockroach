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
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

type changeAggregator struct {
	distsqlrun.ProcessorBase

	flowCtx *distsqlrun.FlowCtx
	spec    distsqlpb.ChangeAggregatorSpec
	memAcc  mon.BoundAccount

	// cancel shuts down the processor, both the `Next()` flow and the poller.
	cancel func()
	// errCh contains the return values of the poller.
	errCh chan error
	// poller runs in the background and puts kv changes and resolved spans into
	// a buffer, which is used by `Next()`.
	poller *poller
	// pollerDoneCh is closed when the poller exits.
	pollerDoneCh chan struct{}

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
	spec distsqlpb.ChangeAggregatorSpec,
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
		&distsqlpb.PostProcessSpec{},
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
	leaseMgr := ca.flowCtx.LeaseManager.(*sql.LeaseManager)
	ca.poller = makePoller(
		ca.flowCtx.Settings, ca.flowCtx.ClientDB, ca.flowCtx.ClientDB.Clock(), ca.flowCtx.Gossip,
		spans, ca.spec.Feed, initialHighWater, buf, leaseMgr, metrics,
	)
	rowsFn := kvsToRows(leaseMgr, ca.spec.Feed, buf.Get)

	var knobs TestingKnobs
	if cfKnobs, ok := ca.flowCtx.TestingKnobs().Changefeed.(*TestingKnobs); ok {
		knobs = *cfKnobs
	}
	ca.tickFn = emitEntries(ca.flowCtx.Settings, ca.spec.Feed, ca.encoder, ca.sink, rowsFn, knobs, metrics)

	// Give errCh enough buffer both possible errors from supporting goroutines,
	// but only the first one is ever used.
	ca.errCh = make(chan error, 2)
	ca.pollerDoneCh = make(chan struct{})
	if err := ca.flowCtx.Stopper().RunAsyncTask(ctx, "changefeed-poller", func(ctx context.Context) {
		defer close(ca.pollerDoneCh)
		var err error
		if storage.RangefeedEnabled.Get(&ca.flowCtx.Settings.SV) {
			err = ca.poller.RunUsingRangefeeds(ctx)
		} else {
			err = ca.poller.Run(ctx)
		}

		// Trying to call MoveToDraining here is racy (`MoveToDraining called in
		// state stateTrailingMeta`), so return the error via a channel.
		ca.errCh <- err
		ca.cancel()
	}); err != nil {
		ca.errCh <- err
		ca.cancel()
	}

	return ctx
}

// close has two purposes: to synchronize on the completion of the helper
// goroutines created by the Start method, and to clean up any resources used by
// the processor. Due to the fact that this method may be called even if the
// processor did not finish completion, there is an excessive amount of nil
// checking.
func (ca *changeAggregator) close() {
	if ca.InternalClose() {
		// Shut down the poller if it wasn't already.
		if ca.cancel != nil {
			ca.cancel()
		}
		// Wait for the poller to finish shutting down.
		if ca.pollerDoneCh != nil {
			<-ca.pollerDoneCh
		}
		if ca.sink != nil {
			if err := ca.sink.Close(); err != nil {
				log.Warningf(ca.Ctx, `error closing sink. goroutines may have leaked: %v`, err)
			}
		}
		ca.memAcc.Close(ca.Ctx)
		ca.MemMonitor.Stop(ca.Ctx)
	}
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
			// If the poller errored first, that's the
			// interesting one, so overwrite `err`.
			case err = <-ca.errCh:
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

const (
	emitAllResolved = 0
	emitNoResolved  = -1
)

type changeFrontier struct {
	distsqlrun.ProcessorBase

	flowCtx *distsqlrun.FlowCtx
	spec    distsqlpb.ChangeFrontierSpec
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
	// freqEmitResolved, if >= 0, is a lower bound on the duration between
	// resolved timestamp emits.
	freqEmitResolved time.Duration
	// lastEmitResolved is the last time a resolved timestamp was emitted.
	lastEmitResolved time.Time
	// lastSlowSpanLog is the last time a slow span from `sf` was logged.
	lastSlowSpanLog time.Time

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
	spec distsqlpb.ChangeFrontierSpec,
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
		cf, &distsqlpb.PostProcessSpec{},
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

	if r, ok := cf.spec.Feed.Opts[optResolvedTimestamps]; ok {
		var err error
		if r == `` {
			// Empty means emit them as often as we have them.
			cf.freqEmitResolved = emitAllResolved
		} else if cf.freqEmitResolved, err = time.ParseDuration(r); err != nil {
			return nil, err
		}
	} else {
		cf.freqEmitResolved = emitNoResolved
	}

	var err error
	if cf.encoder, err = getEncoder(spec.Feed.Opts); err != nil {
		return nil, err
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
	if cf.InternalClose() {
		if cf.sink != nil {
			if err := cf.sink.Close(); err != nil {
				log.Warningf(cf.Ctx, `error closing sink. goroutines may have leaked: %v`, err)
			}
		}
		cf.memAcc.Close(cf.Ctx)
		cf.MemMonitor.Stop(cf.Ctx)
	}
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

	frontierChanged := cf.sf.Forward(resolved.Span, resolved.Timestamp)
	if frontierChanged {
		newResolved := cf.sf.Frontier()
		cf.metrics.mu.Lock()
		if cf.metricsID != -1 {
			cf.metrics.mu.resolved[cf.metricsID] = newResolved
		}
		cf.metrics.mu.Unlock()
		if err := checkpointResolvedTimestamp(cf.Ctx, cf.jobProgressedFn, cf.sf); err != nil {
			return err
		}
		sinceEmitted := newResolved.GoTime().Sub(cf.lastEmitResolved)
		if cf.freqEmitResolved != emitNoResolved && sinceEmitted >= cf.freqEmitResolved {
			// Keeping this after the checkpointResolvedTimestamp call will avoid
			// some duplicates if a restart happens.
			if err := emitResolvedTimestamp(cf.Ctx, cf.encoder, cf.sink, newResolved); err != nil {
				return err
			}
			cf.lastEmitResolved = newResolved.GoTime()
		}
	}

	// Potentially log the most behind span in the frontier for debugging.
	slownessThreshold := 10 * changefeedPollInterval.Get(&cf.flowCtx.Settings.SV)
	frontier := cf.sf.Frontier()
	now := timeutil.Now()
	if resolvedBehind := now.Sub(frontier.GoTime()); resolvedBehind > slownessThreshold {
		if frontierChanged {
			log.Infof(cf.Ctx, "job %d new resolved timestamp %s is behind by %s",
				cf.spec.JobID, frontier, resolvedBehind)
		}
		const slowSpanMaxFrequency = 10 * time.Second
		if now.Sub(cf.lastSlowSpanLog) > slowSpanMaxFrequency {
			cf.lastSlowSpanLog = now
			s := cf.sf.peekFrontierSpan()
			log.Infof(cf.Ctx, "job %d span [%s,%s) is behind by %s",
				cf.spec.JobID, s.Key, s.EndKey, resolvedBehind)
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
