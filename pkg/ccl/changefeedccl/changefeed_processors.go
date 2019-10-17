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
	"fmt"
	"math"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage/closedts"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type changeAggregator struct {
	execinfra.ProcessorBase

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.ChangeAggregatorSpec
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
	pollerMemMon *mon.BytesMonitor

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

type timestampLowerBoundOracle interface {
	inclusiveLowerBoundTS() hlc.Timestamp
}

type changeAggregatorLowerBoundOracle struct {
	sf                         *spanFrontier
	initialInclusiveLowerBound hlc.Timestamp
}

// inclusiveLowerBoundTs is used to generate a representative timestamp to name
// cloudStorageSink files. This timestamp is either the statement time (in case this
// changefeed job hasn't yet seen any resolved timestamps) or the successor timestamp to
// the local span frontier. This convention is chosen to preserve CDC's ordering
// guarantees. See comment on cloudStorageSink for more details.
func (o *changeAggregatorLowerBoundOracle) inclusiveLowerBoundTS() hlc.Timestamp {
	if frontier := o.sf.Frontier(); !frontier.IsEmpty() {
		// We call `Next()` here on the frontier because this allows us
		// to name files using a timestamp that is an inclusive lower bound
		// on the timestamps of the updates contained within the file.
		// Files being created at the point this method is called are guaranteed
		// to contain row updates with timestamps strictly greater than the local
		// span frontier timestamp.
		return frontier.Next()
	}
	// This should only be returned in the case where the changefeed job hasn't yet
	// seen a resolved timestamp.
	return o.initialInclusiveLowerBound
}

var _ execinfra.Processor = &changeAggregator{}
var _ execinfra.RowSource = &changeAggregator{}

func newChangeAggregatorProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.ChangeAggregatorSpec,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	ctx := flowCtx.EvalCtx.Ctx()
	memMonitor := execinfra.NewMonitor(ctx, flowCtx.EvalCtx.Mon, "changeagg-mem")
	ca := &changeAggregator{
		flowCtx: flowCtx,
		spec:    spec,
		memAcc:  memMonitor.MakeBoundAccount(),
	}
	if err := ca.Init(
		ca,
		&execinfrapb.PostProcessSpec{},
		nil, /* types */
		flowCtx,
		processorID,
		output,
		memMonitor,
		execinfra.ProcStateOpts{
			TrailingMetaCallback: func(context.Context) []execinfrapb.ProducerMetadata {
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

func (ca *changeAggregator) OutputTypes() []types.T {
	return changefeedResultTypes
}

// Start is part of the RowSource interface.
func (ca *changeAggregator) Start(ctx context.Context) context.Context {
	ctx, ca.cancel = context.WithCancel(ctx)
	// StartInternal called at the beginning of the function because there are
	// early returns if errors are detected.
	ctx = ca.StartInternal(ctx, changeAggregatorProcName)

	var initialHighWater hlc.Timestamp
	spans := make([]roachpb.Span, 0, len(ca.spec.Watches))
	for _, watch := range ca.spec.Watches {
		spans = append(spans, watch.Span)
		if initialHighWater.IsEmpty() || watch.InitialResolved.Less(initialHighWater) {
			initialHighWater = watch.InitialResolved
		}
	}

	// This SpanFrontier only tracks the spans being watched on this node.
	// There is a different SpanFrontier elsewhere for the entire changefeed.
	// This object is used to filter out some previously emitted rows, and
	// by the cloudStorageSink to name its output files in lexicographically
	// monotonic fashion.
	sf := makeSpanFrontier(spans...)
	for _, watch := range ca.spec.Watches {
		sf.Forward(watch.Span, watch.InitialResolved)
	}
	timestampOracle := &changeAggregatorLowerBoundOracle{sf: sf, initialInclusiveLowerBound: ca.spec.Feed.StatementTime}
	nodeID := ca.flowCtx.EvalCtx.NodeID
	var err error
	if ca.sink, err = getSink(
		ca.spec.Feed.SinkURI, nodeID, ca.spec.Feed.Opts, ca.spec.Feed.Targets,
		ca.flowCtx.Cfg.Settings, timestampOracle, ca.flowCtx.Cfg.ExternalStorageFromURI,
	); err != nil {
		err = MarkRetryableError(err)
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

	// The job registry has a set of metrics used to monitor the various jobs it
	// runs. They're all stored as the `metric.Struct` interface because of
	// dependency cycles.
	metrics := ca.flowCtx.Cfg.JobRegistry.MetricsStruct().Changefeed.(*Metrics)
	ca.sink = makeMetricsSink(metrics, ca.sink)
	ca.sink = &errorWrapperSink{wrapped: ca.sink}

	var knobs TestingKnobs
	if cfKnobs, ok := ca.flowCtx.TestingKnobs().Changefeed.(*TestingKnobs); ok {
		knobs = *cfKnobs
	}

	// It seems like we should also be able to use `ca.ProcessorBase.MemMonitor`
	// for the poller, but there is a race between the flow's MemoryMonitor
	// getting Stopped and `changeAggregator.Close`, which causes panics. Not sure
	// what to do about this yet.
	pollerMemMonCapacity := memBufferDefaultCapacity
	if knobs.MemBufferCapacity != 0 {
		pollerMemMonCapacity = knobs.MemBufferCapacity
	}
	pollerMemMon := mon.MakeMonitorInheritWithLimit("poller", math.MaxInt64, ca.ProcessorBase.MemMonitor)
	pollerMemMon.Start(ctx, nil /* pool */, mon.MakeStandaloneBudget(pollerMemMonCapacity))
	ca.pollerMemMon = &pollerMemMon

	buf := makeBuffer()
	leaseMgr := ca.flowCtx.Cfg.LeaseManager.(*sql.LeaseManager)
	ca.poller = makePoller(
		ca.flowCtx.Cfg.Settings, ca.flowCtx.Cfg.DB, ca.flowCtx.Cfg.DB.Clock(), ca.flowCtx.Cfg.Gossip,
		spans, ca.spec.Feed, initialHighWater, buf, leaseMgr, metrics, ca.pollerMemMon,
	)
	rowsFn := kvsToRows(leaseMgr, ca.spec.Feed, buf.Get)

	ca.tickFn = emitEntries(
		ca.flowCtx.Cfg.Settings, ca.spec.Feed, sf, ca.encoder, ca.sink, rowsFn, knobs, metrics)

	// Give errCh enough buffer both possible errors from supporting goroutines,
	// but only the first one is ever used.
	ca.errCh = make(chan error, 2)

	ca.pollerDoneCh = make(chan struct{})
	if err := ca.flowCtx.Stopper().RunAsyncTask(ctx, "changefeed-poller", func(ctx context.Context) {
		defer close(ca.pollerDoneCh)
		err := ca.poller.RunUsingRangefeeds(ctx)

		// Trying to call MoveToDraining here is racy (`MoveToDraining called in
		// state stateTrailingMeta`), so return the error via a channel.
		ca.errCh <- err
		ca.cancel()
	}); err != nil {
		// If err != nil then the RunAsyncTask closure never ran, which means we
		// need to manually close ca.pollerDoneCh so `(*changeAggregator).close`
		// doesn't hang.
		close(ca.pollerDoneCh)
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
		if ca.pollerMemMon != nil {
			ca.pollerMemMon.Stop(ca.Ctx)
		}
		ca.MemMonitor.Stop(ca.Ctx)
	}
}

// Next is part of the RowSource interface.
func (ca *changeAggregator) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for ca.State == execinfra.StateRunning {
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
	execinfra.ProcessorBase

	flowCtx *execinfra.FlowCtx
	spec    execinfrapb.ChangeFrontierSpec
	memAcc  mon.BoundAccount
	a       sqlbase.DatumAlloc

	// input returns rows from one or more changeAggregator processors
	input execinfra.RowSource

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
	// highWaterAtStart is the greater of the job high-water and the timestamp the
	// CHANGEFEED statement was run at. It's used in an assertion that we never
	// regress the job high-water.
	highWaterAtStart hlc.Timestamp
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
	// metrics.MaxBehindNanos map.
	metricsID int
}

var _ execinfra.Processor = &changeFrontier{}
var _ execinfra.RowSource = &changeFrontier{}

func newChangeFrontierProcessor(
	flowCtx *execinfra.FlowCtx,
	processorID int32,
	spec execinfrapb.ChangeFrontierSpec,
	input execinfra.RowSource,
	output execinfra.RowReceiver,
) (execinfra.Processor, error) {
	ctx := flowCtx.EvalCtx.Ctx()
	memMonitor := execinfra.NewMonitor(ctx, flowCtx.EvalCtx.Mon, "changefntr-mem")
	cf := &changeFrontier{
		flowCtx: flowCtx,
		spec:    spec,
		memAcc:  memMonitor.MakeBoundAccount(),
		input:   input,
		sf:      makeSpanFrontier(spec.TrackedSpans...),
	}
	if err := cf.Init(
		cf, &execinfrapb.PostProcessSpec{},
		input.OutputTypes(),
		flowCtx,
		processorID,
		output,
		memMonitor,
		execinfra.ProcStateOpts{
			TrailingMetaCallback: func(context.Context) []execinfrapb.ProducerMetadata {
				cf.close()
				return nil
			},
			InputsToDrain: []execinfra.RowSource{cf.input},
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

func (cf *changeFrontier) OutputTypes() []types.T {
	return changefeedResultTypes
}

// Start is part of the RowSource interface.
func (cf *changeFrontier) Start(ctx context.Context) context.Context {
	cf.input.Start(ctx)

	// StartInternal called at the beginning of the function because there are
	// early returns if errors are detected.
	ctx = cf.StartInternal(ctx, changeFrontierProcName)

	nodeID := cf.flowCtx.EvalCtx.NodeID
	var err error
	// Pass a nil oracle because this sink is only used to emit resolved timestamps
	// but the oracle is only used when emitting row updates.
	var nilOracle timestampLowerBoundOracle
	if cf.sink, err = getSink(
		cf.spec.Feed.SinkURI, nodeID, cf.spec.Feed.Opts, cf.spec.Feed.Targets,
		cf.flowCtx.Cfg.Settings, nilOracle, cf.flowCtx.Cfg.ExternalStorageFromURI,
	); err != nil {
		err = MarkRetryableError(err)
		cf.MoveToDraining(err)
		return ctx
	}

	if b, ok := cf.sink.(*bufferSink); ok {
		cf.resolvedBuf = &b.buf
	}

	// The job registry has a set of metrics used to monitor the various jobs it
	// runs. They're all stored as the `metric.Struct` interface because of
	// dependency cycles.
	cf.metrics = cf.flowCtx.Cfg.JobRegistry.MetricsStruct().Changefeed.(*Metrics)
	cf.sink = makeMetricsSink(cf.metrics, cf.sink)
	cf.sink = &errorWrapperSink{wrapped: cf.sink}

	cf.highWaterAtStart = cf.spec.Feed.StatementTime
	if cf.spec.JobID != 0 {
		job, err := cf.flowCtx.Cfg.JobRegistry.LoadJob(ctx, cf.spec.JobID)
		if err != nil {
			cf.MoveToDraining(err)
			return ctx
		}
		cf.jobProgressedFn = job.HighWaterProgressed

		p := job.Progress()
		if ts := p.GetHighWater(); ts != nil {
			cf.highWaterAtStart.Forward(*ts)
		}
	}

	cf.metrics.mu.Lock()
	cf.metricsID = cf.metrics.mu.id
	cf.metrics.mu.id++
	cf.metrics.mu.Unlock()
	// TODO(dan): It's very important that we de-register from the metric because
	// if we orphan an entry in there, our monitoring will lie (say the changefeed
	// is behind when it may not be). We call this in `close` but that doesn't
	// always get called when the processor is shut down (especially during crdb
	// chaos), so here's something that maybe will work some of the times that
	// close doesn't. This is all very hacky. The real answer is to fix whatever
	// bugs currently exist in processor shutdown.
	go func() {
		<-ctx.Done()
		cf.closeMetrics()
	}()

	return ctx
}

func (cf *changeFrontier) close() {
	if cf.InternalClose() {
		if cf.metrics != nil {
			cf.closeMetrics()
		}
		if cf.sink != nil {
			if err := cf.sink.Close(); err != nil {
				log.Warningf(cf.Ctx, `error closing sink. goroutines may have leaked: %v`, err)
			}
		}
		cf.memAcc.Close(cf.Ctx)
		cf.MemMonitor.Stop(cf.Ctx)
	}
}

// closeMetrics de-registers from the progress registry that powers
// `changefeed.max_behind_nanos`. This method is idempotent.
func (cf *changeFrontier) closeMetrics() {
	// Delete this feed from the MaxBehindNanos metric so it's no longer
	// considered by the gauge.
	cf.metrics.mu.Lock()
	delete(cf.metrics.mu.resolved, cf.metricsID)
	cf.metricsID = -1
	cf.metrics.mu.Unlock()
}

// Next is part of the RowSource interface.
func (cf *changeFrontier) Next() (sqlbase.EncDatumRow, *execinfrapb.ProducerMetadata) {
	for cf.State == execinfra.StateRunning {
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
		return errors.AssertionFailedf(`unexpected datum type %T: %s`, d.Datum, d.Datum)
	}
	var resolved jobspb.ResolvedSpan
	if err := protoutil.Unmarshal([]byte(*raw), &resolved); err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err,
			`unmarshalling resolved span: %x`, raw)
	}

	// Inserting a timestamp less than the one the changefeed flow started at
	// could potentially regress the job progress. This is not expected, but it
	// was a bug at one point, so assert to prevent regressions.
	//
	// TODO(dan): This is much more naturally expressed as an assert inside the
	// job progress update closure, but it currently doesn't pass along the info
	// we'd need to do it that way.
	if !resolved.Timestamp.IsEmpty() && resolved.Timestamp.Less(cf.highWaterAtStart) {
		log.ReportOrPanic(cf.Ctx, &cf.flowCtx.Cfg.Settings.SV,
			`got a span level timestamp %s for %s that is less than the initial high-water %s`,
			log.Safe(resolved.Timestamp), resolved.Span, log.Safe(cf.highWaterAtStart))
		return nil
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

	// Potentially log the most behind span in the frontier for debugging. These
	// two cluster setting values represent the target responsiveness of poller
	// and range feed. The cluster setting for switching between poller and
	// rangefeed is only checked at changefeed start/resume, so instead of
	// switching on it here, just add them. Also add 1 second in case both these
	// settings are set really low (as they are in unit tests).
	pollInterval := changefeedPollInterval.Get(&cf.flowCtx.Cfg.Settings.SV)
	closedtsInterval := closedts.TargetDuration.Get(&cf.flowCtx.Cfg.Settings.SV)
	slownessThreshold := time.Second + 10*(pollInterval+closedtsInterval)
	frontier := cf.sf.Frontier()
	now := timeutil.Now()
	if resolvedBehind := now.Sub(frontier.GoTime()); resolvedBehind > slownessThreshold {
		description := `sinkless feed`
		if cf.spec.JobID != 0 {
			description = fmt.Sprintf("job %d", cf.spec.JobID)
		}
		if frontierChanged {
			log.Infof(cf.Ctx, "%s new resolved timestamp %s is behind by %s",
				description, frontier, resolvedBehind)
		}
		const slowSpanMaxFrequency = 10 * time.Second
		if now.Sub(cf.lastSlowSpanLog) > slowSpanMaxFrequency {
			cf.lastSlowSpanLog = now
			s := cf.sf.peekFrontierSpan()
			log.Infof(cf.Ctx, "%s span %s is behind by %s", description, s, resolvedBehind)
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
