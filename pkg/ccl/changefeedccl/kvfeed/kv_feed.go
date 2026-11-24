// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package kvfeed provides an abstraction to stream kvs to a buffer.
//
// The kvfeed coordinated performing logical backfills in the face of schema
// changes and then running rangefeeds.
package kvfeed

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/schemafeed"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/timers"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// MonitoringConfig is a set of callbacks which the kvfeed calls to provide
// the caller with information about the state of the kvfeed.
type MonitoringConfig struct {
	OnBackfillCallback      func() func()
	OnBackfillRangeCallback func(int64) (func(), func())
}

// Config configures a kvfeed.
type Config struct {
	Settings           *cluster.Settings
	DB                 *kv.DB
	Codec              keys.SQLCodec
	Clock              *hlc.Clock
	Spans              []roachpb.Span
	Targets            changefeedbase.Targets
	Writer             kvevent.Writer
	Metrics            *kvevent.Metrics
	MonitoringCfg      MonitoringConfig
	MM                 *mon.BytesMonitor
	WithDiff           bool
	SchemaChangeEvents changefeedbase.SchemaChangeEventClass
	SchemaChangePolicy changefeedbase.SchemaChangePolicy
	SchemaFeed         schemafeed.SchemaFeed

	// If true, the feed will begin with a dump of data at exactly the
	// InitialHighWater. This is a peculiar behavior. In general the
	// InitialHighWater is a point in time at which all data is known to have
	// been seen.
	NeedsInitialScan bool

	// InitialHighWater is the timestamp after which new events are guaranteed to
	// be produced. For initial scans, this is the scan time.
	InitialHighWater hlc.Timestamp

	// InitialSpanTimePairs contains pairs of spans and their initial resolved
	// timestamps. The timestamps could be lower than InitialHighWater if the
	// initial scan has not yet been completed.
	InitialSpanTimePairs []kvcoord.SpanTimePair

	// If the end time is set, the changefeed will run until the frontier
	// progresses past the end time. Once the frontier has progressed past the end
	// time, the changefeed job will end with a successful status.
	EndTime hlc.Timestamp

	// WithFiltering is propagated via the RangefeedRequest to the rangefeed
	// server, where if true, the server respects the OmitInRangefeeds flag and
	// enables filtering out any transactional writes with that flag set to true.
	WithFiltering bool

	// WithBulkDelivery is propagated via the RangefeedRequest to the rangefeed
	// server, where if true, the server will deliver rangefeed events in bulk
	// during catchup scans.
	WithBulkDelivery bool

	// WithFrontierQuantize specifies the resolved timestamp quantization
	// granularity. If non-zero, resolved timestamps from rangefeed checkpoint
	// events will be rounded down to the nearest multiple of the quantization
	// granularity.
	WithFrontierQuantize time.Duration

	// Knobs are kvfeed testing knobs.
	Knobs TestingKnobs

	ScopedTimers *timers.ScopedTimers

	ConsumerID int64
}

// Run will run the kvfeed. The feed runs synchronously and returns an
// error when it finishes.
func Run(ctx context.Context, cfg Config) (retErr error) {
	// The writer must always be closed to release resources and signal to the
	// consumer (changeAggregator) that no more writes are expected, allowing it
	// to transition to draining state. The defer ensures cleanup even if Drain
	// panics, while the success path explicitly closes before waiting for the
	// changeAggregator to finish.
	//
	// Closed tracks whether we've already closed the writer, preventing the
	// defer from closing twice.
	closed := false

	defer func() {
		if closed {
			return
		}
		closeReason := retErr
		if closeReason == nil {
			// A nil retErr indicates a panic occurred.
			closeReason = errors.New("kvfeed terminated abnormally")
		}
		if closeErr := cfg.Writer.CloseWithReason(ctx, closeReason); closeErr != nil {
			retErr = errors.CombineErrors(retErr, closeErr)
		}
	}()

	var sc kvScanner
	{
		sc = &scanRequestScanner{
			settings:                cfg.Settings,
			db:                      cfg.DB,
			onBackfillRangeCallback: cfg.MonitoringCfg.OnBackfillRangeCallback,
		}
	}
	var pff physicalFeedFactory
	{
		sender := cfg.DB.NonTransactionalSender()
		distSender := sender.(*kv.CrossRangeTxnWrapperSender).Wrapped().(*kvcoord.DistSender)
		pff = rangefeedFactory(distSender.RangeFeed)
	}

	bf := func() kvevent.Buffer {
		return kvevent.NewMemBuffer(cfg.MM.MakeBoundAccount(), &cfg.Settings.SV, &cfg.Metrics.RangefeedBufferMetrics)
	}

	g := ctxgroup.WithContext(ctx)
	f := newKVFeed(
		cfg.Writer, cfg.Spans,
		cfg.SchemaChangeEvents, cfg.SchemaChangePolicy,
		cfg.NeedsInitialScan, cfg.WithDiff, cfg.WithFiltering, cfg.WithBulkDelivery,
		cfg.WithFrontierQuantize,
		cfg.ConsumerID,
		cfg.InitialHighWater, cfg.InitialSpanTimePairs, cfg.EndTime,
		cfg.Codec,
		cfg.SchemaFeed,
		sc, pff, bf, cfg.Targets, cfg.ScopedTimers, cfg.Knobs)
	f.onBackfillCallback = cfg.MonitoringCfg.OnBackfillCallback

	g.GoCtx(cfg.SchemaFeed.Run)
	g.GoCtx(f.run)
	err := g.Wait()

	// NB: The higher layers of the changefeed should detect the boundary and the
	// policy and tear everything down. Returning before the higher layers tear down
	// the changefeed exposes synchronization challenges if the provided writer is
	// buffered. Errors returned from this function will cause the
	// changefeedAggregator to exit even if all values haven't been read out of the
	// provided buffer.
	var scErr schemaChangeDetectedError
	isChangefeedCompleted := errors.Is(err, errChangefeedCompleted)
	if !isChangefeedCompleted && !errors.As(err, &scErr) {
		log.Changefeed.Errorf(ctx, "stopping kv feed due to error: %s", err)
		// Regardless of whether we exited KV feed with or without an error, that error
		// is not a schema change; so, return and let the defer handle cleanup.
		return err
	}

	if isChangefeedCompleted {
		log.Changefeed.Info(ctx, "stopping kv feed: changefeed completed")
	} else {
		log.Changefeed.Infof(ctx, "stopping kv feed due to schema change at %v", scErr.ts)
	}

	// Drain the writer before we close it so that all events emitted prior to schema change
	// or changefeed completion boundary are consumed by the change aggregator.
	if drainErr := f.writer.Drain(ctx); drainErr != nil {
		return errors.Wrap(drainErr, "failed to drain kv feed writer")
	}

	// Close the writer to signal completion, then wait for the
	// changeAggregator to finish consuming all buffered events.
	closed = true
	if err := cfg.Writer.CloseWithReason(ctx, kvevent.ErrNormalRestartReason); err != nil {
		return err
	}

	// This context is canceled by the change aggregator when it receives
	// an error reading from the Writer that was closed above.
	<-ctx.Done()
	return ctx.Err()
}

// schemaChangeDetectedError is a sentinel error to indicate to Run() that the
// schema change is stopping due to a schema change. This is handy to trigger
// the context group to stop; the error is handled entirely in this package.
type schemaChangeDetectedError struct {
	ts hlc.Timestamp
}

func (e schemaChangeDetectedError) Error() string {
	return fmt.Sprintf("schema change detected at %v", e.ts)
}

type kvFeed struct {
	spans                []roachpb.Span
	withFrontierQuantize time.Duration
	withDiff             bool
	withFiltering        bool
	withInitialBackfill  bool
	withBulkDelivery     bool
	consumerID           int64
	initialHighWater     hlc.Timestamp
	initialSpanTimePairs []kvcoord.SpanTimePair
	endTime              hlc.Timestamp
	writer               kvevent.Writer
	codec                keys.SQLCodec

	onBackfillCallback func() func()
	rangeObserver      kvcoord.RangeObserver
	schemaChangeEvents changefeedbase.SchemaChangeEventClass
	schemaChangePolicy changefeedbase.SchemaChangePolicy

	targets changefeedbase.Targets
	timers  *timers.ScopedTimers

	// These dependencies are made available for test injection.
	bufferFactory func() kvevent.Buffer
	tableFeed     schemafeed.SchemaFeed
	scanner       kvScanner
	physicalFeed  physicalFeedFactory
	knobs         TestingKnobs
}

// TODO(yevgeniy): This method is a kitchen sink. Refactor.
func newKVFeed(
	writer kvevent.Writer,
	spans []roachpb.Span,
	schemaChangeEvents changefeedbase.SchemaChangeEventClass,
	schemaChangePolicy changefeedbase.SchemaChangePolicy,
	withInitialBackfill, withDiff, withFiltering, withBulkDelivery bool,
	withFrontierQuantize time.Duration,
	consumerID int64,
	initialHighWater hlc.Timestamp,
	initialSpanTimePairs []kvcoord.SpanTimePair,
	endTime hlc.Timestamp,
	codec keys.SQLCodec,
	tf schemafeed.SchemaFeed,
	sc kvScanner,
	pff physicalFeedFactory,
	bf func() kvevent.Buffer,
	targets changefeedbase.Targets,
	ts *timers.ScopedTimers,
	knobs TestingKnobs,
) *kvFeed {
	return &kvFeed{
		writer:               writer,
		spans:                spans,
		initialSpanTimePairs: initialSpanTimePairs,
		withInitialBackfill:  withInitialBackfill,
		withDiff:             withDiff,
		withFiltering:        withFiltering,
		withFrontierQuantize: withFrontierQuantize,
		withBulkDelivery:     withBulkDelivery,
		consumerID:           consumerID,
		initialHighWater:     initialHighWater,
		endTime:              endTime,
		schemaChangeEvents:   schemaChangeEvents,
		schemaChangePolicy:   schemaChangePolicy,
		codec:                codec,
		tableFeed:            tf,
		scanner:              sc,
		physicalFeed:         pff,
		bufferFactory:        bf,
		targets:              targets,
		timers:               ts,
		knobs:                knobs,
	}
}

var errChangefeedCompleted = errors.New("changefeed completed")

func (f *kvFeed) run(ctx context.Context) (err error) {
	ctx, sp := tracing.ChildSpan(ctx, "changefeed.kvfeed.run")
	defer sp.Finish()
	log.Changefeed.Infof(ctx, "kv feed run starting")

	emitResolved := func(ts hlc.Timestamp, boundary jobspb.ResolvedSpan_BoundaryType) error {
		if log.V(2) {
			log.Changefeed.Infof(ctx, "emitting resolved spans at time %s with boundary %s for spans: %s", ts, boundary, f.spans)
		}
		for _, sp := range f.spans {
			if err := f.writer.Add(ctx, kvevent.NewBackfillResolvedEvent(sp, ts, boundary)); err != nil {
				return err
			}
		}
		return nil
	}

	// Use the initial span-time pairs to restore progress.
	rangeFeedResumeFrontier, err := span.MakeFrontier(f.spans...)
	if err != nil {
		return err
	}
	for _, stp := range f.initialSpanTimePairs {
		if stp.StartAfter.IsSet() && stp.StartAfter.Compare(f.initialHighWater) < 0 {
			return errors.AssertionFailedf("initial span time pair with non-empty timestamp "+
				"earlier than initial highwater %v: %v", f.initialHighWater, stp)
		}
		if _, err := rangeFeedResumeFrontier.Forward(stp.Span, stp.StartAfter); err != nil {
			return err
		}
	}
	rangeFeedResumeFrontier = span.MakeConcurrentFrontier(rangeFeedResumeFrontier)
	defer rangeFeedResumeFrontier.Release()

	for i := 0; ; i++ {
		initialScan := i == 0
		initialScanOnly := f.endTime == f.initialHighWater

		scannedSpans, scannedTS, err := f.scanIfShould(ctx, initialScan, initialScanOnly, rangeFeedResumeFrontier)
		if err != nil {
			return err
		}
		// We have scanned scannedSpans up to and including scannedTS.  Advance frontier
		// for those spans.  Note, since rangefeed start time is *exclusive* (that it, rangefeed
		// starts from timestamp.Next()), we advanced frontier to the scannedTS.
		for _, sp := range scannedSpans {
			if _, err := rangeFeedResumeFrontier.Forward(sp, scannedTS); err != nil {
				return err
			}
		}

		if initialScanOnly {
			if err := emitResolved(f.initialHighWater, jobspb.ResolvedSpan_EXIT); err != nil {
				return err
			}
			return errChangefeedCompleted
		}

		if err = f.runUntilTableEvent(ctx, rangeFeedResumeFrontier); err != nil {
			if tErr := (*errEndTimeReached)(nil); errors.As(err, &tErr) {
				if err := emitResolved(rangeFeedResumeFrontier.Frontier(), jobspb.ResolvedSpan_EXIT); err != nil {
					return err
				}
				return errChangefeedCompleted
			}
			return err
		}

		boundaryTS := rangeFeedResumeFrontier.Frontier()
		schemaChangeTS := boundaryTS.Next()
		boundaryType := jobspb.ResolvedSpan_BACKFILL
		events, err := f.tableFeed.Peek(ctx, schemaChangeTS)
		if err != nil {
			return err
		}
		if log.V(2) {
			log.Changefeed.Infof(ctx, "kv feed encountered table events at or before %s: %#v", schemaChangeTS, events)
		}
		var tables []redact.RedactableString
		for _, event := range events {
			tables = append(tables, redact.Sprintf("table %q (id %d, version %d -> %d)",
				redact.Safe(event.Before.GetName()), event.Before.GetID(), event.Before.GetVersion(), event.After.GetVersion()))
		}
		log.Changefeed.Infof(ctx, "kv feed encountered schema change(s) at or before %s: %s",
			schemaChangeTS, redact.Join(", ", tables))

		// Detect whether the event corresponds to a primary index change. Also
		// detect whether the change corresponds to any change in the set of visible
		// primary key columns.
		//
		// If a primary key is being changed and there are no changes in the
		// primary key's columns, this may be due to a column which was dropped
		// logically before and is presently being physically dropped.
		//
		// If is no change in the primary key columns, then a primary key change
		// should not trigger a failure in the `stop` policy because this change is
		// effectively invisible to consumers.
		primaryIndexChange, noColumnChanges := isPrimaryKeyChange(events, f.targets)
		if primaryIndexChange && (noColumnChanges ||
			f.schemaChangePolicy != changefeedbase.OptSchemaChangePolicyStop) {
			boundaryType = jobspb.ResolvedSpan_RESTART
		} else if f.schemaChangePolicy == changefeedbase.OptSchemaChangePolicyStop {
			boundaryType = jobspb.ResolvedSpan_EXIT
		}
		// Resolve all of the spans as a boundary if the policy indicates that
		// we should do so.
		if f.schemaChangePolicy != changefeedbase.OptSchemaChangePolicyNoBackfill ||
			boundaryType == jobspb.ResolvedSpan_RESTART {
			if err := emitResolved(boundaryTS, boundaryType); err != nil {
				return err
			}
		}

		// Exit if the policy says we should.
		if boundaryType == jobspb.ResolvedSpan_RESTART || boundaryType == jobspb.ResolvedSpan_EXIT {
			log.Changefeed.Infof(ctx, "kv feed run loop exiting due to schema change at %s and boundary type %s", schemaChangeTS, boundaryType)
			return schemaChangeDetectedError{ts: schemaChangeTS}
		}

		log.Changefeed.Infof(ctx, "kv feed run loop restarting because of schema change at %s and boundary type %s", schemaChangeTS, boundaryType)
	}
}

func isPrimaryKeyChange(
	events []schemafeed.TableEvent, targets changefeedbase.Targets,
) (isPrimaryIndexChange, hasNoColumnChanges bool) {
	hasNoColumnChanges = true
	for _, ev := range events {
		if ok, noColumnChange := schemafeed.IsPrimaryIndexChange(ev, targets); ok {
			isPrimaryIndexChange = true
			hasNoColumnChanges = hasNoColumnChanges && noColumnChange
		}
	}
	return isPrimaryIndexChange, isPrimaryIndexChange && hasNoColumnChanges
}

// scanIfShould performs a scan of KV pairs in watched span if
// - this is the initial scan, or
// - table schema is changed (a column is added/dropped) and a re-scan is needed.
// It returns spans it has scanned, the timestamp at which the scan happened, and error if any.
//
// This function is responsible for emitting rows from either the initial reporting
// or from a table descriptor change. It is *not* responsible for capturing data changes
// from DMLs (INSERT, UPDATE, etc.). That is handled elsewhere from the underlying rangefeed.
//
// `highWater` is the largest timestamp at or below which we know all events in
// watched span have been seen (i.e. frontier.smallestTS).
func (f *kvFeed) scanIfShould(
	ctx context.Context, initialScan bool, initialScanOnly bool, resumeFrontier span.Frontier,
) ([]roachpb.Span, hlc.Timestamp, error) {
	ctx, sp := tracing.ChildSpan(ctx, "changefeed.kvfeed.scan_if_should")
	defer sp.Finish()

	highWater := resumeFrontier.Frontier()
	scanTime := func() hlc.Timestamp {
		if highWater.IsEmpty() {
			return f.initialHighWater
		}
		return highWater.Next()
	}()

	events, err := f.tableFeed.Peek(ctx, scanTime)
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}
	// This off-by-one is a little weird. It says that if you create a changefeed
	// at some statement time then you're going to get the table as of that statement
	// time with an initial backfill but if you use a cursor then you will get the
	// updates after that timestamp.
	isInitialScan := initialScan && f.withInitialBackfill
	var spansToScan []roachpb.Span
	if isInitialScan {
		spansToScan = f.spans
	} else if len(events) > 0 {
		// Only backfill for the tables which have events which may not be all
		// of the targets.
		for _, ev := range events {
			// If the event corresponds to a primary index change, it does not
			// indicate a need for a backfill. Furthermore, if the changefeed was
			// started at this timestamp because of a restart due to a primary index
			// change, then a backfill should not be performed for that table.
			// Below the code detects whether the set of spans to backfill is empty
			// and returns early. This is important because a change to a primary
			// index may occur in the same transaction as a change requiring a
			// backfill.
			if schemafeed.IsOnlyPrimaryIndexChange(ev) {
				continue
			}
			tablePrefix := f.codec.TablePrefix(uint32(ev.After.GetID()))
			tableSpan := roachpb.Span{Key: tablePrefix, EndKey: tablePrefix.PrefixEnd()}
			for _, sp := range f.spans {
				if tableSpan.Overlaps(sp) {
					spansToScan = append(spansToScan, sp)
				}
			}
			if !scanTime.Equal(ev.After.GetModificationTime()) {
				return nil, hlc.Timestamp{}, errors.Newf(
					"found event in scanIfShould which did not occur at the scan time %v: %v",
					scanTime, ev)
			}
		}
	} else {
		return nil, hlc.Timestamp{}, nil
	}

	// Consume the events up to scanTime.
	if _, err := f.tableFeed.Pop(ctx, scanTime); err != nil {
		return nil, hlc.Timestamp{}, err
	}

	if !isInitialScan && f.schemaChangePolicy == changefeedbase.OptSchemaChangePolicyNoBackfill {
		return spansToScan, scanTime, nil
	}

	// Filter out any spans that have already been backfilled.
	var spansToBackfill roachpb.SpanGroup
	spansToBackfill.Add(spansToScan...)
	for sp, ts := range resumeFrontier.Entries() {
		if scanTime.LessEq(ts) {
			spansToBackfill.Sub(sp)
		}
	}

	// Skip scan if there are no spans to scan.
	if spansToBackfill.Len() == 0 {
		return spansToScan, scanTime, nil
	}

	if f.onBackfillCallback != nil {
		defer f.onBackfillCallback()()
	}

	boundaryType := jobspb.ResolvedSpan_NONE
	if initialScanOnly {
		boundaryType = jobspb.ResolvedSpan_EXIT
	}
	if err := f.scanner.Scan(ctx, f.writer, scanConfig{
		Spans:     spansToBackfill.Slice(),
		Timestamp: scanTime,
		WithDiff:  !isInitialScan && f.withDiff,
		Knobs:     f.knobs,
		Boundary:  boundaryType,
	}); err != nil {
		return nil, hlc.Timestamp{}, err
	}

	// We return entire set of spans (ignoring possible checkpoint) because all of those
	// spans have been scanned up to and including scanTime.
	return spansToScan, scanTime, nil
}

// runUntilTableEvent starts rangefeeds for the spans being watched by
// the kv feed and runs until a table event (schema change) is encountered.
//
// If the function returns a nil error, resumeFrontier.Frontier() will be
// ts.Prev() where ts is the schema change timestamp.
func (f *kvFeed) runUntilTableEvent(ctx context.Context, resumeFrontier span.Frontier) (err error) {
	ctx, sp := tracing.ChildSpan(ctx, "changefeed.kvfeed.run_until_table_event")
	defer sp.Finish()

	startFrom := resumeFrontier.Frontier()

	// Determine whether to request the previous value of each update from
	// RangeFeed based on whether the `diff` option is specified.
	if _, err := f.tableFeed.Peek(ctx, startFrom); err != nil {
		return err
	}

	memBuf := f.bufferFactory()
	defer func() {
		err = errors.CombineErrors(err, memBuf.CloseWithReason(ctx, err))
	}()

	var stps []kvcoord.SpanTimePair
	for s, ts := range resumeFrontier.Entries() {
		stps = append(stps, kvcoord.SpanTimePair{Span: s, StartAfter: ts})
	}

	g := ctxgroup.WithContext(ctx)
	physicalCfg := rangeFeedConfig{
		Spans:                stps,
		Frontier:             resumeFrontier.Frontier(),
		WithDiff:             f.withDiff,
		WithFiltering:        f.withFiltering,
		WithFrontierQuantize: f.withFrontierQuantize,
		WithBulkDelivery:     f.withBulkDelivery,
		ConsumerID:           f.consumerID,
		Knobs:                f.knobs,
		Timers:               f.timers,
		RangeObserver:        f.rangeObserver,
	}

	// The following two synchronous calls works as follows:
	// - `f.physicalFeed.Run` establish a rangefeed on the watched spans at the
	// high watermark ts (i.e. frontier.smallestTS), which we know we have scanned,
	// and it will detect and send any changed data (from DML operations) to `membuf`.
	// - `copyFromSourceToDestUntilTableEvent` consumes `membuf` into `f.writer`
	// until a table event (i.e. a column is added/dropped) has occurred, which
	// signals another possible scan.
	g.GoCtx(func(ctx context.Context) error {
		return copyFromSourceToDestUntilTableEvent(ctx, f.writer, memBuf, resumeFrontier, f.tableFeed, f.endTime, f.knobs, f.timers)
	})
	g.GoCtx(func(ctx context.Context) error {
		return f.physicalFeed.Run(ctx, memBuf, physicalCfg)
	})

	// TODO(mrtracy): We are currently tearing down the entire rangefeed set in
	// order to perform a scan; however, given that we have an intermediate
	// buffer, its seems that we could do this without having to destroy and
	// recreate the rangefeeds.
	err = g.Wait()
	if err == nil {
		return errors.AssertionFailedf("feed exited with no error and no copy boundary")
	} else if tErr := (*errTableEventReached)(nil); errors.As(err, &tErr) {
		// TODO(ajwerner): iterate the spans and add a Resolved timestamp.
		// We'll need to do this to ensure that a resolved timestamp propagates
		// when we're trying to exit.
		return nil
	} else if tErr := (*errEndTimeReached)(nil); errors.As(err, &tErr) {
		return err
	} else {
		return err
	}
}

// copyBoundary is used within copyFromSourceToDestUntilTableEvent
// to encapsulate the timestamp at which we should stop copying and an
// error explaining the reason.
type copyBoundary interface {
	error
	Timestamp() hlc.Timestamp
}

var _ copyBoundary = (*errTableEventReached)(nil)
var _ copyBoundary = (*errEndTimeReached)(nil)

// errTableEventReached contains the earliest table event we receive, which
// contains the timestamp at which we should stop copying.
type errTableEventReached struct {
	schemafeed.TableEvent
}

func (e *errTableEventReached) Error() string {
	return "table event reached: " + e.String()
}

// errEndTimeReached contains the end timestamp at which we should stop copying.
type errEndTimeReached struct {
	endTime hlc.Timestamp
}

func (e *errEndTimeReached) Error() string {
	return "end time reached: " + e.endTime.String()
}

func (e *errEndTimeReached) Timestamp() hlc.Timestamp {
	return e.endTime
}

// errUnknownEvent indicates we should stop copying because we encountered an unknown event type.
type errUnknownEvent struct {
	kvevent.Event
}

func (e *errUnknownEvent) Error() string {
	return "unknown event type: " + e.String()
}

// copyFromSourceToDestUntilTableEvent will copy events from the source to the
// dest until a copy boundary is reached (i.e. the table event is encountered or
// the end time (if specified) is reached). Once this happens, the function will
// return after all of the spans have been resolved up to the copy boundary time.
// The frontier is forwarded for the relevant span whenever a resolved event is
// copied. A non-nil error containing details about why the copying stopped will
// always be returned.
func copyFromSourceToDestUntilTableEvent(
	ctx context.Context,
	dest kvevent.Writer,
	source kvevent.Reader,
	frontier span.Frontier,
	schemaFeed schemafeed.SchemaFeed,
	endTime hlc.Timestamp,
	knobs TestingKnobs,
	st *timers.ScopedTimers,
) error {
	ctx, sp := tracing.ChildSpan(ctx, "changefeed.kvfeed.copy_from_source_to_dest_until_table_event")
	defer sp.Finish()

	// Initially, the only copy boundary is the end time if one is specified.
	// Once we discover a table event (which is before the end time), that will
	// become the new boundary.
	var boundary copyBoundary
	if endTime.IsSet() {
		boundary = &errEndTimeReached{
			endTime: endTime,
		}
	}

	var (
		// checkForTableEvent takes in a new event's timestamp (event generated
		// from rangefeed) and checks if a table event was encountered at or before
		// said timestamp. If so, it replaces the copy boundary with the table event.
		checkForTableEvent = func(ts hlc.Timestamp) error {
			defer st.KVFeedWaitForTableEvent.Start().End()
			// There's no need to check for table events again if we already found one
			// since that should already be the earliest one.
			if _, ok := boundary.(*errTableEventReached); ok {
				return nil
			}

			nextEvents, err := schemaFeed.Peek(ctx, ts)
			if err != nil {
				return err
			}

			if len(nextEvents) > 0 {
				boundary = &errTableEventReached{nextEvents[0]}
			}

			return nil
		}

		// spanFrontier returns the frontier timestamp for the specified span by
		// finding the minimum timestamp of its subspans in the frontier.
		spanFrontier = func(sp roachpb.Span) hlc.Timestamp {
			minTs := hlc.MaxTimestamp
			for _, ts := range frontier.SpanEntries(sp) {
				if ts.Less(minTs) {
					minTs = ts
				}
			}
			if minTs == hlc.MaxTimestamp {
				return hlc.Timestamp{}
			}
			return minTs
		}

		// checkCopyBoundary checks the event against the current copy boundary
		// to determine if we should skip the event and/or whether we can stop copying.
		// We can stop copying once the frontier has reached boundary.Timestamp().Prev().
		// In most cases, a boundary does not exist, and thus we do nothing.
		// If a boundary has been discovered, but the event happens before that boundary,
		// we let the event proceed.
		// Otherwise (if `e.ts` >= `boundary.ts`), we will act as follows:
		//  - KV event: do nothing (we shouldn't emit this event)
		//  - Resolved event: advance this span to `boundary.ts` in the frontier
		checkCopyBoundary = func(e kvevent.Event) (skipEvent, stopCopying bool, err error) {
			if boundary == nil {
				return false, false, nil
			}
			if knobs.EndTimeReached != nil && knobs.EndTimeReached() {
				return true, true, nil
			}
			if e.Timestamp().Less(boundary.Timestamp()) {
				return false, false, nil
			}
			switch e.Type() {
			case kvevent.TypeKV:
				return true, false, nil
			case kvevent.TypeResolved:
				boundaryResolvedTimestamp := boundary.Timestamp().Prev()
				resolved := e.Resolved()
				if resolved.Timestamp.LessEq(boundaryResolvedTimestamp) {
					return false, false, nil
				}

				// At this point, we know event is after boundaryResolvedTimestamp.
				skipEvent = true

				if _, ok := boundary.(*errEndTimeReached); ok {
					// We know we've hit the end time boundary. In this case, we do not want to
					// skip this event because we want to make sure we emit checkpoint at
					// exactly boundaryResolvedTimestamp. This checkpoint can be used to
					// produce span based changefeed checkpoints if needed.
					// We only want to emit this checkpoint once, and then we can skip
					// subsequent checkpoints for this span until entire frontier reaches
					// boundary timestamp.
					if boundaryResolvedTimestamp.Compare(spanFrontier(resolved.Span)) > 0 {
						e.Raw().Checkpoint.ResolvedTS = boundaryResolvedTimestamp
						skipEvent = false
					}
				}

				if _, err := frontier.Forward(resolved.Span, boundaryResolvedTimestamp); err != nil {
					return false, false, err
				}

				return skipEvent, frontier.Frontier() == boundaryResolvedTimestamp, nil
			case kvevent.TypeFlush:
				// TypeFlush events have a timestamp of zero and should have already
				// been processed by the timestamp check above. We include this here
				// for completeness.
				return false, false, nil
			default:
				return false, false, &errUnknownEvent{e}
			}
		}

		// writeToDest writes an event to the dest.
		writeToDest = func(e kvevent.Event) error {
			defer st.KVFeedBuffer.Start().End()

			switch e.Type() {
			case kvevent.TypeKV, kvevent.TypeFlush:
				return dest.Add(ctx, e)
			case kvevent.TypeResolved:
				// TODO(ajwerner): technically this doesn't need to happen for most
				// events - we just need to make sure we forward for events which are
				// at boundary.Prev(). We may not yet know about that boundary.
				// The logic currently doesn't make this clean.
				resolved := e.Resolved()
				if _, err := frontier.Forward(resolved.Span, resolved.Timestamp); err != nil {
					return err
				}
				return dest.Add(ctx, e)
			default:
				return &errUnknownEvent{e}
			}
		}

		// checkAndCopyEvent checks to see if a new copy boundary exists and
		// whether the event should be copied. If so, it writes the event to dest.
		checkAndCopyEvent = func(e kvevent.Event) error {
			if err := checkForTableEvent(e.Timestamp()); err != nil {
				return err
			}
			skipEntry, stopCopying, err := checkCopyBoundary(e)
			if err != nil {
				return err
			}

			if skipEntry || stopCopying {
				// We will skip this entry or outright terminate kvfeed (if boundary reached).
				// Regardless of the reason, we must release this event memory allocation
				// since other ranges might not have reached copy boundary yet.
				// Failure to release this event allocation may prevent other events from being
				// enqueued in the blocking buffer due to memory limit.
				a := e.DetachAlloc()
				a.Release(ctx)
			}

			if stopCopying {
				// All component rangefeeds are now at the boundary.
				// Break out of the ctxgroup by returning the sentinel error.
				// (We don't care if skipEntry is false -- copy boundary can only be
				// returned for resolved event, and we don't care if we emit this event
				// since exiting with copy boundary error will cause appropriate
				// boundary type (EXIT) to be emitted for the entire frontier)
				return boundary
			}

			if skipEntry {
				return nil
			}
			return writeToDest(e)
		}
	)

	for {
		e, err := source.Get(ctx)
		if err != nil {
			return err
		}
		if err := checkAndCopyEvent(e); err != nil {
			return err
		}
	}
}
