// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

// Package kvfeed provides an abstraction to stream kvs to a buffer.
//
// The kvfeed coordinated performing logical backfills in the face of schema
// changes and then running rangefeeds.
package kvfeed

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/schemafeed"
	"github.com/cockroachdb/cockroach/pkg/gossip"
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
	"github.com/cockroachdb/errors"
)

// Config configures a kvfeed.
type Config struct {
	Settings                *cluster.Settings
	DB                      *kv.DB
	Codec                   keys.SQLCodec
	Clock                   *hlc.Clock
	Gossip                  gossip.OptionalGossip
	Spans                   []roachpb.Span
	CheckpointSpans         []roachpb.Span
	CheckpointTimestamp     hlc.Timestamp
	Targets                 changefeedbase.Targets
	Writer                  kvevent.Writer
	Metrics                 *kvevent.Metrics
	OnBackfillCallback      func() func()
	OnBackfillRangeCallback func(int64) (func(), func())
	MM                      *mon.BytesMonitor
	WithDiff                bool
	SchemaChangeEvents      changefeedbase.SchemaChangeEventClass
	SchemaChangePolicy      changefeedbase.SchemaChangePolicy
	SchemaFeed              schemafeed.SchemaFeed

	// If true, the feed will begin with a dump of data at exactly the
	// InitialHighWater. This is a peculiar behavior. In general the
	// InitialHighWater is a point in time at which all data is known to have
	// been seen.
	NeedsInitialScan bool

	// InitialHighWater is the timestamp after which new events are guaranteed to
	// be produced.
	InitialHighWater hlc.Timestamp

	// If the end time is set, the changefeed will run until the frontier
	// progresses past the end time. Once the frontier has progressed past the end
	// time, the changefeed job will end with a successful status.
	EndTime hlc.Timestamp

	// Knobs are kvfeed testing knobs.
	Knobs TestingKnobs

	// UseMux enables MuxRangeFeed rpc
	UseMux bool
}

// Run will run the kvfeed. The feed runs synchronously and returns an
// error when it finishes.
func Run(ctx context.Context, cfg Config) error {

	var sc kvScanner
	{
		sc = &scanRequestScanner{
			settings:                cfg.Settings,
			gossip:                  cfg.Gossip,
			db:                      cfg.DB,
			onBackfillRangeCallback: cfg.OnBackfillRangeCallback,
		}
	}
	var pff physicalFeedFactory
	{
		sender := cfg.DB.NonTransactionalSender()
		distSender := sender.(*kv.CrossRangeTxnWrapperSender).Wrapped().(*kvcoord.DistSender)
		pff = rangefeedFactory(distSender.RangeFeedSpans)
	}

	bf := func() kvevent.Buffer {
		return kvevent.NewMemBuffer(cfg.MM.MakeBoundAccount(), &cfg.Settings.SV, cfg.Metrics)
	}

	f := newKVFeed(
		cfg.Writer, cfg.Spans, cfg.CheckpointSpans, cfg.CheckpointTimestamp,
		cfg.SchemaChangeEvents, cfg.SchemaChangePolicy,
		cfg.NeedsInitialScan, cfg.WithDiff,
		cfg.InitialHighWater, cfg.EndTime,
		cfg.Codec,
		cfg.SchemaFeed,
		sc, pff, bf, cfg.UseMux, cfg.Targets, cfg.Knobs)
	f.onBackfillCallback = cfg.OnBackfillCallback

	g := ctxgroup.WithContext(ctx)
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
	if !(isChangefeedCompleted || errors.As(err, &scErr)) {
		// Regardless of whether we exited KV feed with or without an error, that error
		// is not a schema change; so, close the writer and return.
		return errors.CombineErrors(err, f.writer.CloseWithReason(ctx, err))
	}

	if isChangefeedCompleted {
		log.Info(ctx, "stopping kv feed: changefeed completed")
	} else {
		log.Infof(ctx, "stopping kv feed due to schema change at %v", scErr.ts)
	}

	// Drain the writer before we close it so that all events emitted prior to schema change
	// or changefeed completion boundary are consumed by the change aggregator.
	// Regardless of whether drain succeeds, we must also close the buffer to release
	// any resources, and to let the consumer (changeAggregator) know that no more writes
	// are expected so that it can transition to a draining state.
	err = errors.CombineErrors(
		f.writer.Drain(ctx),
		f.writer.CloseWithReason(ctx, kvevent.ErrNormalRestartReason),
	)

	if err == nil {
		// This context is canceled by the change aggregator when it receives
		// an error reading from the Writer that was closed above.
		<-ctx.Done()
	}

	return err
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
	spans               []roachpb.Span
	checkpoint          []roachpb.Span
	checkpointTimestamp hlc.Timestamp
	withDiff            bool
	withInitialBackfill bool
	initialHighWater    hlc.Timestamp
	endTime             hlc.Timestamp
	writer              kvevent.Writer
	codec               keys.SQLCodec

	onBackfillCallback func() func()
	schemaChangeEvents changefeedbase.SchemaChangeEventClass
	schemaChangePolicy changefeedbase.SchemaChangePolicy

	useMux bool

	targets changefeedbase.Targets

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
	checkpoint []roachpb.Span,
	checkpointTimestamp hlc.Timestamp,
	schemaChangeEvents changefeedbase.SchemaChangeEventClass,
	schemaChangePolicy changefeedbase.SchemaChangePolicy,
	withInitialBackfill, withDiff bool,
	initialHighWater hlc.Timestamp,
	endTime hlc.Timestamp,
	codec keys.SQLCodec,
	tf schemafeed.SchemaFeed,
	sc kvScanner,
	pff physicalFeedFactory,
	bf func() kvevent.Buffer,
	useMux bool,
	targets changefeedbase.Targets,
	knobs TestingKnobs,
) *kvFeed {
	return &kvFeed{
		writer:              writer,
		spans:               spans,
		checkpoint:          checkpoint,
		checkpointTimestamp: checkpointTimestamp,
		withInitialBackfill: withInitialBackfill,
		withDiff:            withDiff,
		initialHighWater:    initialHighWater,
		endTime:             endTime,
		schemaChangeEvents:  schemaChangeEvents,
		schemaChangePolicy:  schemaChangePolicy,
		codec:               codec,
		tableFeed:           tf,
		scanner:             sc,
		physicalFeed:        pff,
		bufferFactory:       bf,
		useMux:              useMux,
		targets:             targets,
		knobs:               knobs,
	}
}

var errChangefeedCompleted = errors.New("changefeed completed")

func (f *kvFeed) run(ctx context.Context) (err error) {
	emitResolved := func(ts hlc.Timestamp, boundary jobspb.ResolvedSpan_BoundaryType) error {
		for _, sp := range f.spans {
			if err := f.writer.Add(ctx, kvevent.NewBackfillResolvedEvent(sp, ts, boundary)); err != nil {
				return err
			}
		}
		return nil
	}

	// Frontier initialized to initialHighwater timestamp which
	// represents the point in time at or before which we know
	// we've seen all events or is the initial starting time of the feed.
	rangeFeedResumeFrontier, err := span.MakeFrontierAt(f.initialHighWater, f.spans...)
	if err != nil {
		return err
	}

	for i := 0; ; i++ {
		initialScan := i == 0
		scannedSpans, scannedTS, err := f.scanIfShould(ctx, initialScan, rangeFeedResumeFrontier.Frontier())
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

		initialScanOnly := f.endTime.EqOrdering(f.initialHighWater)
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

		// Clear out checkpoint after the initial scan or rangefeed.
		if initialScan {
			f.checkpoint = nil
			f.checkpointTimestamp = hlc.Timestamp{}
		}

		highWater := rangeFeedResumeFrontier.Frontier()
		boundaryType := jobspb.ResolvedSpan_BACKFILL
		events, err := f.tableFeed.Peek(ctx, highWater.Next())
		if err != nil {
			return err
		}

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
			if err := emitResolved(highWater, boundaryType); err != nil {
				return err
			}
		}

		// Exit if the policy says we should.
		if boundaryType == jobspb.ResolvedSpan_RESTART || boundaryType == jobspb.ResolvedSpan_EXIT {
			return schemaChangeDetectedError{highWater.Next()}
		}
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

// filterCheckpointSpans filters spans which have already been completed,
// and returns the list of spans that still need to be done.
func filterCheckpointSpans(spans []roachpb.Span, completed []roachpb.Span) []roachpb.Span {
	var sg roachpb.SpanGroup
	sg.Add(spans...)
	sg.Sub(completed...)
	return sg.Slice()
}

func (f *kvFeed) scanIfShould(
	ctx context.Context, initialScan bool, highWater hlc.Timestamp,
) ([]roachpb.Span, hlc.Timestamp, error) {
	scanTime := highWater.Next()

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
		scanTime = highWater
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
				return nil, hlc.Timestamp{}, errors.AssertionFailedf(
					"found event in shouldScan which did not occur at the scan time %v: %v",
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

	// If we have initial checkpoint information specified, filter out
	// spans which we no longer need to scan.
	spansToBackfill := filterCheckpointSpans(spansToScan, f.checkpoint)

	if (!isInitialScan && f.schemaChangePolicy == changefeedbase.OptSchemaChangePolicyNoBackfill) ||
		len(spansToBackfill) == 0 {
		return spansToScan, scanTime, nil
	}

	if f.onBackfillCallback != nil {
		defer f.onBackfillCallback()()
	}

	if err := f.scanner.Scan(ctx, f.writer, scanConfig{
		Spans:     spansToBackfill,
		Timestamp: scanTime,
		WithDiff:  !isInitialScan && f.withDiff,
		Knobs:     f.knobs,
	}); err != nil {
		return nil, hlc.Timestamp{}, err
	}

	// We return entire set of spans (ignoring possible checkpoint) because all of those
	// spans have been scanned up to and including scanTime.
	return spansToScan, scanTime, nil
}

func (f *kvFeed) runUntilTableEvent(
	ctx context.Context, resumeFrontier *span.Frontier,
) (err error) {
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

	// We have catchup scan checkpoint.  Advance frontier.
	if startFrom.Less(f.checkpointTimestamp) {
		for _, s := range f.checkpoint {
			if _, err := resumeFrontier.Forward(s, f.checkpointTimestamp); err != nil {
				return err
			}
		}
	}

	var stps []kvcoord.SpanTimePair
	resumeFrontier.Entries(func(s roachpb.Span, ts hlc.Timestamp) (done span.OpResult) {
		stps = append(stps, kvcoord.SpanTimePair{Span: s, StartAfter: ts})
		return span.ContinueMatch
	})

	g := ctxgroup.WithContext(ctx)
	physicalCfg := rangeFeedConfig{
		Spans:    stps,
		Frontier: resumeFrontier.Frontier(),
		WithDiff: f.withDiff,
		Knobs:    f.knobs,
		UseMux:   f.useMux,
	}

	g.GoCtx(func(ctx context.Context) error {
		return copyFromSourceToDestUntilTableEvent(ctx, f.writer, memBuf, resumeFrontier, f.tableFeed, f.endTime, f.knobs)
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
		return errors.AssertionFailedf("feed exited with no error and no scan boundary")
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

type errBoundaryReached interface {
	error
	Timestamp() hlc.Timestamp
}

type errTableEventReached struct {
	schemafeed.TableEvent
}

func (e *errTableEventReached) Error() string {
	return "scan boundary reached: " + e.String()
}

type errEndTimeReached struct {
	endTime hlc.Timestamp
}

func (e *errEndTimeReached) Error() string {
	return "end time reached: " + e.endTime.String()
}

func (e *errEndTimeReached) Timestamp() hlc.Timestamp {
	return e.endTime
}

type errUnknownEvent struct {
	kvevent.Event
}

var _ errBoundaryReached = (*errTableEventReached)(nil)
var _ errBoundaryReached = (*errEndTimeReached)(nil)

func (e *errUnknownEvent) Error() string {
	return "unknown event type"
}

// copyFromSourceToDestUntilTableEvents will pull read entries from source and
// publish them to the destination if there is no table event from the SchemaFeed. If a
// tableEvent occurs then the function will return once all of the spans have
// been resolved up to the event. The first such event will be returned as
// *errBoundaryReached. A nil error will never be returned.
func copyFromSourceToDestUntilTableEvent(
	ctx context.Context,
	dest kvevent.Writer,
	source kvevent.Reader,
	frontier *span.Frontier,
	tables schemafeed.SchemaFeed,
	endTime hlc.Timestamp,
	knobs TestingKnobs,
) error {
	var (
		scanBoundary         errBoundaryReached
		checkForScanBoundary = func(ts hlc.Timestamp) error {
			// If the scanBoundary is not nil, it either means that there is a table
			// event boundary set or a boundary for the end time. If the boundary is
			// for the end time, we should keep looking for table events.
			_, isEndTimeBoundary := scanBoundary.(*errEndTimeReached)
			if scanBoundary != nil && !isEndTimeBoundary {
				return nil
			}
			nextEvents, err := tables.Peek(ctx, ts)
			if err != nil {
				return err
			}

			// If there are any table events that occur, we will set the scan boundary
			// to this table event. However, if the end time is not empty, we will set
			// the scan boundary to the specified end time. Hence, we give a higher
			// precedence to table events.
			if len(nextEvents) > 0 {
				scanBoundary = &errTableEventReached{nextEvents[0]}
			} else if !endTime.IsEmpty() && scanBoundary == nil {
				scanBoundary = &errEndTimeReached{
					endTime: endTime,
				}
			}
			return nil
		}
		applyScanBoundary = func(e kvevent.Event) (skipEvent, reachedBoundary bool, err error) {
			if scanBoundary == nil {
				return false, false, nil
			}
			if knobs.EndTimeReached != nil && knobs.EndTimeReached() {
				return true, true, nil
			}
			if e.Timestamp().Less(scanBoundary.Timestamp()) {
				return false, false, nil
			}
			switch e.Type() {
			case kvevent.TypeKV:
				return true, false, nil
			case kvevent.TypeResolved:
				boundaryResolvedTimestamp := scanBoundary.Timestamp().Prev()
				resolved := e.Resolved()
				if resolved.Timestamp.LessEq(boundaryResolvedTimestamp) {
					return false, false, nil
				}
				if _, err := frontier.Forward(resolved.Span, boundaryResolvedTimestamp); err != nil {
					return false, false, err
				}
				return true, frontier.Frontier().EqOrdering(boundaryResolvedTimestamp), nil
			case kvevent.TypeFlush:
				// TypeFlush events have a timestamp of zero and should have already
				// been processed by the timestamp check above. We include this here
				// for completeness.
				return false, false, nil

			default:
				return false, false, &errUnknownEvent{e}
			}
		}
		addEntry = func(e kvevent.Event) error {
			switch e.Type() {
			case kvevent.TypeKV, kvevent.TypeFlush:
				return dest.Add(ctx, e)
			case kvevent.TypeResolved:
				// TODO(ajwerner): technically this doesn't need to happen for most
				// events - we just need to make sure we forward for events which are
				// at scanBoundary.Prev(). We may not yet know about that scanBoundary.
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
		copyEvent = func(e kvevent.Event) error {
			if err := checkForScanBoundary(e.Timestamp()); err != nil {
				return err
			}
			skipEntry, scanBoundaryReached, err := applyScanBoundary(e)
			if err != nil {
				return err
			}
			if scanBoundaryReached {
				// All component rangefeeds are now at the boundary.
				// Break out of the ctxgroup by returning the sentinel error.
				return scanBoundary
			}
			if skipEntry {
				return nil
			}
			return addEntry(e)
		}
	)

	for {
		e, err := source.Get(ctx)
		if err != nil {
			return err
		}
		if err := copyEvent(e); err != nil {
			return err
		}
	}
}
