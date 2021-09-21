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
	Settings           *cluster.Settings
	DB                 *kv.DB
	Codec              keys.SQLCodec
	Clock              *hlc.Clock
	Gossip             gossip.OptionalGossip
	Spans              []roachpb.Span
	BackfillCheckpoint []roachpb.Span
	Targets            jobspb.ChangefeedTargets
	Metrics            *kvevent.Metrics
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
	// be produced.
	InitialHighWater hlc.Timestamp

	// Knobs are kvfeed testing knobs.
	Knobs TestingKnobs
}

// SetupFeed configures KV feed and returns a blocking function, used ot run the feed, and
// a reader which receives KV events.
func SetupFeed(
	ctx context.Context, cfg Config,
) (func(ctx context.Context) error, kvevent.Reader, error) {
	var sc kvScanner
	{
		sc = &scanRequestScanner{
			settings: cfg.Settings,
			gossip:   cfg.Gossip,
			db:       cfg.DB,
		}
	}
	var pff physicalFeedFactory
	{
		sender := cfg.DB.NonTransactionalSender()
		distSender := sender.(*kv.CrossRangeTxnWrapperSender).Wrapped().(*kvcoord.DistSender)
		pff = rangefeedFactory(distSender.RangeFeed)
	}

	f, reader, err := setupFeed(cfg, sc, pff)
	if err != nil {
		return nil, nil, err
	}
	return f.execute, reader, nil
}

// setupFeed is provided for test dependency injection.
func setupFeed(cfg Config, sc kvScanner, pff physicalFeedFactory) (*kvFeed, kvevent.Reader, error) {
	buf := kvevent.NewErrorWrapperEventBuffer(
		kvevent.NewMemBuffer(cfg.MM.MakeBoundAccount(), &cfg.Settings.SV, cfg.Metrics))
	writer, err := newSchemaFeedFilteringWriter(
		buf.(kvevent.Writer), cfg.Spans, cfg.InitialHighWater, cfg.SchemaFeed)
	if err != nil {
		return nil, nil, err
	}

	f := &kvFeed{
		Config:       cfg,
		writer:       writer,
		scanner:      sc,
		physicalFeed: pff,
	}

	return f, buf.(kvevent.Reader), nil
}

// execute will run the kvfeed. The feed runs synchronously and returns an
// error when it finishes.
func (f *kvFeed) execute(ctx context.Context) error {
	g := ctxgroup.WithContext(ctx)
	g.GoCtx(f.SchemaFeed.Run)
	g.GoCtx(f.run)
	err := g.Wait()

	// NB: The higher layers of the changefeed should detect the boundary and the
	// policy and tear everything down. Returning before the higher layers tear down
	// the changefeed exposes synchronization challenges if the provided writer is
	// buffered. Errors returned from this function will cause the
	// changefeedAggregator to exit even if all values haven't been read out of the
	// provided buffer.
	var scErr schemaChangeDetectedError
	if !errors.As(err, &scErr) {
		// Regardless of whether we exited KV feed with or without an error, that error
		// is not a schema change; so, close the writer and return.
		return errors.CombineErrors(err, f.writer.Close(ctx))
	}

	log.Infof(ctx, "stopping kv feed due to schema change at %v", scErr.ts)

	// Drain the writer before we close it so that all events emitted prior to schema change
	// boundary are consumed by the change aggregator.
	// Regardless of whether drain succeeds, we must also close the buffer to release
	// any resources, and to let the consumer (changeAggregator) know that no more writes
	// are expected so that it can transition to a draining state.
	err = errors.CombineErrors(f.writer.Drain(ctx), f.writer.Close(ctx))

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
	Config
	writer       *schemaFeedFilteringWriter
	scanner      kvScanner
	physicalFeed physicalFeedFactory
}

func (f *kvFeed) run(ctx context.Context) (err error) {
	// highWater represents the point in time at or before which we know
	// we've seen all events or is the initial starting time of the feed.
	highWater := f.InitialHighWater
	for i := 0; ; i++ {
		initialScan := i == 0
		f.writer.filter.disable() // No need to filter during backfills.
		if err = f.scanIfShould(ctx, initialScan, highWater); err != nil {
			return err
		}

		// Before starting kvfeed, drain the buffer, and enable schema event filtering.
		err := f.writer.Drain(ctx)
		if err != nil {
			return err
		}
		f.writer.filter.enable()

		highWater, err = f.runUntilTableEvent(ctx, highWater)
		if err != nil {
			return err
		}

		boundaryType := jobspb.ResolvedSpan_BACKFILL
		if f.SchemaChangePolicy == changefeedbase.OptSchemaChangePolicyStop {
			boundaryType = jobspb.ResolvedSpan_EXIT
		} else if events, err := f.SchemaFeed.Peek(ctx, highWater.Next()); err == nil && isPrimaryKeyChange(events) {
			boundaryType = jobspb.ResolvedSpan_RESTART
		} else if err != nil {
			return err
		}
		// Resolve all of the spans as a boundary if the policy indicates that
		// we should do so.
		if f.SchemaChangePolicy != changefeedbase.OptSchemaChangePolicyNoBackfill ||
			boundaryType == jobspb.ResolvedSpan_RESTART {
			for _, sp := range f.Spans {
				if err := f.writer.Add(
					ctx,
					kvevent.MakeResolvedEvent(sp, highWater, boundaryType),
				); err != nil {
					return err
				}
			}
		}

		// Exit if the policy says we should.
		if boundaryType == jobspb.ResolvedSpan_RESTART || boundaryType == jobspb.ResolvedSpan_EXIT {
			return schemaChangeDetectedError{highWater.Next()}
		}
	}
}

func isPrimaryKeyChange(events []schemafeed.TableEvent) bool {
	for _, ev := range events {
		if schemafeed.IsPrimaryIndexChange(ev) {
			return true
		}
	}
	return false
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
) error {
	scanTime := highWater.Next()
	events, err := f.SchemaFeed.Peek(ctx, scanTime)
	if err != nil {
		return err
	}
	// This off-by-one is a little weird. It says that if you create a changefeed
	// at some statement time then you're going to get the table as of that statement
	// time with an initial backfill but if you use a cursor then you will get the
	// updates after that timestamp.
	isInitialScan := initialScan && f.NeedsInitialScan
	var spansToBackfill []roachpb.Span
	if isInitialScan {
		scanTime = highWater
		spansToBackfill = f.Spans
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
			tablePrefix := f.Codec.TablePrefix(uint32(ev.After.GetID()))
			tableSpan := roachpb.Span{Key: tablePrefix, EndKey: tablePrefix.PrefixEnd()}
			for _, sp := range f.Spans {
				if tableSpan.Overlaps(sp) {
					spansToBackfill = append(spansToBackfill, sp)
				}
			}
			if !scanTime.Equal(ev.After.GetModificationTime()) {
				return errors.AssertionFailedf("found event in shouldScan which did not occur at the scan time %v: %v",
					scanTime, ev)
			}
		}
	} else {
		return nil
	}

	// Consume the events up to scanTime.
	if _, err := f.SchemaFeed.Pop(ctx, scanTime); err != nil {
		return err
	}

	// If we have initial BackfillCheckpoint information specified, filter out
	// spans which we no longer need to scan.
	if initialScan {
		spansToBackfill = filterCheckpointSpans(spansToBackfill, f.BackfillCheckpoint)
	}

	if (!isInitialScan && f.SchemaChangePolicy == changefeedbase.OptSchemaChangePolicyNoBackfill) ||
		len(spansToBackfill) == 0 {
		return nil
	}

	if err := f.scanner.Scan(ctx, f.writer, physicalConfig{
		Spans:     spansToBackfill,
		Timestamp: scanTime,
		WithDiff:  !isInitialScan && f.WithDiff,
		Knobs:     f.Knobs,
	}); err != nil {
		return err
	}

	// NB: We don't update the highwater even though we've technically seen all
	// events for all spans at the previous highwater.Next(). We choose not to
	// because doing so would be wrong once we only backfill some tables.
	return nil
}

func (f *kvFeed) runUntilTableEvent(
	ctx context.Context, startFrom hlc.Timestamp,
) (resolvedUpTo hlc.Timestamp, err error) {
	// Determine whether to request the previous value of each update from
	// RangeFeed based on whether the `diff` option is specified.
	if _, err := f.SchemaFeed.Peek(ctx, startFrom); err != nil {
		return hlc.Timestamp{}, err
	}

	physicalCfg := physicalConfig{
		Spans:     f.Spans,
		Timestamp: startFrom,
		WithDiff:  f.WithDiff,
		Knobs:     f.Knobs,
	}
	err = f.physicalFeed.Run(ctx, f.writer, physicalCfg)

	// TODO(mrtracy): We are currently tearing down the entire rangefeed set in
	// order to perform a scan; however, given that we have an intermediate
	// buffer, its seems that we could do this without having to destroy and
	// recreate the rangefeeds.
	if err == nil {
		return hlc.Timestamp{},
			errors.AssertionFailedf("feed exited with no error and no scan boundary")
	} else if tErr := (*errBoundaryReached)(nil); errors.As(err, &tErr) {
		// TODO(ajwerner): iterate the spans and add a Resolved timestamp.
		// We'll need to do this to ensure that a resolved timestamp propagates
		// when we're trying to exit.
		return tErr.Timestamp().Prev(), nil
	} else {
		return hlc.Timestamp{}, err
	}
}

type errBoundaryReached struct {
	schemafeed.TableEvent
}

func (e *errBoundaryReached) Error() string {
	return "scan boundary reached: " + e.String()
}

type errUnknownEvent struct {
	kvevent.Event
}

func (e *errUnknownEvent) Error() string {
	return "unknown event type"
}

type filterResult bool

const keepEvent filterResult = true
const skipEvent filterResult = false

type schemaFeedFilteringWriter struct {
	kvevent.Writer
	filter schemaFeedFilter
}

var _ kvevent.Writer = (*schemaFeedFilteringWriter)(nil)

// newSchemaFeedFilteringWriter returns a writer that will apply schema feed filtering
// prior to forwarding such events to the underlying writer.
func newSchemaFeedFilteringWriter(
	wrapped kvevent.Writer, spans []roachpb.Span, ts hlc.Timestamp, tables schemafeed.SchemaFeed,
) (*schemaFeedFilteringWriter, error) {
	// Maintain a local spanfrontier to tell when all the component rangefeeds
	// being watched have reached the Scan boundary.
	frontier, err := span.MakeFrontier(spans...)
	if err != nil {
		return nil, err
	}
	for _, sp := range spans {
		if _, err := frontier.Forward(sp, ts); err != nil {
			return nil, err
		}
	}

	return &schemaFeedFilteringWriter{
		Writer: wrapped,
		filter: schemaFeedFilter{
			tables:   tables,
			frontier: frontier,
		},
	}, nil
}

func (f *schemaFeedFilteringWriter) Add(ctx context.Context, event kvevent.Event) error {
	if filter, err := f.filter.filter(ctx, event); err != nil {
		return err
	} else if filter == skipEvent {
		return nil
	}
	return f.Writer.Add(ctx, event)
}

type schemaFeedFilter struct {
	tables       schemafeed.SchemaFeed
	frontier     *span.Frontier
	enabled      bool
	scanBoundary *errBoundaryReached
}

func (f *schemaFeedFilter) enable() {
	f.enabled = true
}
func (f *schemaFeedFilter) disable() {
	f.enabled = false
	f.scanBoundary = nil
}

// filter applies schema feed filter to an event.
// When no table events have been seen by the SchemaFeed, all entries are kept.
// Once a table event occurs, KV updates with timestamps greater than the event's timestamp
// are skipped and the function returns once all the spans have been resolved up to the event.
// The first such event is returned as *errBoundaryReached.
func (f *schemaFeedFilter) filter(ctx context.Context, e kvevent.Event) (filterResult, error) {
	if !f.enabled {
		return keepEvent, nil
	}

	if err := f.checkForScanBoundary(ctx, e.Timestamp()); err != nil {
		return skipEvent, err
	}
	filter, scanBoundaryReached, err := f.applyScanBoundary(e)
	if err != nil {
		return skipEvent, err
	}
	if scanBoundaryReached {
		// All component rangefeeds are now at the boundary.
		// Break out of the ctxgroup by returning the sentinel error.
		return skipEvent, f.scanBoundary
	}
	if filter == skipEvent {
		return skipEvent, nil
	}

	if e.Type() == kvevent.TypeResolved {
		// TODO(ajwerner): technically this doesn't need to happen for most
		// events - we just need to make sure we forward for events which are
		// at scanBoundary.Prev(). We may not yet know about that scanBoundary.
		// The logic currently doesn't make this clean.
		resolved := e.Resolved()
		if _, err := f.frontier.Forward(resolved.Span, resolved.Timestamp); err != nil {
			return skipEvent, err
		}
	}

	return keepEvent, nil
}

func (f *schemaFeedFilter) checkForScanBoundary(ctx context.Context, ts hlc.Timestamp) error {
	if f.scanBoundary != nil {
		return nil
	}
	nextEvents, err := f.tables.Peek(ctx, ts)
	if err != nil {
		return err
	}
	if len(nextEvents) > 0 {
		f.scanBoundary = &errBoundaryReached{nextEvents[0]}
	}
	return nil
}

func (f *schemaFeedFilter) applyScanBoundary(
	e kvevent.Event,
) (filter filterResult, reachedBoundary bool, err error) {
	if f.scanBoundary == nil {
		return keepEvent, false, nil
	}
	if e.Timestamp().Less(f.scanBoundary.Timestamp()) {
		return keepEvent, false, nil
	}
	switch e.Type() {
	case kvevent.TypeKV:
		return skipEvent, false, nil
	case kvevent.TypeResolved:
		boundaryResolvedTimestamp := f.scanBoundary.Timestamp().Prev()
		resolved := e.Resolved()
		if resolved.Timestamp.LessEq(boundaryResolvedTimestamp) {
			return keepEvent, false, nil
		}
		if _, err := f.frontier.Forward(resolved.Span, boundaryResolvedTimestamp); err != nil {
			return keepEvent, false, err
		}
		return skipEvent, f.frontier.Frontier().EqOrdering(boundaryResolvedTimestamp), nil
	default:
		return keepEvent, false, &errUnknownEvent{e}
	}
}
