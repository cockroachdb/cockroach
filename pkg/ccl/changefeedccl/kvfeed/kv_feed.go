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
	BackfillCheckpoint      []roachpb.Span
	Targets                 []jobspb.ChangefeedTargetSpecification
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

	// Knobs are kvfeed testing knobs.
	Knobs TestingKnobs
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
		pff = rangefeedFactory(distSender.RangeFeed)
	}

	bf := func() kvevent.Buffer {
		return kvevent.NewErrorWrapperEventBuffer(
			kvevent.NewMemBuffer(cfg.MM.MakeBoundAccount(), &cfg.Settings.SV, cfg.Metrics))
	}

	f := newKVFeed(
		cfg.Writer, cfg.Spans, cfg.BackfillCheckpoint,
		cfg.SchemaChangeEvents, cfg.SchemaChangePolicy,
		cfg.NeedsInitialScan, cfg.WithDiff,
		cfg.InitialHighWater,
		cfg.Codec,
		cfg.SchemaFeed,
		sc, pff, bf, cfg.Knobs)
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
	if !errors.As(err, &scErr) {
		// Regardless of whether we exited KV feed with or without an error, that error
		// is not a schema change; so, close the writer and return.
		return errors.CombineErrors(err, f.writer.CloseWithReason(ctx, err))
	}

	log.Infof(ctx, "stopping kv feed due to schema change at %v", scErr.ts)

	// Drain the writer before we close it so that all events emitted prior to schema change
	// boundary are consumed by the change aggregator.
	// Regardless of whether drain succeeds, we must also close the buffer to release
	// any resources, and to let the consumer (changeAggregator) know that no more writes
	// are expected so that it can transition to a draining state.
	err = errors.CombineErrors(f.writer.Drain(ctx), f.writer.CloseWithReason(ctx, kvevent.ErrNormalRestartReason))

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
	withDiff            bool
	withInitialBackfill bool
	initialHighWater    hlc.Timestamp
	writer              kvevent.Writer
	codec               keys.SQLCodec

	onBackfillCallback func() func()
	schemaChangeEvents changefeedbase.SchemaChangeEventClass
	schemaChangePolicy changefeedbase.SchemaChangePolicy

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
	schemaChangeEvents changefeedbase.SchemaChangeEventClass,
	schemaChangePolicy changefeedbase.SchemaChangePolicy,
	withInitialBackfill, withDiff bool,
	initialHighWater hlc.Timestamp,
	codec keys.SQLCodec,
	tf schemafeed.SchemaFeed,
	sc kvScanner,
	pff physicalFeedFactory,
	bf func() kvevent.Buffer,
	knobs TestingKnobs,
) *kvFeed {
	return &kvFeed{
		writer:              writer,
		spans:               spans,
		checkpoint:          checkpoint,
		withInitialBackfill: withInitialBackfill,
		withDiff:            withDiff,
		initialHighWater:    initialHighWater,
		schemaChangeEvents:  schemaChangeEvents,
		schemaChangePolicy:  schemaChangePolicy,
		codec:               codec,
		tableFeed:           tf,
		scanner:             sc,
		physicalFeed:        pff,
		bufferFactory:       bf,
		knobs:               knobs,
	}
}

func (f *kvFeed) run(ctx context.Context) (err error) {
	// highWater represents the point in time at or before which we know
	// we've seen all events or is the initial starting time of the feed.
	highWater := f.initialHighWater
	for i := 0; ; i++ {
		initialScan := i == 0
		if err = f.scanIfShould(ctx, initialScan, highWater); err != nil {
			return err
		}

		highWater, err = f.runUntilTableEvent(ctx, highWater)
		if err != nil {
			return err
		}

		boundaryType := jobspb.ResolvedSpan_BACKFILL
		if f.schemaChangePolicy == changefeedbase.OptSchemaChangePolicyStop {
			boundaryType = jobspb.ResolvedSpan_EXIT
		} else if events, err := f.tableFeed.Peek(ctx, highWater.Next()); err == nil && isPrimaryKeyChange(events) {
			boundaryType = jobspb.ResolvedSpan_RESTART
		} else if err != nil {
			return err
		}
		// Resolve all of the spans as a boundary if the policy indicates that
		// we should do so.
		if f.schemaChangePolicy != changefeedbase.OptSchemaChangePolicyNoBackfill ||
			boundaryType == jobspb.ResolvedSpan_RESTART {
			for _, sp := range f.spans {
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

	events, err := f.tableFeed.Peek(ctx, scanTime)
	if err != nil {
		return err
	}
	// This off-by-one is a little weird. It says that if you create a changefeed
	// at some statement time then you're going to get the table as of that statement
	// time with an initial backfill but if you use a cursor then you will get the
	// updates after that timestamp.
	isInitialScan := initialScan && f.withInitialBackfill
	var spansToBackfill []roachpb.Span
	if isInitialScan {
		scanTime = highWater
		spansToBackfill = f.spans
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
	if _, err := f.tableFeed.Pop(ctx, scanTime); err != nil {
		return err
	}

	// If we have initial checkpoint information specified, filter out
	// spans which we no longer need to scan.
	spansToBackfill = filterCheckpointSpans(spansToBackfill, f.checkpoint)

	if (!isInitialScan && f.schemaChangePolicy == changefeedbase.OptSchemaChangePolicyNoBackfill) ||
		len(spansToBackfill) == 0 {
		return nil
	}

	if f.onBackfillCallback != nil {
		defer f.onBackfillCallback()()
	}

	if err := f.scanner.Scan(ctx, f.writer, physicalConfig{
		Spans:     spansToBackfill,
		Timestamp: scanTime,
		WithDiff:  !isInitialScan && f.withDiff,
		Knobs:     f.knobs,
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
	if _, err := f.tableFeed.Peek(ctx, startFrom); err != nil {
		return hlc.Timestamp{}, err
	}

	memBuf := f.bufferFactory()
	defer func() {
		err = errors.CombineErrors(err, memBuf.CloseWithReason(ctx, err))
	}()

	g := ctxgroup.WithContext(ctx)
	physicalCfg := physicalConfig{
		Spans:     f.spans,
		Timestamp: startFrom,
		WithDiff:  f.withDiff,
		Knobs:     f.knobs,
	}
	g.GoCtx(func(ctx context.Context) error {
		return copyFromSourceToDestUntilTableEvent(ctx, f.writer, memBuf, physicalCfg, f.tableFeed)
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
		return hlc.Timestamp{},
			errors.AssertionFailedf("feed exited with no error and no scan boundary")
	} else if tErr := (*errBoundaryReached)(nil); errors.As(err, &tErr) {
		// TODO(ajwerner): iterate the spans and add a Resolved timestamp.
		// We'll need to do this to ensure that a resolved timestamp propagates
		// when we're trying to exit.
		return tErr.Timestamp().Prev(), nil
	} else if kvcoord.IsSendError(err) {
		// During node shutdown it is possible for all outgoing transports used by
		// the kvfeed to expire, producing a SendError that the node is still able
		// to propagate to the frontier. This has been known to happen during
		// cluster upgrades. This scenario should not fail the changefeed.
		err = changefeedbase.MarkRetryableError(err)
		return hlc.Timestamp{}, err
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

// copyFromSourceToDestUntilTableEvents will pull read entries from source and
// publish them to the destination if there is no table event from the SchemaFeed. If a
// tableEvent occurs then the function will return once all of the spans have
// been resolved up to the event. The first such event will be returned as
// *errBoundaryReached. A nil error will never be returned.
func copyFromSourceToDestUntilTableEvent(
	ctx context.Context,
	dest kvevent.Writer,
	source kvevent.Reader,
	cfg physicalConfig,
	tables schemafeed.SchemaFeed,
) error {
	// Maintain a local spanfrontier to tell when all the component rangefeeds
	// being watched have reached the Scan boundary.
	frontier, err := span.MakeFrontier(cfg.Spans...)
	if err != nil {
		return err
	}
	for _, span := range cfg.Spans {
		if _, err := frontier.Forward(span, cfg.Timestamp); err != nil {
			return err
		}
	}
	var (
		scanBoundary         *errBoundaryReached
		checkForScanBoundary = func(ts hlc.Timestamp) error {
			if scanBoundary != nil {
				return nil
			}
			nextEvents, err := tables.Peek(ctx, ts)
			if err != nil {
				return err
			}
			if len(nextEvents) > 0 {
				scanBoundary = &errBoundaryReached{nextEvents[0]}
			}
			return nil
		}
		applyScanBoundary = func(e kvevent.Event) (skipEvent, reachedBoundary bool, err error) {
			if scanBoundary == nil {
				return false, false, nil
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
