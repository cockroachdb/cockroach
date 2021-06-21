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
	Sink               kvevent.Writer
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

// Run will run the kvfeed. The feed runs synchronously and returns an
// error when it finishes.
func Run(ctx context.Context, cfg Config) error {

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

	bf := func() kvevent.Buffer {
		return kvevent.NewErrorWrapperEventBuffer(kvevent.NewMemBuffer(cfg.MM.MakeBoundAccount(), cfg.Metrics))
	}

	f := newKVFeed(
		cfg.Sink, cfg.Spans, cfg.BackfillCheckpoint,
		cfg.SchemaChangeEvents, cfg.SchemaChangePolicy,
		cfg.NeedsInitialScan, cfg.WithDiff,
		cfg.InitialHighWater,
		cfg.Codec,
		cfg.SchemaFeed,
		sc, pff, bf, cfg.Knobs)

	g := ctxgroup.WithContext(ctx)
	g.GoCtx(cfg.SchemaFeed.Run)
	g.GoCtx(f.run)
	err := g.Wait()
	// NB: The higher layers of the changefeed should detect the boundary and the
	// policy and tear everything down. Returning before the higher layers tear
	// down the changefeed exposes synchronization challenges.
	var scErr schemaChangeDetectedError
	if errors.As(err, &scErr) {
		log.Infof(ctx, "stopping changefeed due to schema change at %v", scErr.ts)
		<-ctx.Done()
		err = nil
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

type unsupportedSchemaChangeDetected struct {
	desc string
	ts   hlc.Timestamp
}

func (e unsupportedSchemaChangeDetected) Error() string {
	return fmt.Sprintf("unsupported schema change %s detected at %s", e.desc, e.ts.AsOfSystemTime())
}

type kvFeed struct {
	spans               []roachpb.Span
	checkpoint          []roachpb.Span
	withDiff            bool
	withInitialBackfill bool
	initialHighWater    hlc.Timestamp
	sink                kvevent.Writer
	codec               keys.SQLCodec

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
	sink kvevent.Writer,
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
		sink:                sink,
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
		} else if events, err := f.tableFeed.Peek(ctx, highWater.Next()); err == nil && isRegionalByRowChange(events) {
			// NOTE(ssd): The user is unlikely to see this
			// error. The schemafeed will fail with an
			// non-retriable error, meaning we likely
			// return right after runUntilTableEvent
			// above.
			return unsupportedSchemaChangeDetected{
				desc: "SET REGIONAL BY ROW",
				ts:   highWater.Next(),
			}
		} else if err == nil && isPrimaryKeyChange(events) {
			boundaryType = jobspb.ResolvedSpan_RESTART
		} else if err != nil {
			return err
		}
		// Resolve all of the spans as a boundary if the policy indicates that
		// we should do so.
		if f.schemaChangePolicy != changefeedbase.OptSchemaChangePolicyNoBackfill ||
			boundaryType == jobspb.ResolvedSpan_RESTART {
			for _, sp := range f.spans {
				if err := f.sink.AddResolved(ctx, sp, highWater, boundaryType); err != nil {
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

func isRegionalByRowChange(events []schemafeed.TableEvent) bool {
	for _, ev := range events {
		if schemafeed.IsRegionalByRowChange(ev) {
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
				log.Fatalf(ctx, "found event in shouldScan which did not occur at the scan time %v: %v",
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
	if initialScan {
		spansToBackfill = filterCheckpointSpans(spansToBackfill, f.checkpoint)
	}

	if (!isInitialScan && f.schemaChangePolicy == changefeedbase.OptSchemaChangePolicyNoBackfill) ||
		len(spansToBackfill) == 0 {
		return nil
	}

	if err := f.scanner.Scan(ctx, f.sink, physicalConfig{
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
	defer memBuf.Close(ctx)

	g := ctxgroup.WithContext(ctx)
	physicalCfg := physicalConfig{
		Spans:     f.spans,
		Timestamp: startFrom,
		WithDiff:  f.withDiff,
		Knobs:     f.knobs,
	}
	g.GoCtx(func(ctx context.Context) error {
		return copyFromSourceToSinkUntilTableEvent(ctx, f.sink, memBuf, physicalCfg, f.tableFeed)
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
		log.Fatalf(ctx, "feed exited with no error and no scan boundary")
		return hlc.Timestamp{}, nil // unreachable
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

// copyFromSourceToSinkUntilTableEvents will pull read entries from source and
// publish them to sink if there is no table event from the SchemaFeed. If a
// tableEvent occurs then the function will return once all of the spans have
// been resolved up to the event. The first such event will be returned as
// *errBoundaryReached. A nil error will never be returned.
func copyFromSourceToSinkUntilTableEvent(
	ctx context.Context,
	sink kvevent.Writer,
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
			default:
				log.Fatal(ctx, "unknown event type")
				return false, false, nil
			}
		}
		addEntry = func(e kvevent.Event) error {
			switch e.Type() {
			case kvevent.TypeKV:
				return sink.AddKV(ctx, e.KV(), e.PrevValue(), e.BackfillTimestamp())
			case kvevent.TypeResolved:
				// TODO(ajwerner): technically this doesn't need to happen for most
				// events - we just need to make sure we forward for events which are
				// at scanBoundary.Prev(). We may not yet know about that scanBoundary.
				// The logic currently doesn't make this clean.
				resolved := e.Resolved()
				if _, err := frontier.Forward(resolved.Span, resolved.Timestamp); err != nil {
					return err
				}
				return sink.AddResolved(ctx, resolved.Span, resolved.Timestamp, jobspb.ResolvedSpan_NONE)
			default:
				log.Fatal(ctx, "unknown event type")
				return nil
			}
		}
	)
	for {
		e, err := source.Get(ctx)
		if err != nil {
			return err
		}
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
			continue
		}
		if err := addEntry(e); err != nil {
			return err
		}
	}
}
