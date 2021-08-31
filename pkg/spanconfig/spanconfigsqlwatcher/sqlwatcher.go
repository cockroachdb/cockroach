// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigsqlwatcher

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

// SQLWatcher implements the spanconfig.SQLWatcher interface.
var _ spanconfig.SQLWatcher = &SQLWatcher{}

// SQLWatcher is the concrete implementation of spanconfig.SQLWatcher. It
// establishes rangefeeds over system.zones and system.descriptors to
// incrementally watch for SQL updates.
type SQLWatcher struct {
	codec            keys.SQLCodec
	settings         *cluster.Settings
	rangeFeedFactory *rangefeed.Factory
	stopper          *stop.Stopper
	buffer           *buffer

	descriptorsRF *rangefeed.RangeFeed
	zonesRF       *rangefeed.RangeFeed

	knobs *spanconfig.TestingKnobs
}

// limit for the rangefeed buffer.
// TODO(arul): Placeholder until we come up with something sane out of thin air.
//  Don't merge this.
const limit = 10000

// New constructs and returns a SQLWatcher.
func New(
	codec keys.SQLCodec,
	settings *cluster.Settings,
	rangeFeedFactory *rangefeed.Factory,
	stopper *stop.Stopper,
	knobs *spanconfig.TestingKnobs,
) *SQLWatcher {
	return &SQLWatcher{
		codec:            codec,
		settings:         settings,
		rangeFeedFactory: rangeFeedFactory,
		stopper:          stopper,
		buffer:           newBuffer(limit),
		knobs:            knobs,
	}
}

// WatchForSQLUpdates is part of the spanconfig.SQLWatcher interface.
func (s *SQLWatcher) WatchForSQLUpdates(
	ctx context.Context, timestamp hlc.Timestamp, handle spanconfig.SQLWatcherHandleFunc,
) error {
	// The callbacks below are invoked by both the rangefeeds we establish, which
	// run on separate goroutines. The SQLWatcher interface aims to provide the
	// following guarantees:
	// 1. Calls to the handle callback must be serial (and with a monotonically
	// increasing timestamp).
	// 2. If there is an error, either in the handle callback or the underlying
	// rangefeed that informs its invocation, there will be no further calls to
	// handle.
	// We uphold these semantics by only ever calling handle on the main thread
	// (as opposed to calling it in the rangefeed callbacks with some
	// synchronization). This has the added benefit of keeping the rangefeed
	// callbacks fairly cheap, which is desirable because we don't want to block
	// the rangefeed for too long. Users of this interface should thus be free to
	// do expensive operations in the callback, such as multiple RPCs, which is
	// something the spanconfig.Reconciler will do in practice.
	errCh := make(chan error)
	frontierAdvanced := make(chan struct{})
	onFrontierAdvance := func(ctx context.Context, rangefeed rangefeedKind, timestamp hlc.Timestamp) {
		s.buffer.advance(rangefeed, timestamp)
		select {
		case <-ctx.Done():
		case frontierAdvanced <- struct{}{}:
		}
	}
	onEvent := func(ctx context.Context, event event) {
		err := func() error {
			if fn := s.knobs.SQLWatcherOnEventInterceptor; fn != nil {
				if err := fn(); err != nil {
					return err
				}
			}
			return s.buffer.add(event)
		}()
		log.Warningf(ctx, "error adding event %v: %v", event, err)
		if err != nil {
			select {
			case <-ctx.Done():
			case errCh <- err:
			}
		}
	}

	err := s.watchForDescriptorUpdates(ctx, timestamp, onEvent, onFrontierAdvance)
	if err != nil {
		return errors.Wrapf(err, "error establishing rangefeed over system.descriptors")
	}
	err = s.watchForZoneConfigUpdates(ctx, timestamp, onEvent, onFrontierAdvance)
	if err != nil {
		return errors.Wrapf(err, "error establishing rangefeed over system.zones")
	}
	// We always tear down rangefeeds before returning to the caller.
	defer s.close()

	for {
		select {
		case <-ctx.Done():
			return nil
		case err = <-errCh:
			return err
		case <-frontierAdvanced:
			events, combinedFrontierTS := s.buffer.flush(ctx)
			if len(events) != 0 {
				if err := handle(ctx, events, combinedFrontierTS); err != nil {
					return err
				}
			}
		}
	}
}

// close stops the rangefeeds created by the SQLWatcher and waits for them
// to shut down before returning. Close is idempotent.
func (s *SQLWatcher) close() {
	s.zonesRF.Close()
	s.descriptorsRF.Close()
}

// watchForDescriptorUpdates establishes a rangefeed over system.descriptors and
// invokes the onEvent callback for observed events. The onFrontierAdvance
// callback is invoked whenever the rangefeed frontier is advanced as well.
func (s *SQLWatcher) watchForDescriptorUpdates(
	ctx context.Context,
	timestamp hlc.Timestamp,
	onEvent func(context.Context, event),
	onFrontierAdvance func(context.Context, rangefeedKind, hlc.Timestamp),
) error {
	descriptorTableStart := s.codec.TablePrefix(keys.DescriptorTableID)
	descriptorTableSpan := roachpb.Span{
		Key:    descriptorTableStart,
		EndKey: descriptorTableStart.PrefixEnd(),
	}
	handleEvent := func(ctx context.Context, ev *roachpb.RangeFeedValue) {
		if !ev.Value.IsPresent() && !ev.PrevValue.IsPresent() {
			// Event for a tombstone on a tombstone -- nothing for us to do here.
			return
		}
		value := ev.Value
		if !ev.Value.IsPresent() {
			// The descriptor was deleted.
			value = ev.PrevValue
		}

		var descriptor descpb.Descriptor
		if err := value.GetProto(&descriptor); err != nil {
			logcrash.ReportOrPanic(
				ctx,
				&s.settings.SV,
				"%s: failed to unmarshal descriptor %v",
				ev.Key,
				value,
			)
			return
		}
		if descriptor.Union == nil {
			return
		}

		table, database, typ, schema := descpb.FromDescriptorWithMVCCTimestamp(&descriptor, value.Timestamp)

		var id descpb.ID
		var descType catalog.DescriptorType
		switch {
		case table != nil:
			id = table.GetID()
			descType = catalog.Table
		case database != nil:
			id = database.GetID()
			descType = catalog.Database
		case typ != nil:
			id = typ.GetID()
			descType = catalog.Type
		case schema != nil:
			id = schema.GetID()
			descType = catalog.Schema
		default:
			logcrash.ReportOrPanic(ctx, &s.settings.SV, "unknown descriptor unmarshalled %v", descriptor)
		}

		rangefeedEvent := event{
			timestamp: timestamp,
			update: spanconfig.SQLWatcherUpdate{
				ID:             id,
				DescriptorType: descType,
			},
		}
		onEvent(ctx, rangefeedEvent)
	}
	rf, err := s.rangeFeedFactory.RangeFeed(
		ctx,
		"sql-watcher-descriptor-rangefeed",
		descriptorTableSpan,
		timestamp,
		handleEvent,
		rangefeed.WithDiff(),
		rangefeed.WithOnFrontierAdvance(func(ctx context.Context, resolvedTS hlc.Timestamp) {
			onFrontierAdvance(ctx, descriptorsRangefeed, resolvedTS)
		}),
	)
	if err != nil {
		return err
	}
	s.stopper.AddCloser(rf)
	s.descriptorsRF = rf

	log.Infof(ctx, "established range feed over system.descriptors table starting at time %s", timestamp)
	return nil
}

// watchForZoneConfigUpdates establishes a rangefeed over system.zones and
// invokes the onEvent callback whenever an event is observed. The
// onFrontierAdvance callback is also invoked whenever the rangefeed frontier is
// advanced.
func (s *SQLWatcher) watchForZoneConfigUpdates(
	ctx context.Context,
	timestamp hlc.Timestamp,
	onEvent func(context.Context, event),
	onFrontierAdvance func(context.Context, rangefeedKind, hlc.Timestamp),
) error {
	zoneTableStart := s.codec.TablePrefix(keys.ZonesTableID)
	zoneTableSpan := roachpb.Span{
		Key:    zoneTableStart,
		EndKey: zoneTableStart.PrefixEnd(),
	}

	decoder := NewZonesDecoder(s.codec)
	handleEvent := func(ctx context.Context, ev *roachpb.RangeFeedValue) {
		descID, err := decoder.DecodePrimaryKey(ev.Key)
		if err != nil {
			logcrash.ReportOrPanic(
				ctx,
				&s.settings.SV,
				"sql watcher zones range feed error: %v",
				err,
			)
			return
		}

		rangefeedEvent := event{
			timestamp: ev.Value.Timestamp,
			update: spanconfig.SQLWatcherUpdate{
				ID:             descID,
				DescriptorType: catalog.Any,
			},
		}
		onEvent(ctx, rangefeedEvent)
	}
	rf, err := s.rangeFeedFactory.RangeFeed(
		ctx,
		"sql-watcher-zones-rangefeed",
		zoneTableSpan,
		timestamp,
		handleEvent,
		rangefeed.WithOnFrontierAdvance(func(ctx context.Context, resolvedTS hlc.Timestamp) {
			onFrontierAdvance(ctx, zonesRangefeed, resolvedTS)
		}),
	)
	if err != nil {
		return err
	}
	s.stopper.AddCloser(rf)
	s.zonesRF = rf

	log.Infof(ctx, "established range feed over system.zones table starting at time %s", timestamp)
	return nil
}
