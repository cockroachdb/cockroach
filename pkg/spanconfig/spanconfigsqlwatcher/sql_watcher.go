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
	"sync"

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
)

// SQLWatcher implements the spanconfig.SQLWatcher interface.
var _ spanconfig.SQLWatcher = &SQLWatcher{}

// SQLWatcher is the concrete implementation of spanconfig.SQLWatcher. It
// establishes rangefeeds over system.zones and system.descriptors to
// incrementally watch for SQL events.
type SQLWatcher struct {
	codec            keys.SQLCodec
	settings         *cluster.Settings
	rangeFeedFactory *rangefeed.Factory
	stopper          *stop.Stopper
	eventBuffer      *eventBuffer

	closeOnce sync.Once
	cancel    context.CancelFunc
	stopped   chan struct{}

	descriptorsRF *rangefeed.RangeFeed
	zonesRF       *rangefeed.RangeFeed
}

// New constructs and returns a SQLWatcher.
func New(
	codec keys.SQLCodec,
	settings *cluster.Settings,
	rangeFeedFactory *rangefeed.Factory,
	stopper *stop.Stopper,
) *SQLWatcher {
	return &SQLWatcher{
		codec:            codec,
		settings:         settings,
		rangeFeedFactory: rangeFeedFactory,
		stopper:          stopper,
		eventBuffer:      newEventBuffer(),
		stopped:          make(chan struct{}),
	}
}

// WatchForSQLUpdates is part of the spanconfig.SQLWatcher interface.
func (s *SQLWatcher) WatchForSQLUpdates(
	ctx context.Context, timestamp hlc.Timestamp, handle spanconfig.SQLWatcherHandleFunc,
) error {
	updatesCh := make(chan watcherRangefeedEvent)
	err := s.watchForDescriptorUpdates(ctx, timestamp, updatesCh)
	if err != nil {
		log.Warningf(ctx, "error establishing rangefeed over system.descritpors %v", err)
		return err
	}
	err = s.watchForZoneConfigUpdates(ctx, timestamp, updatesCh)
	if err != nil {
		log.Warningf(ctx, "error establishing rangefeed over system.zones %v", err)
		return err
	}

	ctx, s.cancel = s.stopper.WithCancelOnQuiesce(ctx)
	if err := s.stopper.RunAsyncTask(ctx, "sql-watcher-rangefeed-consumer", func(ctx context.Context) {
		defer close(s.stopped)
		for {
			var ev watcherRangefeedEvent
			select {
			case <-ctx.Done():
				return
			case <-s.stopper.ShouldQuiesce():
				return
			case ev = <-updatesCh:
				s.eventBuffer.recordRangefeedEvent(ev)
			}
		}
	}); err != nil {
		s.cancel()
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		// TODO(arul): Should we add a timer here to accumulate updates for some
		// amount of time before calling the handler?
		default:
		}
		events, checkpointTS := s.eventBuffer.flushSQLWatcherEventsBelowCheckpoint()
		if len(events) != 0 {
			if err := handle(events, checkpointTS); err != nil {
				return err
			}
		}
	}
}

// Close implements the spanconfig.SQLWatcher interface.
func (s *SQLWatcher) Close() {
	s.closeOnce.Do(func() {
		s.zonesRF.Close()
		s.descriptorsRF.Close()
		s.cancel()
		<-s.stopped
	})
}

// watcherRangefeedEvent is the struct produced by the rangefeeds the SQLWatcher
// establishes over system.zones and system.descriptors.
type watcherRangefeedEvent struct {
	// timestamp for which the event was received. It should always be set.
	timestamp hlc.Timestamp

	// eventChannel represents the channel (zones, descriptors) an event
	// originated from. It should always be set.
	eventChannel eventChannel

	// isCheckpoint is set to true iff the event is a checkpoint event.
	isCheckpoint bool

	// id of the descriptor for which the event was received. It is only set if
	// the event is not a checkpoint.
	id descpb.ID

	// descriptorType represents the descriptor type corresponding to the ID above.
	// It is only set if the event is not a checkpoint.
	descriptorType catalog.DescriptorType
}

// watchForDescriptorUpdates establishes a rangefeed over system.descriptors and
// sends updates on the provided channel.
func (s *SQLWatcher) watchForDescriptorUpdates(
	ctx context.Context, timestamp hlc.Timestamp, updatesCh chan watcherRangefeedEvent,
) error {
	descriptorTableStart := s.codec.TablePrefix(keys.DescriptorTableID)
	descriptorTableSpan := roachpb.Span{
		Key:    descriptorTableStart,
		EndKey: descriptorTableStart.PrefixEnd(),
	}
	handleEvent := func(ctx context.Context, ev *roachpb.RangeFeedValue) {
		value := ev.Value
		if !ev.Value.IsPresent() {
			// The descriptor was deleted.
			value = ev.PrevValue
		}
		if !value.IsPresent() {
			return
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

		select {
		case <-ctx.Done():
		case updatesCh <- watcherRangefeedEvent{
			id:             id,
			descriptorType: descType,
			timestamp:      value.Timestamp,
			eventChannel:   descriptorsEventChannel,
			isCheckpoint:   false,
		}:
		}
	}
	rf, err := s.rangeFeedFactory.RangeFeed(
		ctx,
		"sql-watcher-descriptor-rangefeed",
		descriptorTableSpan,
		timestamp,
		handleEvent,
		rangefeed.WithDiff(),
		rangefeed.WithOnCheckpoint(func(ctx context.Context, checkpoint *roachpb.RangeFeedCheckpoint) {
			updatesCh <- watcherRangefeedEvent{
				timestamp:    checkpoint.ResolvedTS,
				eventChannel: descriptorsEventChannel,
				isCheckpoint: true,
			}
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

// watchForZoneConfigUpdates establishes a rangefeed over system.zones and sends
// updates on the provided channel.
func (s *SQLWatcher) watchForZoneConfigUpdates(
	ctx context.Context, timestamp hlc.Timestamp, updatesCh chan watcherRangefeedEvent,
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

		event := watcherRangefeedEvent{
			id:             descID,
			descriptorType: catalog.Any,
			timestamp:      ev.Value.Timestamp,
			eventChannel:   zonesEventChannel,
			isCheckpoint:   false,
		}

		select {
		case <-ctx.Done():
		case updatesCh <- event:
		}
	}
	rf, err := s.rangeFeedFactory.RangeFeed(
		ctx,
		"sql-watcher-zones-rangefeed",
		zoneTableSpan,
		timestamp,
		handleEvent,
		rangefeed.WithOnCheckpoint(func(ctx context.Context, checkpoint *roachpb.RangeFeedCheckpoint) {
			updatesCh <- watcherRangefeedEvent{
				timestamp:    checkpoint.ResolvedTS,
				eventChannel: zonesEventChannel,
				isCheckpoint: true,
			}
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
