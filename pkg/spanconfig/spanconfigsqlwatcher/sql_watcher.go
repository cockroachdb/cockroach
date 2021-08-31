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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// SQLWatcher implements the spanconfig.SQLWatcher interface.
var _ spanconfig.SQLWatcher = &SQLWatcher{}

// SQLWatcher is the concrete implementation of spanconfig.SQLWatcher. It
// establishes rangefeeds over system.{zones, descriptor} to incrementally watch
// for SQL events that may imply a change in the span configuration state.
type SQLWatcher struct {
	codec            keys.SQLCodec
	settings         *cluster.Settings
	rangeFeedFactory *rangefeed.Factory
	stopper          *stop.Stopper
	eventBuffer      *eventBuffer
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
	}
}

// WatchForSQLUpdates is part of the spanconfig.SQLWatcher interface.
func (s *SQLWatcher) WatchForSQLUpdates(
	ctx context.Context, timestamp hlc.Timestamp, handle spanconfig.SQLWatcherHandleFunc,
) error {
	descUpdatesCh, err := s.watchForDescriptorUpdates(ctx, timestamp)
	if err != nil {
		log.Warningf(ctx, "error establishing rangefeed over system.descritpors %v", err)
		return err
	}
	zonesUpdateCh, err := s.watchForZoneConfigUpdates(ctx, timestamp)
	if err != nil {
		log.Warningf(ctx, "error establishing rangefeed over system.zones %v", err)
		return err
	}
	for {
		var ev watcherEvent
		select {
		case <-ctx.Done():
			return nil
		case <-s.stopper.ShouldQuiesce():
			return nil
		case ev = <-descUpdatesCh:
		case ev = <-zonesUpdateCh:
		}
		s.eventBuffer.recordEvent(ev)
		ids, checkpointTS := s.eventBuffer.flushIDsBelowCheckpoint()
		if len(ids) != 0 {
			if err := s.stopper.RunAsyncTask(ctx, "sql-watcher", func(context.Context) {
				if err := handle(ids, checkpointTS); err != nil {
					log.Infof(ctx, "error handling sql watcher updates: %v", err)
				}
			}); err != nil {
				return err
			}
		}
	}
}

// watcherEvent is the struct produced by the rangefeeds the SQLWatcher
// establishes over system.{zones,descriptors}.
type watcherEvent struct {
	// Only meaningful if the event is a non checkpoint event.
	id descpb.ID

	timestamp hlc.Timestamp

	eventType eventType

	// isCheckpoint is set to true if the timestamp corresponds to a rangefeed
	// checkpoint.
	isCheckpoint bool
}

// watchForDescriptorUpdates establishes a rangefeed over system.descriptors.
// The descriptor ID is sent on the returned channel if the descriptor changing
// _may_ imply span configuration changes. Concretely, an update is sent for:
// 1. Changes to a table/database
// 2. table deletion.
// Note that no update is generated when a {type, schema} descriptor changes or
// a database descriptor is deleted. This is because such changes never imply
// span configuration changes, and as such, are of no interest to the
// SQLWatcher.
func (s *SQLWatcher) watchForDescriptorUpdates(
	ctx context.Context, timestamp hlc.Timestamp,
) (<-chan watcherEvent, error) {
	updatesCh := make(chan watcherEvent)
	descriptorTableStart := s.codec.TablePrefix(keys.DescriptorTableID)
	descriptorTableSpan := roachpb.Span{
		Key:    descriptorTableStart,
		EndKey: descriptorTableStart.PrefixEnd(),
	}
	handleEvent := func(ctx context.Context, ev *roachpb.RangeFeedValue) {
		deleted := false
		value := ev.Value
		if !ev.Value.IsPresent() {
			// The descriptor was deleted.
			value = ev.PrevValue
			deleted = true
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

		table, database, _, _ := descpb.FromDescriptorWithMVCCTimestamp(&descriptor, ev.Value.Timestamp)

		var id descpb.ID
		if table != nil {
			id = table.GetID()
		} else if database != nil {
			if deleted {
				// There's nothing for us to do if a database descriptor is deleted.
				// Databases don't create their own span config entries -- they only
				// serve as a placeholder for configs to be used during hydration. Thus
				// there are no entries to clean up here and we can simply return.
				return
			}
			id = database.GetID()
		} else {
			// We only care about database or table descriptors being updated.
			return
		}

		event := watcherEvent{
			id:           id,
			timestamp:    ev.Value.Timestamp,
			eventType:    DescriptorsEventType,
			isCheckpoint: false,
		}

		// TODO(arul): The rangefeed currently can't pass this handler a checkpoint
		// watcherEvent. We still need to handle that so that we can pass along the
		// checkpoint watcherEvent to the consumer of updatesCh.

		select {
		case <-ctx.Done():
		case updatesCh <- event:
		}
	}
	opts := []rangefeed.Option{
		rangefeed.WithDiff(),
	}
	rf, err := s.rangeFeedFactory.RangeFeed(
		ctx,
		"sql-watcher-descriptor-rangefeed",
		descriptorTableSpan,
		timestamp,
		handleEvent,
		opts...,
	)
	if err != nil {
		return nil, err
	}
	s.stopper.AddCloser(rf)

	log.Infof(ctx, "established range feed over system.descriptors table starting at time %s", timestamp)
	return updatesCh, nil
}

// watchForZoneConfigUpdates establishes a rangefeed over system.zones and sends
// updates on the returned channel.
func (s *SQLWatcher) watchForZoneConfigUpdates(
	ctx context.Context, timestamp hlc.Timestamp,
) (<-chan watcherEvent, error) {
	updatesCh := make(chan watcherEvent)

	zoneTableStart := s.codec.TablePrefix(keys.ZonesTableID)
	zoneTableSpan := roachpb.Span{
		Key:    zoneTableStart,
		EndKey: zoneTableStart.PrefixEnd(),
	}

	handleEvent := func(ctx context.Context, ev *roachpb.RangeFeedValue) {
		decoder := NewZonesDecoder(s.codec)
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

		event := watcherEvent{
			id:           descID,
			timestamp:    ev.Value.Timestamp,
			eventType:    ZonesEventType,
			isCheckpoint: false,
		}

		// TODO(arul): The rangefeed currently can't pass this handler a checkpoint
		// watcherEvent. We still need to handle that so that we can pass along the
		// checkpoint watcherEvent to the consumer of updatesCh.

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
	)
	if err != nil {
		return nil, err
	}
	s.stopper.AddCloser(rf)

	log.Infof(ctx, "established range feed over system.zones table starting at time %s", timestamp)
	return updatesCh, nil
}
