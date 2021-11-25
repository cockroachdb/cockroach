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
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// SQLWatcher implements the spanconfig.SQLWatcher interface.
var _ spanconfig.SQLWatcher = &SQLWatcher{}

// SQLWatcher is the concrete implementation of spanconfig.SQLWatcher. It
// establishes rangefeeds over system.zones and system.descriptors to
// incrementally watch for SQL updates.
type SQLWatcher struct {
	codec                keys.SQLCodec
	settings             *cluster.Settings
	stopper              *stop.Stopper
	knobs                *spanconfig.TestingKnobs
	rangeFeedFactory     *rangefeed.Factory
	bufferMemLimit       int64
	checkpointNoopsEvery time.Duration
}

// New constructs a new SQLWatcher.
func New(
	codec keys.SQLCodec,
	settings *cluster.Settings,
	rangeFeedFactory *rangefeed.Factory,
	bufferMemLimit int64,
	stopper *stop.Stopper,
	checkpointNoopsEvery time.Duration,
	knobs *spanconfig.TestingKnobs,
) *SQLWatcher {
	if knobs == nil {
		knobs = &spanconfig.TestingKnobs{}
	}
	return &SQLWatcher{
		codec:                codec,
		settings:             settings,
		rangeFeedFactory:     rangeFeedFactory,
		stopper:              stopper,
		bufferMemLimit:       bufferMemLimit,
		checkpointNoopsEvery: checkpointNoopsEvery,
		knobs:                knobs,
	}
}

// sqlWatcherBufferEntrySize is the size of an entry stored in the SQLWatcher's
// buffer. We use this value to calculate the buffer capacity.
const sqlWatcherBufferEntrySize = int64(unsafe.Sizeof(event{}) + unsafe.Sizeof(rangefeedbuffer.Event(nil)))

// WatchForSQLUpdates is part of the spanconfig.SQLWatcher interface.
func (s *SQLWatcher) WatchForSQLUpdates(
	ctx context.Context,
	startTS hlc.Timestamp,
	handler func(context.Context, []spanconfig.DescriptorUpdate, hlc.Timestamp) error,
) error {
	return s.watch(ctx, startTS, handler)
}

func (s *SQLWatcher) watch(
	ctx context.Context,
	startTS hlc.Timestamp,
	handler func(context.Context, []spanconfig.DescriptorUpdate, hlc.Timestamp) error,
) error {

	// The callbacks below are invoked by both the rangefeeds we establish, both
	// of which run on separate goroutines. We serialize calls to the handler
	// function by invoking in this single watch thread (instead of pushing it
	// into the rangefeed callbacks). The rangefeed callbacks use channels to
	// report errors and notifications to flush events from the buffer. As
	// WatchForSQLUpdate's main thread is the sole listener on these channels,
	// doing expensive work in the handler function can lead to blocking the
	// rangefeed, which isn't great. This is an unfortunate asterisk for users
	// of this interface to be aware of.
	//
	// TODO(arul): Possibly get rid of this limitation by introducing another
	// buffer interface here to store updates produced by the Watcher so that
	// we can run the handler in a separate goroutine and still provide the
	// serial semantics.
	errCh := make(chan error)
	frontierAdvanced := make(chan struct{})
	buf := newBuffer(int(s.bufferMemLimit/sqlWatcherBufferEntrySize), startTS)
	onFrontierAdvance := func(ctx context.Context, rangefeed rangefeedKind, timestamp hlc.Timestamp) {
		buf.advance(rangefeed, timestamp)
		select {
		case <-ctx.Done():
			// The context is canceled when the rangefeed is being closed, which
			// happens after we've stopped listening on the frontierAdvancedCh.
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
			return buf.add(event)
		}()
		if err != nil {
			log.Warningf(ctx, "error adding event %v: %v", event, err)
			select {
			case <-ctx.Done():
				// The context is canceled when the rangefeed is being closed, which
				// happens after we've stopped listening on the errCh.
			case errCh <- err:
			}
		}
	}

	descriptorsRF, err := s.watchForDescriptorUpdates(ctx, startTS, onEvent, onFrontierAdvance)
	if err != nil {
		return errors.Wrapf(err, "error establishing rangefeed over system.descriptors")
	}
	defer descriptorsRF.Close()
	zonesRF, err := s.watchForZoneConfigUpdates(ctx, startTS, onEvent, onFrontierAdvance)
	if err != nil {
		return errors.Wrapf(err, "error establishing rangefeed over system.zones")
	}
	defer zonesRF.Close()

	checkpointNoops := util.Every(s.checkpointNoopsEvery)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.stopper.ShouldQuiesce():
			return nil
		case err := <-errCh:
			return err
		case <-frontierAdvanced:
			events, combinedFrontierTS, err := buf.flush(ctx)
			if err != nil {
				return err
			}
			if len(events) == 0 && !checkpointNoops.ShouldProcess(timeutil.Now()) {
				continue
			}
			if err := handler(ctx, events, combinedFrontierTS); err != nil {
				return err
			}
		}
	}
}

// watchForDescriptorUpdates establishes a rangefeed over system.descriptors and
// invokes the onEvent callback for observed events. The onFrontierAdvance
// callback is invoked whenever the rangefeed frontier is advanced as well.
func (s *SQLWatcher) watchForDescriptorUpdates(
	ctx context.Context,
	startTS hlc.Timestamp,
	onEvent func(context.Context, event),
	onFrontierAdvance func(context.Context, rangefeedKind, hlc.Timestamp),
) (*rangefeed.RangeFeed, error) {
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
			timestamp: ev.Value.Timestamp,
			update: spanconfig.DescriptorUpdate{
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
		startTS,
		handleEvent,
		rangefeed.WithDiff(),
		rangefeed.WithOnFrontierAdvance(func(ctx context.Context, resolvedTS hlc.Timestamp) {
			onFrontierAdvance(ctx, descriptorsRangefeed, resolvedTS)
		}),
	)
	if err != nil {
		return nil, err
	}

	log.Infof(ctx, "established range feed over system.descriptors table starting at time %s", startTS)
	return rf, nil
}

// watchForZoneConfigUpdates establishes a rangefeed over system.zones and
// invokes the onEvent callback whenever an event is observed. The
// onFrontierAdvance callback is also invoked whenever the rangefeed frontier is
// advanced.
func (s *SQLWatcher) watchForZoneConfigUpdates(
	ctx context.Context,
	startTS hlc.Timestamp,
	onEvent func(context.Context, event),
	onFrontierAdvance func(context.Context, rangefeedKind, hlc.Timestamp),
) (*rangefeed.RangeFeed, error) {
	zoneTableStart := s.codec.TablePrefix(keys.ZonesTableID)
	zoneTableSpan := roachpb.Span{
		Key:    zoneTableStart,
		EndKey: zoneTableStart.PrefixEnd(),
	}

	decoder := newZonesDecoder(s.codec)
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
			update: spanconfig.DescriptorUpdate{
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
		startTS,
		handleEvent,
		rangefeed.WithOnFrontierAdvance(func(ctx context.Context, resolvedTS hlc.Timestamp) {
			onFrontierAdvance(ctx, zonesRangefeed, resolvedTS)
		}),
	)
	if err != nil {
		return nil, err
	}

	log.Infof(ctx, "established range feed over system.zones table starting at time %s", startTS)
	return rf, nil
}
