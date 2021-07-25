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
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

// SQLWatcher implements the spanconfig.SQLWatcher interface.
var _ spanconfig.SQLWatcher = &SQLWatcher{}

// The SQLWatcher is in charge of watching for specific SQL events and reacting
// to them by constructing the list of span configurations implied as a result.
// It establishes a rangefeed over system.zones and system.descriptor to do, as
// changes to these tables may result in changes to the implied span
// configurations for a tenant.
//
// When the SQLWatcher learns about a particular descriptor or zone
// configuration  update, it constructs and sends the list of implied span
// configurations on the channel returned by the Watch, which is the
// orchestrating method for this whole process.
type SQLWatcher struct {
	codec            keys.SQLCodec
	DB               *kv.DB
	settings         *cluster.Settings
	rangeFeedFactory *rangefeed.Factory
	clock            *hlc.Clock
	ie               sqlutil.InternalExecutor
	leaseManager     *lease.Manager
	stopper          *stop.Stopper
	knobs            *spanconfig.TestingKnobs
}

// New constructs and returns a SQLWatcher.
func New(
	codec keys.SQLCodec,
	DB *kv.DB,
	settings *cluster.Settings,
	rangeFeedFactory *rangefeed.Factory,
	clock *hlc.Clock,
	ie sqlutil.InternalExecutor,
	leaseManager *lease.Manager,
	stopper *stop.Stopper,
	knobs *spanconfig.TestingKnobs,
) *SQLWatcher {
	if knobs == nil {
		knobs = &spanconfig.TestingKnobs{}
	}
	return &SQLWatcher{
		codec:            codec,
		DB:               DB,
		settings:         settings,
		rangeFeedFactory: rangeFeedFactory,
		clock:            clock,
		ie:               ie,
		leaseManager:     leaseManager,
		stopper:          stopper,
		knobs:            knobs,
	}
}

// Watch is the entry point for the SQLWatcher.
func (s *SQLWatcher) Watch(ctx context.Context) (<-chan spanconfig.Update, error) {
	updatesCh := make(chan spanconfig.Update)

	descUpdatesCh, err := s.watchForDescriptorUpdates(ctx)
	zonesUpdateCh, err := s.watchForZoneConfigUpdates(ctx)
	if err != nil {
		log.Warningf(ctx, "error establishing rangefeed over system.descritpors %v", err)
		return nil, err
	}
	if err := s.stopper.RunAsyncTask(ctx, "span-config-reconciliation", func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				return
			case <-s.stopper.ShouldQuiesce():
				return
			case descID := <-descUpdatesCh:
				err := s.onDescIDUpdate(ctx, descID, updatesCh)
				if err != nil {
					log.Errorf(ctx, "could not react to desc id %d update err: %v", descID, err)
				}
			case descID := <-zonesUpdateCh:
				err := s.onDescIDUpdate(ctx, descID, updatesCh)
				if err != nil {
					log.Errorf(ctx, "could not react to zone config update with id %d err: %v", descID, err)
				}
			}
		}
	}); err != nil {
		return nil, err
	}
	return updatesCh, nil
}

// watchForDescriptorUpdates establishes a rangefeed over system.descriptors and
// sends updates on the returned channel. The rangefeed performs an initial scan
// over the table unless indicated otherwise by the
// SQLWatcherDisableInitialScan testing knob.
func (s *SQLWatcher) watchForDescriptorUpdates(ctx context.Context) (<-chan descpb.ID, error) {
	updatesCh := make(chan descpb.ID)
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
		}
		if descriptor.Union == nil {
			return
		}

		table, database, _, _ := descpb.FromDescriptorWithMVCCTimestamp(&descriptor, ev.Value.Timestamp)

		var id descpb.ID
		if table != nil {
			id = table.GetID()
		} else if database != nil {
			id = database.GetID()
		} else {
			// We only care about database or table descriptors being updated.
			return
		}

		select {
		case <-ctx.Done():
		case updatesCh <- id:
		}
	}
	opts := []rangefeed.Option{
		rangefeed.WithDiff(),
	}
	if !s.knobs.SQLWatcherDisableInitialScan {
		opts = append(opts, rangefeed.WithInitialScan(nil))
		opts = append(opts, rangefeed.WithOnInitialScanError(func(ctx context.Context, err error) bool {
			logcrash.ReportOrPanic(
				ctx,
				&s.settings.SV,
				"error performing full reconciliation: %v",
				err,
			)
			return false
		}))
	}
	rf, err := s.rangeFeedFactory.RangeFeed(
		ctx,
		"sql-watcher-descriptor-rangefeed",
		descriptorTableSpan,
		s.clock.Now(),
		handleEvent,
		opts...,
	)
	if err != nil {
		return nil, err
	}
	s.stopper.AddCloser(rf)

	log.Infof(ctx, "established range feed over system.descriptors table")
	return updatesCh, nil
}

// watchForZoneConfigUpdates establishes a rangefeed over system.zones and sends
// updates on the returned channel.
func (s *SQLWatcher) watchForZoneConfigUpdates(ctx context.Context) (<-chan descpb.ID, error) {
	updatesCh := make(chan descpb.ID)

	zoneTableStart := s.codec.TablePrefix(keys.ZonesTableID)
	descriptorTableSpan := roachpb.Span{
		Key:    zoneTableStart,
		EndKey: zoneTableStart.PrefixEnd(),
	}

	handleEvent := func(ctx context.Context, ev *roachpb.RangeFeedValue) {
		var descID descpb.ID
		{
			// Decode the descriptor ID from the key.
			tbl := systemschema.ZonesTable
			types := []*types.T{tbl.PublicColumns()[0].GetType()}
			startKeyRow := make([]rowenc.EncDatum, 1)
			_, matches, _, err := rowenc.DecodeIndexKey(
				s.codec, tbl, tbl.GetPrimaryIndex(),
				types, startKeyRow, nil, ev.Key,
			)
			if err != nil || !matches {
				logcrash.ReportOrPanic(
					ctx,
					&s.settings.SV,
					"failed to decode key in system.zones: %v",
					ev.Key,
				)
				return
			}
			alloc := &rowenc.DatumAlloc{}
			if err := startKeyRow[0].EnsureDecoded(types[0], alloc); err != nil {
				logcrash.ReportOrPanic(
					ctx,
					&s.settings.SV,
					"failed to decode key in system.zones: %v",
					ev.Key,
				)
				return
			}
			descID = descpb.ID(tree.MustBeDInt(startKeyRow[0].Datum))
		}

		select {
		case <-ctx.Done():
		case updatesCh <- descID:
		}
	}
	rf, err := s.rangeFeedFactory.RangeFeed(
		ctx,
		"sql-watcher-zones-rangefeed",
		descriptorTableSpan,
		s.clock.Now(),
		handleEvent,
	)
	if err != nil {
		return nil, err
	}
	s.stopper.AddCloser(rf)

	log.Infof(ctx, "established range feed over system.zones table")
	return updatesCh, nil
}

// getAllAffectedDescriptors returns a list of IDs that need to have their span
// configurations refreshed because the given id changed.
func getAllAffectedIDs(
	ctx context.Context, id descpb.ID, txn *kv.Txn, descsCol *descs.Collection,
) (descpb.IDs, error) {
	desc, err := descsCol.GetImmutableDescriptorByID(ctx, txn, id, tree.CommonLookupFlags{
		IncludeDropped: true,
	})
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return descpb.IDs{id}, nil
		}
		return nil, err
	}
	switch desc.DescriptorType() {
	case catalog.Table:
		return descpb.IDs{id}, nil
	case catalog.Database:
	default:
		return nil, errors.AssertionFailedf("expected either database or table, but found descriptor of type [%s]", desc.DescriptorType())
	}
	// There's nothing to do here if the database has been dropped. If the
	// database was non-empty those objects will have their own rangefeed events.
	if desc.Dropped() {
		return nil, nil
	}
	descriptors, err := descsCol.GetAllDescriptorsInDatabase(ctx, txn, id)
	if err != nil {
		return nil, err
	}
	ret := make(descpb.IDs, 0, len(descriptors))
	for _, desc := range descriptors {
		ret = append(ret, desc.GetID())
	}
	return ret, nil
}

// onDescIDUpdate generates the span configurations for the given descriptor ID
// and sends them to the provided updatesCh.
func (s *SQLWatcher) onDescIDUpdate(
	ctx context.Context, descID descpb.ID, updatesCh chan spanconfig.Update,
) error {
	var updates []spanconfig.Update
	if err := descs.Txn(
		ctx,
		s.settings,
		s.leaseManager,
		s.ie,
		s.DB,
		func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
			affectedIDs, err := getAllAffectedIDs(ctx, descID, txn, descsCol)
			if err != nil {
				return err
			}
			entries, err := s.generateSpanConfigurations(ctx, txn, affectedIDs)
			if err != nil {
				return err
			}
			for _, entry := range entries {
				update := spanconfig.Update{
					Entry:   entry,
					Deleted: false,
				}
				{
					// Try to get the table descriptor regardless of DROP status. The span
					// config entry only needs to be removed when the descriptor is
					// deleted, not when it is dropped, so we set the Deleted flag only if
					// no descriptor with the given ID exists.
					_, err := descsCol.GetImmutableDescriptorByID(ctx, txn, descID, tree.CommonLookupFlags{
						AvoidCached:    true,
						IncludeDropped: true,
					})
					if err != nil {
						if errors.Is(err, catalog.ErrDescriptorNotFound) {
							update.Deleted = true
						} else {
							return err
						}
					}
				}
				updates = append(updates, update)
			}
			return nil
		}); err != nil {
		return err
	}

	// Send all the entries on the updatesCh.
	for _, update := range updates {
		select {
		case <-s.stopper.ShouldQuiesce():
		case <-ctx.Done():
		case updatesCh <- update:
		}
	}
	return nil
}

// generateSpanConfigurations generates the span configurations corresponding to
// the provided list of IDs. It uses a transactional view of system.zones and
// system.descriptors to do so.
func (s *SQLWatcher) generateSpanConfigurations(
	ctx context.Context, txn *kv.Txn, ids descpb.IDs,
) ([]roachpb.SpanConfigEntry, error) {
	copyKey := func(k roachpb.Key) roachpb.Key {
		k2 := make([]byte, len(k))
		copy(k2, k)
		return k2
	}

	ret := make([]roachpb.SpanConfigEntry, 0, len(ids))
	for _, id := range ids {
		zone, err := sql.GetHydratedZoneConfigForTable(ctx, txn, s.codec, id)
		if err != nil {
			return nil, err
		}
		spanConfig, err := zone.ToSpanConfig()
		if err != nil {
			return nil, err
		}

		tablePrefix := s.codec.TablePrefix(uint32(id))
		prev := tablePrefix
		for i := range zone.SubzoneSpans {
			// We need to prepend the tablePrefix to the spans stored inside the
			// SubzoneSpans field because we store the stripped version here for
			// historical reasons.
			span := roachpb.Span{
				Key:    append(tablePrefix, zone.SubzoneSpans[i].Key...),
				EndKey: append(tablePrefix, zone.SubzoneSpans[i].EndKey...),
			}

			{
				// The zone config code sets the EndKey to be nil before storing the
				// proto if it is equal to `Key.PrefixEnd()`, so we bring it back if we
				// required.
				if zone.SubzoneSpans[i].EndKey == nil {
					span.EndKey = span.Key.PrefixEnd()
				}
			}

			// If there is a "hole" in the spans covered by the subzones array we fill
			// it using the parent zone configuration.
			if !prev.Equal(span.Key) {
				ret = append(ret,
					roachpb.SpanConfigEntry{
						Span:   roachpb.Span{Key: copyKey(prev), EndKey: copyKey(span.Key)},
						Config: spanConfig,
					},
				)
			}

			// Add an entry for the subzone.
			subzoneSpanConfig, err := zone.Subzones[zone.SubzoneSpans[i].SubzoneIndex].Config.ToSpanConfig()
			if err != nil {
				return nil, err
			}
			ret = append(ret,
				roachpb.SpanConfigEntry{
					Span:   roachpb.Span{Key: copyKey(span.Key), EndKey: copyKey(span.EndKey)},
					Config: subzoneSpanConfig,
				},
			)

			prev = copyKey(span.EndKey)
		}

		if !prev.Equal(tablePrefix.PrefixEnd()) {
			ret = append(ret,
				roachpb.SpanConfigEntry{
					Span:   roachpb.Span{Key: prev, EndKey: tablePrefix.PrefixEnd()},
					Config: spanConfig,
				},
			)
		}
	}
	return ret, nil
}
