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

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	execCfg          *sql.ExecutorConfig
	settings         *cluster.Settings
	rangeFeedFactory *rangefeed.Factory
	clock            *hlc.Clock
	stopper          *stop.Stopper
	knobs            *spanconfig.TestingKnobs
}

// New constructs and returns a SQLWatcher.
func New(
	codec keys.SQLCodec,
	execCfg *sql.ExecutorConfig,
	settings *cluster.Settings,
	rangeFeedFactory *rangefeed.Factory,
	clock *hlc.Clock,
	stopper *stop.Stopper,
	knobs *spanconfig.TestingKnobs,
) *SQLWatcher {
	if knobs == nil {
		knobs = &spanconfig.TestingKnobs{}
	}
	return &SQLWatcher{
		codec:            codec,
		execCfg:          execCfg,
		settings:         settings,
		rangeFeedFactory: rangeFeedFactory,
		clock:            clock,
		stopper:          stopper,
		knobs:            knobs,
	}
}

// WatchForSQLUpdates is part of the spanconfig.SQLWatcher interface.
func (s *SQLWatcher) WatchForSQLUpdates(ctx context.Context) (<-chan spanconfig.Update, error) {
	updatesCh := make(chan spanconfig.Update)

	descUpdatesCh, err := s.watchForDescriptorUpdates(ctx)
	if err != nil {
		log.Warningf(ctx, "error establishing rangefeed over system.descritpors %v", err)
		return nil, err
	}
	zonesUpdateCh, err := s.watchForZoneConfigUpdates(ctx)
	if err != nil {
		log.Warningf(ctx, "error establishing rangefeed over system.zones %v", err)
		return nil, err
	}
	if err := s.stopper.RunAsyncTask(ctx, "span-config-reconciliation", func(ctx context.Context) {
		for {
			var descID descpb.ID
			select {
			case <-ctx.Done():
				return
			case <-s.stopper.ShouldQuiesce():
				return
			case descID = <-descUpdatesCh:
			case descID = <-zonesUpdateCh:
			}
			err := s.onDescUpdate(ctx, descID, updatesCh)
			// TODO(zcfgs-pod): Is swallowing errors here the right thing to do?
			if err != nil {
				log.Errorf(ctx, "could not react to desc id %d update err: %v", descID, err)
			}
		}
	}); err != nil {
		return nil, err
	}
	return updatesCh, nil
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
//
// The rangefeed performs an initial scan over the table unless indicated
// otherwise by the SQLWatcherDisableInitialScan testing knob.
func (s *SQLWatcher) watchForDescriptorUpdates(ctx context.Context) (<-chan descpb.ID, error) {
	updatesCh := make(chan descpb.ID)
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
			log.Errorf(ctx, "error performing full reconciliation: %v", err)
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

		select {
		case <-ctx.Done():
		case updatesCh <- descID:
		}
	}
	rf, err := s.rangeFeedFactory.RangeFeed(
		ctx,
		"sql-watcher-zones-rangefeed",
		zoneTableSpan,
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

// getAllAffectedTableIDs returns a list of table IDs that need to have their
// span configurations refreshed because the provided id has changed.
func getAllAffectedTableIDs(
	ctx context.Context, id descpb.ID, txn *kv.Txn, descsCol *descs.Collection,
) (descpb.IDs, error) {
	desc, err := descsCol.GetImmutableDescriptorByID(ctx, txn, id, tree.CommonLookupFlags{
		// TODO(arul): Seems like we can't use a leased version of a descriptor here
		// if it is offline, which is why I'm passing `AvoidCached` here. Need to
		// understand what's happening here a bit better.
		AvoidCached:    true,
		IncludeDropped: true,
		IncludeOffline: true,
	})
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return descpb.IDs{id}, nil
		}
		return nil, err
	}

	// There's nothing for us to do if the descriptor is offline.
	if desc.Offline() {
		return nil, nil
	}

	if desc.DescriptorType() != catalog.Table && desc.DescriptorType() != catalog.Database {
		return nil, errors.AssertionFailedf("expected either database or table, but found descriptor of type [%s]", desc.DescriptorType())
	}

	if desc.DescriptorType() == catalog.Table {
		return descpb.IDs{id}, nil
	}

	// Now that we know the descriptor belongs to a database, the list of affected
	// table IDs is simply all tables under this database.

	// There's nothing to do here if the database has been dropped. If the
	// database was non-empty those objects will have their own rangefeed events.
	if desc.Dropped() {
		return nil, nil
	}

	tables, err := descsCol.GetAllTableDescriptorsInDatabase(ctx, txn, id)
	if err != nil {
		return nil, err
	}
	ret := make(descpb.IDs, 0, len(tables))
	for _, table := range tables {
		ret = append(ret, table.GetID())
	}
	return ret, nil
}

// onDescUpdate generates the span configurations for the given descriptor ID
// and sends them to the provided updatesCh.
func (s *SQLWatcher) onDescUpdate(
	ctx context.Context, descID descpb.ID, updatesCh chan spanconfig.Update,
) error {
	var updates []spanconfig.Update
	if err := sql.DescsTxn(
		ctx,
		s.execCfg,
		func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
			updates = nil // We're in a retryable closure.

			var err error
			var affectedIDs descpb.IDs
			if zonepb.IsNamedZoneID(descID) {
				updates, affectedIDs, err = s.handleNamedZoneUpdate(ctx, descID, txn, descsCol)
				if err != nil {
					return err
				}
			} else {
				// The ID belongs to a SQL object.
				affectedIDs, err = getAllAffectedTableIDs(ctx, descID, txn, descsCol)
				if err != nil {
					return err
				}
			}
			for _, id := range affectedIDs {
				entries, err := s.generateSpanConfigurationsForTable(ctx, txn, id)
				if err != nil {
					return err
				}
				for _, entry := range entries {
					update := spanconfig.Update{
						Entry:   entry,
						Deleted: false,
					}
					// Try to get the table descriptor regardless of DROP status. The span
					// config entry only needs to be removed when the descriptor is
					// deleted, not when it is dropped, so we set the Deleted flag only if
					// no descriptor with the given ID exists.
					_, err := descsCol.GetImmutableDescriptorByID(ctx, txn, descID, tree.CommonLookupFlags{
						AvoidCached:    true,
						IncludeDropped: true,
					})
					if errors.Is(err, catalog.ErrDescriptorNotFound) {
						update.Deleted = true
					} else if err != nil {
						return err
					}
					updates = append(updates, update)
				}
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

// generateSpanConfigurationsForTable generates the span configurations
// corresponding to the given tableID. It uses a transactional view of
// system.zones and system.descriptors to do so.
func (s *SQLWatcher) generateSpanConfigurationsForTable(
	ctx context.Context, txn *kv.Txn, id descpb.ID,
) ([]roachpb.SpanConfigEntry, error) {
	zone, err := sql.GetHydratedZoneConfigForTable(ctx, txn, s.codec, id)
	if err != nil {
		return nil, err
	}
	spanConfig := zone.AsSpanConfig()

	ret := make([]roachpb.SpanConfigEntry, 0)
	prevEndKey := s.codec.TablePrefix(uint32(id))
	for i := range zone.SubzoneSpans {
		// We need to prepend the tablePrefix to the spans stored inside the
		// SubzoneSpans field because we store the stripped version there for
		// historical reasons.
		span := roachpb.Span{
			Key:    append(s.codec.TablePrefix(uint32(id)), zone.SubzoneSpans[i].Key...),
			EndKey: append(s.codec.TablePrefix(uint32(id)), zone.SubzoneSpans[i].EndKey...),
		}

		{
			// The zone config code sets the EndKey to be nil before storing the
			// proto if it is equal to `Key.PrefixEnd()`, so we bring it back if
			// required.
			if zone.SubzoneSpans[i].EndKey == nil {
				span.EndKey = span.Key.PrefixEnd()
			}
		}

		// If there is a "hole" in the spans covered by the subzones array we fill
		// it using the parent zone configuration.
		if !prevEndKey.Equal(span.Key) {
			ret = append(ret,
				roachpb.SpanConfigEntry{
					Span:   roachpb.Span{Key: prevEndKey, EndKey: span.Key},
					Config: spanConfig,
				},
			)
		}

		// Add an entry for the subzone.
		subzoneSpanConfig := zone.Subzones[zone.SubzoneSpans[i].SubzoneIndex].Config.AsSpanConfig()
		ret = append(ret,
			roachpb.SpanConfigEntry{
				Span:   roachpb.Span{Key: span.Key, EndKey: span.EndKey},
				Config: subzoneSpanConfig,
			},
		)

		prevEndKey = span.EndKey
	}

	// If the last subzone span doesn't cover the entire table's keyspace then we
	// cover the remaining key range with the table's zone configuration.
	if !prevEndKey.Equal(s.codec.TablePrefix(uint32(id)).PrefixEnd()) {
		ret = append(ret,
			roachpb.SpanConfigEntry{
				Span:   roachpb.Span{Key: prevEndKey, EndKey: s.codec.TablePrefix(uint32(id)).PrefixEnd()},
				Config: spanConfig,
			},
		)
	}
	return ret, nil
}

// handleNamedZoneUpdate expects an ID corresponding to a NamedZone and returns
// a list of resulting updates.
// We also return a list of affected table IDs that may have had their span
// configurations changed. This is only ever the case when the named zone
// that was changed was RANGE DEFAULT. As all tables for a tenant may inherit
// from RANGE DEFAULT, any change to it affects all tables.
func (s *SQLWatcher) handleNamedZoneUpdate(
	ctx context.Context, id descpb.ID, txn *kv.Txn, descsCol *descs.Collection,
) (updates []spanconfig.Update, affectedIDs descpb.IDs, err error) {
	name, ok := zonepb.NamedZonesByID[uint32(id)]
	if !ok {
		return nil, nil, errors.AssertionFailedf("id %d does not belong to a named zone", id)
	}
	entries, err := s.generateSpanConfigsForNamedZone(ctx, txn, name)
	if err != nil {
		return nil, nil, err
	}
	for _, entry := range entries {
		updates = append(updates, spanconfig.Update{Entry: entry})
	}

	switch name {
	case zonepb.DefaultZoneName:
	default:
		return updates, affectedIDs, nil
	}

	// A change to RANGE DEFAULT may imply a change to every SQL object for the
	// tenant.
	databases, err := descsCol.GetAllDatabaseDescriptors(ctx, txn)
	if err != nil {
		return nil, nil, err
	}
	for _, dbDesc := range databases {
		tableIDs, err := getAllAffectedTableIDs(ctx, dbDesc.GetID(), txn, descsCol)
		if err != nil {
			return nil, nil, err
		}
		affectedIDs = append(affectedIDs, tableIDs...)
	}

	return updates, affectedIDs, nil
}

// generateSpanConfigsForNamedZone expects an ID that belongs to a named zone
// and generates the span configuration for that range.
func (s *SQLWatcher) generateSpanConfigsForNamedZone(
	ctx context.Context, txn *kv.Txn, name zonepb.NamedZone,
) ([]roachpb.SpanConfigEntry, error) {
	var spans []roachpb.Span
	switch name {
	case zonepb.DefaultZoneName:
		if !s.codec.ForSystemTenant() {
			// TODO(zcfgs-pod): Possibly pull this out into a named span.
			// Tenant's RANGE DEFAULT addresses the first range in the tenant's prefix.
			// We always want there to be an entry for this span in
			// `system.span_configurations` as this span is at the tenant boundary,
			// and as such, a hard split point.
			spans = append(spans, roachpb.Span{
				Key:    s.codec.TenantPrefix(),
				EndKey: s.codec.TenantPrefix().Next(),
			})
		}

	case zonepb.MetaZoneName:
		spans = append(spans, roachpb.Span{Key: keys.Meta1Span.Key, EndKey: keys.NodeLivenessSpan.Key})
	case zonepb.LivenessZoneName:
		spans = append(spans, keys.NodeLivenessSpan)
	case zonepb.TimeseriesZoneName:
		spans = append(spans, keys.TimeseriesSpan)
	case zonepb.SystemZoneName:
		// Add spans for the system range without the timeseries and
		// liveness ranges, which are individually captured above.
		//
		// Note that the NodeLivenessSpan sorts before the rest of the system
		// keyspace, so the first span here starts at the end of the
		// NodeLivenessSpan.
		spans = append(spans, roachpb.Span{
			Key:    keys.NodeLivenessSpan.EndKey,
			EndKey: keys.TimeseriesSpan.Key,
		})
		spans = append(spans, roachpb.Span{
			Key:    keys.TimeseriesSpan.EndKey,
			EndKey: keys.SystemMax,
		})
	case zonepb.TenantsZoneName: // nothing to do
	default:
		return nil, errors.AssertionFailedf("unknown named zone config %s", name)
	}

	zone, err := sql.GetZoneConfigForNamedZone(ctx, txn, s.codec, name)
	if err != nil {
		return nil, err
	}
	spanConfig := zone.AsSpanConfig()

	var entries []roachpb.SpanConfigEntry
	for _, span := range spans {
		entries = append(entries, roachpb.SpanConfigEntry{
			Span:   span,
			Config: spanConfig,
		})
	}
	return entries, nil
}
