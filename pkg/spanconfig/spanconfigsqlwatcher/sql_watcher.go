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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
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
	db               *kv.DB
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
	db *kv.DB,
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
		db:               db,
		settings:         settings,
		rangeFeedFactory: rangeFeedFactory,
		clock:            clock,
		ie:               ie,
		leaseManager:     leaseManager,
		stopper:          stopper,
		knobs:            knobs,
	}
}

// WatchForSQLUpdates is part of the spanconfig.SQLWatcher interface.
func (s *SQLWatcher) WatchForSQLUpdates(ctx context.Context) (<-chan spanconfig.Update, error) {
	descUpdatesCh, err := s.watchForDescriptorUpdates(ctx)
	if err != nil {
		log.Warningf(ctx, "error establishing rangefeed over system.descriptors %v", err)
		return nil, err
	}
	zonesUpdateCh, err := s.watchForZoneConfigUpdates(ctx)
	if err != nil {
		log.Warningf(ctx, "error establishing rangefeed over system.zones %v", err)
		return nil, err
	}

	updatesCh := make(chan spanconfig.Update)
	if err := s.stopper.RunAsyncTask(ctx, "span-config-reconciliation", func(ctx context.Context) {
		for {
			var update descIDAndType
			select {
			case <-ctx.Done():
				return
			case <-s.stopper.ShouldQuiesce():
				return
			case update = <-descUpdatesCh:
			case update = <-zonesUpdateCh:
			}
			// TODO(zcfgs-pod): Is swallowing errors here the right thing to do or
			// do we want something different? Here and below.
			if err := s.onDescUpdate(ctx, update, updatesCh); err != nil {
				log.Errorf(ctx, "could not react to update for ID %d: %v", update.ID, err)
			}
		}
	}); err != nil {
		return nil, err
	}
	return updatesCh, nil
}

type descIDAndType struct {
	descpb.ID
	catalog.DescriptorType
}

// watchForDescriptorUpdates establishes a rangefeed over system.descriptors and
// sends updates on the returned channel. The rangefeed performs an initial scan
// over the table unless indicated otherwise by the
// SQLWatcherDisableInitialScan testing knob.
func (s *SQLWatcher) watchForDescriptorUpdates(ctx context.Context) (<-chan descIDAndType, error) {
	updatesCh := make(chan descIDAndType)
	descriptorTableStart := s.codec.TablePrefix(keys.DescriptorTableID)
	descriptorTableSpan := roachpb.Span{
		Key:    descriptorTableStart,
		EndKey: descriptorTableStart.PrefixEnd(),
	}
	handleEvent := func(ctx context.Context, ev *roachpb.RangeFeedValue) {
		value := ev.Value
		if !ev.Value.IsPresent() { // the descriptor was deleted
			value = ev.PrevValue
		}

		if !value.IsPresent() {
			return // TODO(zcfgs-pod): Is this expected?
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
			return // TODO(zcfgs-pod): Is this expected?
		}

		table, database, _, _ := descpb.FromDescriptorWithMVCCTimestamp(&descriptor, ev.Value.Timestamp)
		if table == nil && database == nil {
			return // we only care about database or table descriptors being updated
		}

		var d descIDAndType
		if table != nil {
			d = descIDAndType{table.GetID(), catalog.Table}
		} else {
			d = descIDAndType{database.GetID(), catalog.Database}
		}

		select {
		case <-ctx.Done():
		case updatesCh <- d:
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
func (s *SQLWatcher) watchForZoneConfigUpdates(ctx context.Context) (<-chan descIDAndType, error) {
	updatesCh := make(chan descIDAndType)

	zoneTableStart := s.codec.TablePrefix(keys.ZonesTableID)
	zoneTableSpan := roachpb.Span{
		Key:    zoneTableStart,
		EndKey: zoneTableStart.PrefixEnd(),
	}

	decoder := newZonesDecoder(s.codec)
	handleEvent := func(ctx context.Context, ev *roachpb.RangeFeedValue) {
		descID, err := decoder.decodePrimaryKey(ev.Key)
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
		case updatesCh <- descIDAndType{descID, catalog.Any}:
		}
	}
	rf, err := s.rangeFeedFactory.RangeFeed(
		ctx, "sql-watcher-zones-rangefeed", zoneTableSpan, s.clock.Now(), handleEvent,
		rangefeed.WithInitialScan(nil),
		rangefeed.WithOnInitialScanError(func(ctx context.Context, err error) (shouldFail bool) {
			// TODO(zcfgs-pod): Are there errors that should prevent us from
			// retrying again? The setting watcher considers grpc auth errors as
			// permanent.
			return false
		}),
	)
	if err != nil {
		return nil, err
	}
	s.stopper.AddCloser(rf)

	log.Infof(ctx, "established range feed over system.zones table")
	return updatesCh, nil
}

// onDescUpdate generates the span configurations for the given descriptor ID
// and sends them over the provided updatesCh.
func (s *SQLWatcher) onDescUpdate(
	ctx context.Context,
	updated descIDAndType,
	updatesCh chan spanconfig.Update,
) error {
	var updates []spanconfig.Update
	if err := descs.Txn(ctx, s.settings, s.leaseManager, s.ie, s.db,
		func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
			updates = nil // we're in a retryable closure

			if _, ok := zonepb.NamedZonesByID[uint32(updated.ID)]; ok { // named zone
				entries, err := generateSpanConfigsForNamedZone(ctx, txn, s.codec, updated.ID)
				if err != nil {
					return err
				}
				for _, entry := range entries {
					update := spanconfig.Update{Entry: entry}
					updates = append(updates, update)
				}

				return nil
			}

			desc, found, err := exists(ctx, txn, updated.ID, descsCol)
			if err != nil {
				return err
			}

			if !found {
				if updated.DescriptorType == catalog.Database {
					// We only care about tables being dropped. For dropped
					// databases, we'd have seen events for each table.
					return nil
				}

				tablePrefix := s.codec.TablePrefix(uint32(updated.ID))

				update := spanconfig.Update{
					Entry: roachpb.SpanConfigEntry{
						Span: roachpb.Span{
							Key:    tablePrefix,
							EndKey: tablePrefix.PrefixEnd(),
						},
					},
					Deleted: true,
				}
				updates = append(updates, update)
				return nil
			}

			if descType := desc.DescriptorType(); descType != catalog.Table && descType != catalog.Database {
				return errors.AssertionFailedf("expected database or table, found descriptor of type %s", descType)
			}

			// There's nothing for us to do if the descriptor is offline or
			// dropped. We're waiting for the descriptor to be deleted
			// entirely. For non-empty databases that are dropped, inner
			// objects will have had their own rangefeed events.
			//
			// TODO(zcfgs-pod): If a table is dropped, do we still want zone
			// configs for the database to affect its ranges? Ditto for dropped
			// tables?
			if desc.Offline() || desc.Dropped() {
				return nil
			}

			var tableIDs descpb.IDs
			if desc.DescriptorType() == catalog.Table {
				tableIDs = append(tableIDs, updated.ID)
			} else {
				if updated.ID == keys.SystemDatabaseID {
					// We still want to target these pseudo ID ranges, lest they
					// be entirely unaddressable.
					for _, pseudoTableID := range keys.PseudoTableIDs {
						tableIDs = append(tableIDs, descpb.ID(pseudoTableID))
					}
				}

				tables, err := descsCol.GetAllTableDescriptorsInDatabase(ctx, txn, updated.ID)
				if err != nil {
					return err
				}

				for _, table := range tables {
					tableIDs = append(tableIDs, table.GetID())
				}
			}

			for _, tableID := range tableIDs {
				zoneConfig, err := sql.GetHydratedZoneConfigForTable(ctx, txn, s.codec, tableID)
				if err != nil {
					return err
				}

				entries, err := generateSpanConfigsForTable(s.codec, tableID, zoneConfig)
				if err != nil {
					return err
				}

				for _, entry := range entries {
					update := spanconfig.Update{Entry: entry}
					updates = append(updates, update)
				}
			}
			return nil
		},
	); err != nil {
		return err
	}

	// Send forth the updates on the given channel.
	for _, update := range updates {
		select {
		case <-s.stopper.ShouldQuiesce():
		case <-ctx.Done():
		case updatesCh <- update:
		}
	}
	return nil
}

func generateSpanConfigsForNamedZone(ctx context.Context, txn *kv.Txn, codec keys.SQLCodec, descID descpb.ID) ([]roachpb.SpanConfigEntry, error) {
	name := zonepb.NamedZonesByID[uint32(descID)]

	var spans []roachpb.Span
	switch name {
	case zonepb.DefaultZoneName:
		if !codec.ForSystemTenant() {
			// Tenant's RANGE DEFAULT addresses from start of tenant prefix
			// to first system table, and from after the last table's data
			// to the end of the tenant's keyspace.
			//
			// TODO(zcfgs-pod): Should this be part of the DATABASE system
			// instead?
			spans = append(spans, roachpb.Span{
				Key:    codec.TenantPrefix(),
				EndKey: codec.TablePrefix(keys.DescriptorTableID),
			})

			// Let's also hijack the tenant's range default to take over the
			// unaddressable part after their last table.
			//
			// XXX: There's an impedance mismatch. Intuitively I'd imagine range
			// default to target everything, including otherwise unaddressable
			// keyspans. But with us spelling out how RANGE DEFAULT decomposes,
			// we have to make sure we address everything not otherwise covered.
			// This is stuff like the residue after the max table data. This is
			// stuff like the timeseries range absent something more specific.
			// Are there other instances of this? For the timeseries data, we
			// could introduce an explicit migration writing the tsd zone config
			// if not explicitly set.
			// If keyspans don't have a span config set, they're allwed to take
			// up any config. Likely they'll get merged into adjacent configs.
			maxDescIDKeyVal, err := txn.Get(ctx, codec.DescIDSequenceKey())
			if err != nil {
				return nil, err
			}
			maxDescID, err := maxDescIDKeyVal.Value.GetInt()
			if err != nil {
				return nil, err
			}

			spans = append(spans, roachpb.Span{
				Key:    codec.TablePrefix(uint32(maxDescID)).PrefixEnd(),
				EndKey: codec.TenantPrefix().PrefixEnd(),
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
		panic("unexpected")
	}

	zone, err := sql.GetHydratedZoneConfigForTable(ctx, txn, codec, descID)
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

func exists(ctx context.Context, txn *kv.Txn, id descpb.ID, descsCol *descs.Collection) (desc catalog.Descriptor, found bool, _ error) {
	desc, err := descsCol.GetImmutableDescriptorByID(ctx, txn, id, tree.CommonLookupFlags{
		// TODO(zcfgs-pod): Seems like we can't use a leased version of the
		// descriptor if it is offline. I'm not sure if there's special
		// interaction between leasing and offline descriptors, but come back to
		// this.
		AvoidCached: true,
		// TODO(zcfgs-pod): Do we need these if we're simply ignoring it below?
		// Do we want to include dropped tables here? How do zone configs behave
		// today for dropped tables, if they haven't been cleaned up.
		IncludeDropped: true,
		IncludeOffline: true,
	})
	if errors.Is(err, catalog.ErrDescriptorNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return desc, true, nil
}

// generateSpanConfigsForTable generates the span configurations
// corresponding to the given tableID. It uses a transactional view of
// system.zones and system.descriptors to do so.
func generateSpanConfigsForTable(
	codec keys.SQLCodec, tableID descpb.ID, zone *zonepb.ZoneConfig,
) ([]roachpb.SpanConfigEntry, error) {
	spanConfig := zone.AsSpanConfig()
	tablePrefix := codec.TablePrefix(uint32(tableID))
	prevEndKey := tablePrefix

	var entries []roachpb.SpanConfigEntry
	for _, subzoneSpan := range zone.SubzoneSpans {
		// We need to prepend the tablePrefix to the spans stored inside the
		// SubzoneSpans field because we store the stripped version there for
		// historical reasons.
		startKey, endKey := subzoneSpan.Key, subzoneSpan.EndKey
		subSpan := roachpb.Span{
			Key:    append(codec.TablePrefix(uint32(tableID)), startKey...), // can't simply re-use tablePrefix; append can write next to the slice underneath
			EndKey: append(codec.TablePrefix(uint32(tableID)), endKey...),
		}

		// The zone config code sets the EndKey to be nil before storing the
		// proto if it is equal to `Key.PrefixEnd()`, so we bring it back if
		// required.
		if subzoneSpan.EndKey == nil {
			subSpan.EndKey = subSpan.Key.PrefixEnd()
		}

		// If there is a "hole" in the spans covered by the subzones array we
		// fill it using the parent zone configuration.
		if !prevEndKey.Equal(subSpan.Key) {
			entries = append(entries, roachpb.SpanConfigEntry{
				Span:   roachpb.Span{Key: prevEndKey, EndKey: subSpan.Key},
				Config: spanConfig,
			})
		}

		// Add an entry for the subzone.
		subzoneConfig := zone.Subzones[subzoneSpan.SubzoneIndex].Config
		entries = append(entries, roachpb.SpanConfigEntry{
			Span:   subSpan,
			Config: subzoneConfig.AsSpanConfig(),
		})
		prevEndKey = subSpan.EndKey
	}

	// If the last subzone span doesn't cover the entire table's keyspace then
	// we cover the remaining key range with the table's zone configuration.
	if tableEnd := tablePrefix.PrefixEnd(); !prevEndKey.Equal(tableEnd) {
		entries = append(entries, roachpb.SpanConfigEntry{
			Span:   roachpb.Span{Key: prevEndKey, EndKey: tableEnd},
			Config: spanConfig,
		})
	}

	return entries, nil
}
