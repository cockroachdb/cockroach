// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigsqlreconciler

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// SQLReconciler implements the spanconfig.SQLReconciler interface.
var _ spanconfig.SQLReconciler = &SQLReconciler{}

// SQLReconciler is the concrete implementation of spanconfig.SQLReconciler.
type SQLReconciler struct {
	execCfg *sql.ExecutorConfig
	codec   keys.SQLCodec
}

// New constructs and returns a SQLReconciler.
func New(execCfg *sql.ExecutorConfig, codec keys.SQLCodec) *SQLReconciler {
	return &SQLReconciler{
		execCfg: execCfg,
		codec:   codec,
	}
}

// FullReconcile is part of the spanconfig.SQLReconciler interface.
func (s *SQLReconciler) FullReconcile(
	ctx context.Context,
) (updates []spanconfig.Update, timestamp hlc.Timestamp, err error) {
	// As RANGE DEFAULT is the root of all zone configurations (including
	// other named zones for the system tenant), we can construct the entire
	// span configuration state by starting from RANGE DEFAULT.
	return s.reconcile(ctx, descpb.IDs{keys.RootNamespaceID})
}

// Reconcile is part of the spanconfig.SQLReconciler interface.
func (s *SQLReconciler) Reconcile(
	ctx context.Context, ids descpb.IDs,
) ([]spanconfig.Update, error) {
	updates, _, err := s.reconcile(ctx, ids)
	return updates, err
}

func (s *SQLReconciler) reconcile(
	ctx context.Context, ids descpb.IDs,
) (updates []spanconfig.Update, timestamp hlc.Timestamp, err error) {
	err = sql.DescsTxn(
		ctx, s.execCfg, func(
			ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
		) error {
			for _, id := range ids {
				idUpdates, err := s.reconcileID(ctx, id, txn, descsCol)
				if err != nil {
					return err
				}
				updates = append(updates, idUpdates...)
			}
			// TODO(arul): Looks like the use of this method means the transactions
			// timestamp can't be pushed. Confirm that we're okay with this, but I'm
			// not sure if there's a better way to do what we want to do.
			timestamp = txn.CommitTimestamp()
			return nil
		})
	return updates, timestamp, err
}

func (s *SQLReconciler) reconcileID(
	ctx context.Context, id descpb.ID, txn *kv.Txn, descsCol *descs.Collection,
) (updates []spanconfig.Update, err error) {
	var affectedIDs descpb.IDs
	if zonepb.IsNamedZoneID(id) {
		updates, affectedIDs, err = s.reconcileNamedZoneID(ctx, id, txn, descsCol)
		if err != nil {
			return nil, err
		}
	} else {
		// We're dealing with a SQL Object.
		updates, affectedIDs, err = s.reconcileDescriptorID(ctx, id, txn, descsCol)
		if err != nil {
			return nil, err
		}
	}

	for _, affectedID := range affectedIDs {
		affectedIDUpates, err := s.reconcileID(ctx, affectedID, txn, descsCol)
		if err != nil {
			return nil, err
		}
		updates = append(updates, affectedIDUpates...)
	}
	return updates, nil
}

func (s *SQLReconciler) reconcileDescriptorID(
	ctx context.Context, id descpb.ID, txn *kv.Txn, descsCol *descs.Collection,
) (updates []spanconfig.Update, affectedIDs descpb.IDs, err error) {
	var desc catalog.Descriptor
	affectedIDs, desc, err = getAllAffectedTableIDs(ctx, id, txn, descsCol)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			// If the descriptor has been deleted then we want to delete the span
			// config entry. For our delete update the previous config doesn't matter,
			// so we can safely omit it.
			// TODO(arul): With the way the rangefeed is set up we're guaranteed that
			// this deleted ID belongs to a table. This isn't satisfying, but even if
			// this were not a table ID this wouldn't be a correctness issue.
			update := spanconfig.Update{
				Entry: roachpb.SpanConfigEntry{
					Span: roachpb.Span{
						Key:    s.codec.TablePrefix(uint32(id)),
						EndKey: s.codec.TablePrefix(uint32(id)).PrefixEnd(),
					},
				},
				Deleted: true,
			}
			return []spanconfig.Update{update}, nil, nil
		}
		return nil, nil, err
	}

	// We should only ever have a database or table descriptor at this point.
	if desc.DescriptorType() != catalog.Table && desc.DescriptorType() != catalog.Database {
		return nil, nil, errors.AssertionFailedf("expected either database or table, but found descriptor of type [%s]", desc.DescriptorType())
	}

	// If the descriptor is that of a table then we simply generate all span
	// configs.
	if desc.DescriptorType() == catalog.Table {
		entries, err := s.generateSpanConfigurationsForTable(ctx, txn, desc)
		if err != nil {
			return nil, nil, err
		}

		for _, entry := range entries {
			updates = append(updates, spanconfig.Update{
				Entry:   entry,
				Deleted: false,
			})
		}

		return updates, nil, nil
	}

	// This ID belongs to a database. There aren't any updates to generate for
	// the database itself, but we have the list of affected IDs which must be
	// returned to the caller.
	return nil, affectedIDs, nil
}

// handleNamedZoneUpdate expects an ID corresponding to a NamedZone and returns
// a list of resulting updates. It also returns a list of affected (table) IDs
// that may have had their span configurations changed because of our
// inheritance semantics. This is only ever the case when the named zone that
// was changed was RANGE DEFAULT.
func (s *SQLReconciler) reconcileNamedZoneID(
	ctx context.Context, id descpb.ID, txn *kv.Txn, descsCol *descs.Collection,
) (updates []spanconfig.Update, affectedIDs descpb.IDs, err error) {
	name, ok := zonepb.NamedZonesByID[uint32(id)]
	if !ok {
		return nil, nil, errors.AssertionFailedf("id %d does not belong to a named zone", id)
	}

	// Named zones other than RANGE DEFAULT are not a thing for secondary tenants.
	if !s.codec.ForSystemTenant() && name != zonepb.DefaultZoneName {
		return nil,
			nil,
			errors.AssertionFailedf("secondary tenants do not have the notion of %s named zone", name)
	}

	entries, err := s.generateSpanConfigurationsForNamedZone(ctx, txn, name)
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
		tableIDs, _, err := getAllAffectedTableIDs(ctx, dbDesc.GetID(), txn, descsCol)
		if err != nil {
			return nil, nil, err
		}
		affectedIDs = append(affectedIDs, tableIDs...)
	}

	// As all named zones also inherit from RANGE DEFAULT, a change to it may also
	// imply a change to every other named zone. This is only a thing for the
	// host tenant as named zones other than RANGE DEFAULT don't exist for
	// secondary tenants.
	if s.codec.ForSystemTenant() {
		for _, namedZone := range zonepb.NamedZonesList {
			// Add an entry for all named zones bar RANGE DEFAULT.
			if namedZone != zonepb.DefaultZoneName {
				affectedIDs = append(affectedIDs, descpb.ID(zonepb.NamedZones[namedZone]))
			}
		}
	}

	return updates, affectedIDs, nil
}

// generateSpanConfigurationsForNamedZone takes in a named zone and generates
// the span configuration for that range.
func (s *SQLReconciler) generateSpanConfigurationsForNamedZone(
	ctx context.Context, txn *kv.Txn, name zonepb.NamedZone,
) ([]roachpb.SpanConfigEntry, error) {
	var spans []roachpb.Span
	switch name {
	case zonepb.DefaultZoneName:
		if !s.codec.ForSystemTenant() {
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

	zone, err := sql.GetHydratedZoneConfigForNamedZone(ctx, txn, s.codec, name)
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

// generateSpanConfigurationsForTable generates the span configurations
// corresponding to the given tableID. It uses a transactional view of
// system.zones and system.descriptors to do so.
func (s *SQLReconciler) generateSpanConfigurationsForTable(
	ctx context.Context, txn *kv.Txn, desc catalog.Descriptor,
) ([]roachpb.SpanConfigEntry, error) {
	if desc.DescriptorType() != catalog.Table {
		return nil, errors.AssertionFailedf(
			"expected table descriptor, but got descriptor of type %s", desc.DescriptorType(),
		)
	}
	zone, err := sql.GetHydratedZoneConfigForTable(ctx, txn, s.codec, desc.GetID())
	if err != nil {
		return nil, err
	}
	spanConfig := zone.AsSpanConfig()

	ret := make([]roachpb.SpanConfigEntry, 0)
	prevEndKey := s.codec.TablePrefix(uint32(desc.GetID()))
	for i := range zone.SubzoneSpans {
		// We need to prepend the tablePrefix to the spans stored inside the
		// SubzoneSpans field because we store the stripped version there for
		// historical reasons.
		span := roachpb.Span{
			Key:    append(s.codec.TablePrefix(uint32(desc.GetID())), zone.SubzoneSpans[i].Key...),
			EndKey: append(s.codec.TablePrefix(uint32(desc.GetID())), zone.SubzoneSpans[i].EndKey...),
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
	if !prevEndKey.Equal(s.codec.TablePrefix(uint32(desc.GetID())).PrefixEnd()) {
		ret = append(ret,
			roachpb.SpanConfigEntry{
				Span:   roachpb.Span{Key: prevEndKey, EndKey: s.codec.TablePrefix(uint32(desc.GetID())).PrefixEnd()},
				Config: spanConfig,
			},
		)
	}
	return ret, nil
}

// getAllAffectedTableIDs returns a list of table IDs that need to have their
// span configurations refreshed because the provided id has changed.
func getAllAffectedTableIDs(
	ctx context.Context, id descpb.ID, txn *kv.Txn, descsCol *descs.Collection,
) (descpb.IDs, catalog.Descriptor, error) {
	desc, err := descsCol.GetImmutableDescriptorByID(ctx, txn, id, tree.CommonLookupFlags{
		// TODO(arul): Seems like we can't use a leased version of a descriptor here
		// if it is offline, which is why I'm passing `AvoidCached` here. Need to
		// understand what's happening here a bit better.
		AvoidCached:    true,
		IncludeDropped: true,
		IncludeOffline: true,
	})
	if err != nil {
		return nil, nil, err
	}

	// There's nothing for us to do if the descriptor is offline.
	if desc.Offline() {
		return nil, desc, nil
	}

	if desc.DescriptorType() != catalog.Table && desc.DescriptorType() != catalog.Database {
		return nil, nil, errors.AssertionFailedf("expected either database or table, but found descriptor of type [%s]", desc.DescriptorType())
	}

	if desc.DescriptorType() == catalog.Table {
		return descpb.IDs{id}, desc, nil
	}

	// Now that we know the descriptor belongs to a database, the list of affected
	// table IDs is simply all tables under this database.

	// There's nothing to do here if the database has been dropped. If the
	// database was non-empty those objects will have their own rangefeed events.
	if desc.Dropped() {
		return nil, desc, nil
	}

	tables, err := descsCol.GetAllTableDescriptorsInDatabase(ctx, txn, id)
	if err != nil {
		return nil, desc, err
	}
	ret := make(descpb.IDs, 0, len(tables))
	for _, table := range tables {
		ret = append(ret, table.GetID())
	}
	return ret, desc, nil
}
