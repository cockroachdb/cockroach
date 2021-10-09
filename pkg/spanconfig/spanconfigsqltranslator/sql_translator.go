// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigsqltranslator

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

// SQLTranslator implements the spanconfig.SQLTranslator interface.
var _ spanconfig.SQLTranslator = &SQLTranslator{}

// SQLTranslator is the concrete implementation of spanconfig.SQLTranslator.
type SQLTranslator struct {
	execCfg *sql.ExecutorConfig
	codec   keys.SQLCodec
}

// New constructs and returns a SQLTranslator.
func New(execCfg *sql.ExecutorConfig, codec keys.SQLCodec) *SQLTranslator {
	return &SQLTranslator{
		execCfg: execCfg,
		codec:   codec,
	}
}

// FullTranslate is part of the spanconfig.SQLTranslator interface.
func (s *SQLTranslator) FullTranslate(
	ctx context.Context,
) ([]roachpb.SpanConfigEntry, hlc.Timestamp, error) {
	// As RANGE DEFAULT is the root of all zone configurations (including
	// other named zones for the system tenant), we can construct the entire
	// span configuration state by starting from RANGE DEFAULT.
	return s.translate(ctx, descpb.IDs{keys.RootNamespaceID})
}

// Translate is part of the spanconfig.SQLTranslator interface.
func (s *SQLTranslator) Translate(
	ctx context.Context, ids descpb.IDs,
) ([]roachpb.SpanConfigEntry, error) {
	entries, _, err := s.translate(ctx, ids)
	return entries, err
}

// translate generates the implied span configuration state for the given IDs.
func (s *SQLTranslator) translate(
	ctx context.Context, ids descpb.IDs,
) (entries []roachpb.SpanConfigEntry, timestamp hlc.Timestamp, err error) {
	err = sql.DescsTxn(
		ctx, s.execCfg, func(
			ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
		) error {
			for _, id := range ids {
				idEntries, err := s.translateID(ctx, id, txn, descsCol)
				if err != nil {
					return err
				}
				entries = append(entries, idEntries...)
			}
			// TODO(arul): Looks like the use of this method means the transactions
			// timestamp can't be pushed. Confirm that we're okay with this, but I'm
			// not sure if there's a better way to do what we want to do.
			timestamp = txn.CommitTimestamp()
			return nil
		})
	return entries, timestamp, err
}

// translateID generates the implied span configuration state for a single ID.
func (s *SQLTranslator) translateID(
	ctx context.Context, id descpb.ID, txn *kv.Txn, descsCol *descs.Collection,
) (entries []roachpb.SpanConfigEntry, err error) {
	var expandedIDs descpb.IDs
	if zonepb.IsNamedZoneID(id) {
		entries, expandedIDs, err = s.translateNamedZoneConfig(ctx, id, txn, descsCol)
		if err != nil {
			return nil, err
		}
	} else {
		// We're dealing with a SQL Object.
		entries, expandedIDs, err = s.translateDescriptorIDZoneConfig(ctx, id, txn, descsCol)
		if err != nil {
			return nil, err
		}
	}

	for _, ID := range expandedIDs {
		idEntries, err := s.translateID(ctx, ID, txn, descsCol)
		if err != nil {
			return nil, err
		}
		entries = append(entries, idEntries...)
	}
	return entries, nil
}

// translateDescriptorIDZoneConfig generates the span configuration for an ID
// belonging to a SQL descriptor. It also returns a list of IDs below the given
// ID in the zone configuration hierarchy. This list of expandedIDs must be
// further translated.
func (s *SQLTranslator) translateDescriptorIDZoneConfig(
	ctx context.Context, id descpb.ID, txn *kv.Txn, descsCol *descs.Collection,
) (entries []roachpb.SpanConfigEntry, expandedIDs descpb.IDs, err error) {
	desc, err := descsCol.GetImmutableDescriptorByID(ctx, txn, id, tree.CommonLookupFlags{
		AvoidCached:    true,
		IncludeDropped: true,
		IncludeOffline: true,
	})
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			// The descriptor has been deleted. If this is the case then no span
			// configuration exists for it.
			return nil, nil, nil
		}
		return nil, nil, err
	}
	expandedIDs, err = findIDsBelowDescriptorInZoneConfigHierarchy(ctx, desc, txn, descsCol)
	if err != nil {
		return nil, nil, err
	}

	// There's nothing to do for {Type,Schema} descriptors.
	if desc.DescriptorType() == catalog.Type || desc.DescriptorType() == catalog.Schema {
		return nil, nil, nil
	}

	// If the descriptor is that of a table then we simply generate all span
	// configs.
	if desc.DescriptorType() == catalog.Table {
		entries, err = s.generateSpanConfigurationsForTable(ctx, txn, desc)
		if err != nil {
			return nil, nil, err
		}
		return entries, nil, nil
	}

	// The ID belongs to a database. There aren't any entries to generate for the
	// database itself, but we need to pass the list of expanded IDs to the caller
	// to translate further.
	return nil, expandedIDs, nil
}

// translateNamedZoneConfig generates the span configuration state for an ID
// that corresponds to a named zone. It also returns a list of expanded IDs that
// need to be further translated as they lie further down in the zone
// configuration hierarchy. This is only ever the case for RANGE DEFAULT.
func (s *SQLTranslator) translateNamedZoneConfig(
	ctx context.Context, id descpb.ID, txn *kv.Txn, descsCol *descs.Collection,
) (entries []roachpb.SpanConfigEntry, expandedIDs descpb.IDs, err error) {
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

	entries, err = s.generateSpanConfigurationsForNamedZone(ctx, txn, name)
	if err != nil {
		return nil, nil, err
	}

	switch name {
	case zonepb.DefaultZoneName:
	default:
		// No IDs lie below in the zone configuration hierarchy for named zones
		// other than RANGE DEFAULT, so expanded IDs is nil here.
		return entries, nil, nil
	}

	// A change to RANGE DEFAULT may imply a change to every SQL object for the
	// tenant.
	databases, err := descsCol.GetAllDatabaseDescriptors(ctx, txn)
	if err != nil {
		return nil, nil, err
	}
	for _, dbDesc := range databases {
		tableIDs, err := findIDsBelowDescriptorInZoneConfigHierarchy(ctx, dbDesc, txn, descsCol)
		if err != nil {
			return nil, nil, err
		}
		expandedIDs = append(expandedIDs, tableIDs...)
	}

	// As all named zones also inherit from RANGE DEFAULT, a change to it may also
	// imply a change to every other named zone. This is only a thing for the
	// host tenant as named zones other than RANGE DEFAULT don't exist for
	// secondary tenants.
	if s.codec.ForSystemTenant() {
		for _, namedZone := range zonepb.NamedZonesList {
			// Add an entry for all named zones bar RANGE DEFAULT.
			if namedZone != zonepb.DefaultZoneName {
				expandedIDs = append(expandedIDs, descpb.ID(zonepb.NamedZones[namedZone]))
			}
		}
	}

	return entries, expandedIDs, nil
}

// generateSpanConfigurationsForNamedZone takes in a named zone and generates
// the span configuration for that range.
func (s *SQLTranslator) generateSpanConfigurationsForNamedZone(
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
func (s *SQLTranslator) generateSpanConfigurationsForTable(
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

// findIDsBelowDescriptorInZoneConfigHierarchy returns a list of IDs that lie
// below the given descriptor in the zone configuration hierarchy. This is all
// tableIDs for database descriptors. It is a pass through for all other
// descriptor types.
func findIDsBelowDescriptorInZoneConfigHierarchy(
	ctx context.Context, desc catalog.Descriptor, txn *kv.Txn, descsCol *descs.Collection,
) (descpb.IDs, error) {
	// No descriptors lie below {Table,Type,Schema} descriptors in the zone config
	// hierarchy.
	if desc.DescriptorType() == catalog.Table || desc.DescriptorType() == catalog.Type || desc.DescriptorType() == catalog.Schema {
		return nil, nil
	}

	// Now that we know the descriptor belongs to a database, the list of IDs
	// below it in the zone config hierarchy is simply all the tables in the
	// database.

	// There's nothing for us to do if the descriptor is offline or has been
	// dropped.
	if desc.Offline() || desc.Dropped() {
		return nil, nil
	}

	tables, err := descsCol.GetAllTableDescriptorsInDatabase(ctx, txn, desc.GetID())
	if err != nil {
		return nil, err
	}
	ret := make(descpb.IDs, 0, len(tables))
	for _, table := range tables {
		ret = append(ret, table.GetID())
	}
	return ret, nil
}
