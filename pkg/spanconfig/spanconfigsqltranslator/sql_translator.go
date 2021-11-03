// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package spanconfigsqltranslator provides logic to translate sql descriptors
// and their corresponding zone configurations to constituent spans and span
// configurations.
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

// Translate is part of the spanconfig.SQLTranslator interface.
func (s *SQLTranslator) Translate(
	ctx context.Context, ids descpb.IDs,
) ([]roachpb.SpanConfigEntry, hlc.Timestamp, error) {
	var entries []roachpb.SpanConfigEntry
	// txn used to translate the IDs, so that we can get its commit timestamp
	// later.
	var translateTxn *kv.Txn
	if err := sql.DescsTxn(ctx, s.execCfg, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) error {
		// We're in a retryable closure, so clear any entries from previous
		// attempts.
		entries = entries[:0]

		// For every ID we want to translate, first expand it to descendant leaf
		// IDs that have span configurations associated for them. We also
		// de-duplicate leaf IDs so as to not generate redundant entries.
		seen := make(map[descpb.ID]struct{})
		var leafIDs descpb.IDs
		for _, id := range ids {
			descendantLeafIDs, err := s.findDescendantLeafIDs(ctx, id, txn, descsCol)
			if err != nil {
				return err
			}
			for _, descendantLeafID := range descendantLeafIDs {
				if _, found := seen[descendantLeafID]; !found {
					seen[descendantLeafID] = struct{}{}
					leafIDs = append(leafIDs, descendantLeafID)
				}
			}
		}

		// For every leaf ID, which has been de-duplicated, generate span
		// configurations.
		for _, leafID := range leafIDs {
			translatedEntries, err := s.generateSpanConfigurations(ctx, leafID, txn, descsCol)
			if err != nil {
				return err
			}
			entries = append(entries, translatedEntries...)
		}
		translateTxn = txn
		return nil
	}); err != nil {
		return nil, hlc.Timestamp{}, err
	}

	return entries, translateTxn.CommitTimestamp(), nil
}

// generateSpanConfigurations generates the span configurations for the given
// ID. The ID must belong to an object that has a span configuration associated
// with it, i.e, it should either belong to a table or a named zone.
func (s *SQLTranslator) generateSpanConfigurations(
	ctx context.Context, id descpb.ID, txn *kv.Txn, descsCol *descs.Collection,
) (entries []roachpb.SpanConfigEntry, err error) {
	if zonepb.IsNamedZoneID(id) {
		return s.generateSpanConfigurationsForNamedZone(ctx, txn, id)
	}

	// We're dealing with a SQL object.
	desc, err := descsCol.GetImmutableDescriptorByID(ctx, txn, id, tree.CommonLookupFlags{
		// We can (do) generate span configurations for dropped/offline tables.
		IncludeDropped: true,
		IncludeOffline: true,
	})
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			// The descriptor has been deleted. Nothing to do here.
			return nil, nil
		}
		return nil, err
	}

	if desc.DescriptorType() != catalog.Table {
		return nil, errors.AssertionFailedf(
			"can only generate span configurations for tables, but got %s", desc.DescriptorType(),
		)
	}

	return s.generateSpanConfigurationsForTable(ctx, txn, desc)
}

// generateSpanConfigurationsForNamedZone expects an ID corresponding to a named
// zone and generates the span configurations for it.
func (s *SQLTranslator) generateSpanConfigurationsForNamedZone(
	ctx context.Context, txn *kv.Txn, id descpb.ID,
) ([]roachpb.SpanConfigEntry, error) {
	name, ok := zonepb.NamedZonesByID[uint32(id)]
	if !ok {
		return nil, errors.AssertionFailedf("id %d does not belong to a named zone", id)
	}

	// Named zones other than RANGE DEFAULT are not a thing for secondary tenants.
	if !s.codec.ForSystemTenant() && name != zonepb.DefaultZoneName {
		return nil,
			errors.AssertionFailedf("secondary tenants do not have the notion of %s named zone", name)
	}

	var spans []roachpb.Span
	switch name {
	case zonepb.DefaultZoneName: // nothing to do.
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
	case zonepb.TenantsZoneName: // nothing to do.
	default:
		return nil, errors.AssertionFailedf("unknown named zone config %s", name)
	}

	zoneConfig, err := sql.GetHydratedZoneConfigForNamedZone(ctx, txn, s.codec, name)
	if err != nil {
		return nil, err
	}
	spanConfig := zoneConfig.AsSpanConfig()

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

// findDescendantLeafIDs finds all leaf IDs below the given ID in the zone
// configuration hierarchy. Leaf IDs are either table IDs or named zone IDs
// (other than RANGE DEFAULT).
func (s *SQLTranslator) findDescendantLeafIDs(
	ctx context.Context, id descpb.ID, txn *kv.Txn, descsCol *descs.Collection,
) (descpb.IDs, error) {
	if zonepb.IsNamedZoneID(id) {
		return s.findDescendantLeafIDsForNamedZone(ctx, id, txn, descsCol)
	}
	// We're dealing with a SQL Object here.
	return s.findDescendantLeafIDsForDescriptor(ctx, id, txn, descsCol)
}

// findDescendantLeafIDsForDescriptor finds all leaf object IDs below the given
// descriptor ID in the zone configuration hierarchy. Based on the descriptor
// type, these are:
// - Database: IDs of all tables inside the database.
// - Table: ID of the table itself.
// - Schema/Type: Nothing, as schemas/types do not carry zone configurations and
// are not part of the zone configuration hierarchy.
func (s *SQLTranslator) findDescendantLeafIDsForDescriptor(
	ctx context.Context, id descpb.ID, txn *kv.Txn, descsCol *descs.Collection,
) (descpb.IDs, error) {
	desc, err := descsCol.GetImmutableDescriptorByID(ctx, txn, id, tree.CommonLookupFlags{
		// It is reasonable for us to need to traverse zone configuration
		// hierarchies for dropped/offline descriptors. For example, when reacting
		// to a SQLWatcher event when a table is moved from PUBLIC to OFFLINE.
		IncludeDropped: true,
		IncludeOffline: true,
	})
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			// The descriptor has been deleted. Nothing to do here.
			return nil, nil
		}
		return nil, err
	}

	switch desc.DescriptorType() {
	case catalog.Type, catalog.Schema:
		// There is nothing to do for {Type, Schema} descriptors as they are not
		// part of the zone configuration hierarchy.
		return nil, nil
	case catalog.Table:
		// Tables are leaf objects in the zone configuration hierarchy, so simply
		// return the ID.
		return descpb.IDs{id}, nil
	case catalog.Database:
	// Fallthrough.
	default:
		return nil, errors.AssertionFailedf("unknown descriptor type: %s", desc.DescriptorType())
	}

	// There's nothing for us to do if the descriptor is offline or has been
	// dropped.
	if desc.Offline() || desc.Dropped() {
		return nil, nil
	}

	// Expand the database descriptor to all the tables inside it and return their
	// IDs.
	tables, err := descsCol.GetAllTableDescriptorsInDatabase(ctx, txn, desc.GetID(),
		tree.DatabaseLookupFlags{AvoidCached: false})
	if err != nil {
		return nil, err
	}
	ret := make(descpb.IDs, 0, len(tables))
	for _, table := range tables {
		ret = append(ret, table.GetID())
	}
	return ret, nil
}

// findDescendantLeafIDsForNamedZone finds all leaf IDs below the given named
// zone ID in the zone configuration hierarchy.
// Depending on the named zone, these are:
// - RANGE DEFAULT: All tables (and named zones iff system tenant).
// - Any other named zone: ID of the named zone itself.
func (s *SQLTranslator) findDescendantLeafIDsForNamedZone(
	ctx context.Context, id descpb.ID, txn *kv.Txn, descsCol *descs.Collection,
) (descpb.IDs, error) {
	name, ok := zonepb.NamedZonesByID[uint32(id)]
	if !ok {
		return nil, errors.AssertionFailedf("id %d does not belong to a named zone", id)
	}

	if name != zonepb.DefaultZoneName {
		// No IDs lie below named zones other than RANGE DEFAULT in the zone config
		// hierarchy, so simply return the named zone ID.
		return descpb.IDs{id}, nil
	}

	// A change to RANGE DEFAULT translates to every SQL object of the tenant.
	databases, err := descsCol.GetAllDatabaseDescriptors(ctx, txn)
	if err != nil {
		return nil, err
	}
	var descendantIDs descpb.IDs
	for _, dbDesc := range databases {
		tableIDs, err := s.findDescendantLeafIDsForDescriptor(
			ctx, dbDesc.GetID(), txn, descsCol,
		)
		if err != nil {
			return nil, err
		}
		descendantIDs = append(descendantIDs, tableIDs...)
	}

	// All named zones (other than RANGE DEFAULT itself, ofcourse) inherit from
	// RANGE DEFAULT.
	// NB: Only the system tenant has named zones other than RANGE DEFAULT.
	if s.codec.ForSystemTenant() {
		for _, namedZone := range zonepb.NamedZonesList {
			// Add an entry for all named zones bar RANGE DEFAULT.
			if namedZone == zonepb.DefaultZoneName {
				continue
			}
			descendantIDs = append(descendantIDs, descpb.ID(zonepb.NamedZones[namedZone]))
		}
	}
	return descendantIDs, nil
}
