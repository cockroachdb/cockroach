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
	knobs   *spanconfig.TestingKnobs
}

// New constructs and returns a SQLTranslator.
func New(
	execCfg *sql.ExecutorConfig, codec keys.SQLCodec, knobs *spanconfig.TestingKnobs,
) *SQLTranslator {
	if knobs == nil {
		knobs = &spanconfig.TestingKnobs{}
	}
	return &SQLTranslator{
		execCfg: execCfg,
		codec:   codec,
		knobs:   knobs,
	}
}

// Translate is part of the spanconfig.SQLTranslator interface.
func (s *SQLTranslator) Translate(
	ctx context.Context, ids descpb.IDs, generateSystemSpanConfigurations bool,
) ([]spanconfig.Record, hlc.Timestamp, error) {
	var records []spanconfig.Record
	// txn used to translate the IDs, so that we can get its commit timestamp
	// later.
	var translateTxn *kv.Txn
	if err := sql.DescsTxn(ctx, s.execCfg, func(
		ctx context.Context, txn *kv.Txn, descsCol *descs.Collection,
	) error {
		// We're in a retryable closure, so clear any records from previous
		// attempts.
		records = records[:0]

		// Construct an in-memory view of the system.protected_ts_records table to
		// populate the protected timestamp field on the emitted span configs.
		//
		// TODO(adityamaru): This does a full table scan of the
		// `system.protected_ts_records` table. While this is not assumed to be very
		// expensive given the limited number of concurrent users of the protected
		// timestamp subsystem, and the internal limits to limit the size of this
		// table, there is scope for improvement in the future. One option could be
		// a rangefeed-backed materialized view of the system table.
		ptsState, err := s.execCfg.ProtectedTimestampProvider.GetState(ctx, txn)
		if err != nil {
			return errors.Wrap(err, "failed to get protected timestamp state")
		}
		ptsStateReader := spanconfig.NewProtectedTimestampStateReader(ctx, ptsState)

		if generateSystemSpanConfigurations {
			records, err = s.generateSystemSpanConfigRecords(ptsStateReader)
			if err != nil {
				return errors.Wrap(err, "failed to generate SystemTarget records")
			}
		}

		// For every ID we want to translate, first expand it to descendant leaf
		// IDs that have span configurations associated for them. We also
		// de-duplicate leaf IDs to not generate redundant entries.
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

		pseudoTableRecords, err := s.maybeGeneratePseudoTableRecords(ctx, txn, ids)
		if err != nil {
			return err
		}
		records = append(records, pseudoTableRecords...)

		scratchRangeRecord, err := s.maybeGenerateScratchRangeRecord(ctx, txn, ids)
		if err != nil {
			return err
		}
		if !scratchRangeRecord.IsEmpty() {
			records = append(records, scratchRangeRecord)
		}

		// For every unique leaf ID, generate span configurations.
		for _, leafID := range leafIDs {
			translatedRecords, err := s.generateSpanConfigurations(ctx, leafID, txn, descsCol, ptsStateReader)
			if err != nil {
				return err
			}
			records = append(records, translatedRecords...)
		}
		translateTxn = txn
		return nil
	}); err != nil {
		return nil, hlc.Timestamp{}, err
	}

	return records, translateTxn.CommitTimestamp(), nil
}

// descLookupFlags is the set of look up flags used when fetching descriptors.
var descLookupFlags = tree.CommonLookupFlags{
	// We act on errors being surfaced when the descriptor being looked up is
	// not found.
	Required: true,
	// We can (do) generate span configurations for dropped and offline tables.
	IncludeDropped: true,
	IncludeOffline: true,
	// We want consistent reads.
	AvoidLeased: true,
}

// generateSystemSpanConfigRecords is responsible for generating all the SpanConfigs
// that apply to spanconfig.SystemTargets.
func (s *SQLTranslator) generateSystemSpanConfigRecords(
	ptsStateReader *spanconfig.ProtectedTimestampStateReader,
) ([]spanconfig.Record, error) {
	tenantPrefix := s.codec.TenantPrefix()
	_, sourceTenantID, err := keys.DecodeTenantPrefix(tenantPrefix)
	if err != nil {
		return nil, err
	}
	records := make([]spanconfig.Record, 0)

	// Aggregate cluster target protections for the tenant.
	clusterProtections := ptsStateReader.GetProtectionPoliciesForCluster()
	if len(clusterProtections) != 0 {
		var systemTarget spanconfig.SystemTarget
		var err error
		if sourceTenantID == roachpb.SystemTenantID {
			systemTarget = spanconfig.MakeEntireKeyspaceTarget()
		} else {
			systemTarget, err = spanconfig.MakeTenantKeyspaceTarget(sourceTenantID, sourceTenantID)
			if err != nil {
				return nil, err
			}
		}
		clusterSystemRecord := spanconfig.Record{
			Target: spanconfig.MakeTargetFromSystemTarget(systemTarget),
			Config: roachpb.SpanConfig{GCPolicy: roachpb.GCPolicy{ProtectionPolicies: clusterProtections}}}
		records = append(records, clusterSystemRecord)
	}

	// Aggregate tenant target protections.
	tenantProtections := ptsStateReader.GetProtectionPoliciesForTenants()
	for _, protection := range tenantProtections {
		tenantProtection := protection
		systemTarget, err := spanconfig.MakeTenantKeyspaceTarget(sourceTenantID, tenantProtection.GetTenantID())
		if err != nil {
			return nil, err
		}
		tenantSystemRecord := spanconfig.Record{
			Target: spanconfig.MakeTargetFromSystemTarget(systemTarget),
			Config: roachpb.SpanConfig{GCPolicy: roachpb.GCPolicy{
				ProtectionPolicies: tenantProtection.GetTenantProtections()}},
		}
		records = append(records, tenantSystemRecord)
	}
	return records, nil
}

// generateSpanConfigurations generates the span configurations for the given
// ID. The ID must belong to an object that has a span configuration associated
// with it, i.e, it should either belong to a table or a named zone.
func (s *SQLTranslator) generateSpanConfigurations(
	ctx context.Context,
	id descpb.ID,
	txn *kv.Txn,
	descsCol *descs.Collection,
	ptsStateReader *spanconfig.ProtectedTimestampStateReader,
) (_ []spanconfig.Record, err error) {
	if zonepb.IsNamedZoneID(id) {
		return s.generateSpanConfigurationsForNamedZone(ctx, txn, id)
	}

	// We're dealing with a SQL object.
	desc, err := descsCol.GetImmutableDescriptorByID(ctx, txn, id, descLookupFlags)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil, nil // the descriptor has been deleted; nothing to do here
		}
		return nil, err
	}
	if s.knobs.ExcludeDroppedDescriptorsFromLookup && desc.Dropped() {
		return nil, nil // we're excluding this descriptor; nothing to do here
	}

	if desc.DescriptorType() != catalog.Table {
		return nil, errors.AssertionFailedf(
			"can only generate span configurations for tables, but got %s", desc.DescriptorType(),
		)
	}

	return s.generateSpanConfigurationsForTable(ctx, txn, desc, ptsStateReader)
}

// generateSpanConfigurationsForNamedZone expects an ID corresponding to a named
// zone and generates the span configurations for it.
func (s *SQLTranslator) generateSpanConfigurationsForNamedZone(
	ctx context.Context, txn *kv.Txn, id descpb.ID,
) ([]spanconfig.Record, error) {
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
		// We also don't apply configurations over the SystemSpanConfigSpan; spans
		// carved from this range have no data, instead, associated configurations
		// for these spans have special meaning in `system.span_configurations`.
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
			EndKey: keys.SystemSpanConfigSpan.Key,
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
	var records []spanconfig.Record
	for _, span := range spans {
		records = append(records, spanconfig.Record{
			Target: spanconfig.MakeTargetFromSpan(span),
			Config: spanConfig,
		})
	}
	return records, nil
}

// generateSpanConfigurationsForTable generates the span configurations
// corresponding to the given tableID. It uses a transactional view of
// system.zones and system.descriptors to do so.
func (s *SQLTranslator) generateSpanConfigurationsForTable(
	ctx context.Context,
	txn *kv.Txn,
	desc catalog.Descriptor,
	ptsStateReader *spanconfig.ProtectedTimestampStateReader,
) ([]spanconfig.Record, error) {
	if desc.DescriptorType() != catalog.Table {
		return nil, errors.AssertionFailedf(
			"expected table descriptor, but got descriptor of type %s", desc.DescriptorType(),
		)
	}
	zone, err := sql.GetHydratedZoneConfigForTable(ctx, txn, s.codec, desc.GetID())
	if err != nil {
		return nil, err
	}

	isSystemDesc := catalog.IsSystemDescriptor(desc)
	tableStartKey := s.codec.TablePrefix(uint32(desc.GetID()))
	tableEndKey := tableStartKey.PrefixEnd()
	tableSpanConfig := zone.AsSpanConfig()
	if isSystemDesc {
		// We enable rangefeeds for system tables; various internal subsystems
		// (leveraging system tables) rely on rangefeeds to function.
		tableSpanConfig.RangefeedEnabled = true
		// We exclude system tables from strict GC enforcement, it's only really
		// applicable to user tables.
		tableSpanConfig.GCPolicy.IgnoreStrictEnforcement = true
	}

	// Set the ProtectionPolicies on the table's SpanConfig to include protected
	// timestamps that apply to the table, and its parent database.
	tableSpanConfig.GCPolicy.ProtectionPolicies = append(
		ptsStateReader.GetProtectionPoliciesForSchemaObject(desc.GetID()),
		ptsStateReader.GetProtectionPoliciesForSchemaObject(desc.GetParentID())...)

	// Set whether the table's row data has been marked to be excluded from
	// backups.
	tableSpanConfig.ExcludeDataFromBackup = desc.(catalog.TableDescriptor).GetExcludeDataFromBackup()

	records := make([]spanconfig.Record, 0)
	if desc.GetID() == keys.DescriptorTableID {
		// We have some special handling for `system.descriptor` on account of
		// it being the first non-empty table in every tenant's keyspace.
		if !s.codec.ForSystemTenant() {
			// We start the span at the tenant prefix. This effectively installs
			// the tenant's split boundary at /Tenant/<id> instead of
			// /Tenant/<id>/Table/3. This doesn't really make a difference given
			// there's no data within [/Tenant/<id>/ - /Tenant/<id>/Table/3),
			// but looking at range boundaries, it's slightly less confusing
			// this way.
			records = append(records, spanconfig.Record{
				Target: spanconfig.MakeTargetFromSpan(roachpb.Span{
					Key:    s.codec.TenantPrefix(),
					EndKey: tableEndKey,
				}),
				Config: tableSpanConfig,
			})
		} else {
			// The same as above, except we have named ranges preceding
			// `system.descriptor`. Not doing anything special here would mean
			// splitting on /Table/3 instead of /Table/0 (pretty printed as
			// /Table/SystemConfigSpan/Start), which is benign since there's no
			// data under /Table/{0-2}. Still, doing it this way reduces the
			// differences between the gossip-backed subsystem and this one --
			// somewhat useful for understandability reasons and reducing the
			// (tiny) re-splitting costs when switching between the two
			// subsystems.
			records = append(records, spanconfig.Record{
				Target: spanconfig.MakeTargetFromSpan(roachpb.Span{
					Key:    keys.SystemConfigSpan.Key,
					EndKey: tableEndKey,
				}),
				Config: tableSpanConfig,
			})
		}

		return records, nil

		// TODO(irfansharif): There's an attack vector here that we haven't
		// addressed satisfactorily. By splitting only on start keys of span
		// configs, a malicious tenant could augment their reconciliation
		// process to install configs starting much later in their addressable
		// keyspace. This could induce KV to consider a range boundary that
		// starts at the previous tenant's keyspace (albeit the tail end of it)
		// and ending within the malicious tenant's one -- that's no good. We
		// could do two things:
		// (i)  Have each tenant install a span config that demarcates the end of
		//      its keyspace (in our example the previous tenant would defensively
		//      prevent leaking its user data through this hard boundary);
		// (ii) Have KV enforce these hard boundaries in with keyspans that
		//      straddle tenant keyspaces.
		//
		// Doing (ii) feels saner, and we already do something similar when
		// seeding `system.span_configurations` for newly created tenants. We
		// could have secondary tenants still govern span configurations over
		// their keyspace, but we'd always split on the tenant boundary. For
		// malicious tenants, the config we'd apply over that range would be the
		// fallback one KV already uses for missing spans. For non-malicious
		// tenants, it could either be the config for (i) `system.descriptor` as
		// done above, or (ii) whatever the tenant's RANGE DEFAULT is.
		//
		// See #73749.
	}

	prevEndKey := tableStartKey
	for i := range zone.SubzoneSpans {
		// We need to prepend the tablePrefix to the spans stored inside the
		// SubzoneSpans field because we store the stripped version there for
		// historical reasons.
		//
		// NB: Re-using tableStartKey/prevEndKey here, or pulling out a
		// variable, would be buggy -- the underlying buffer gets mutated by the
		// append, throwing everything else off below.
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
			records = append(records,
				spanconfig.Record{
					Target: spanconfig.MakeTargetFromSpan(roachpb.Span{Key: prevEndKey, EndKey: span.Key}),
					Config: tableSpanConfig,
				},
			)
		}

		// Add an entry for the subzone.
		subzoneSpanConfig := zone.Subzones[zone.SubzoneSpans[i].SubzoneIndex].Config.AsSpanConfig()
		// Copy relevant fields that apply to the table's SpanConfig onto its
		// SubzoneSpanConfig.
		subzoneSpanConfig.GCPolicy.ProtectionPolicies = tableSpanConfig.GCPolicy.ProtectionPolicies[:]
		subzoneSpanConfig.ExcludeDataFromBackup = tableSpanConfig.ExcludeDataFromBackup
		if isSystemDesc { // same as above
			subzoneSpanConfig.RangefeedEnabled = true
			subzoneSpanConfig.GCPolicy.IgnoreStrictEnforcement = true
		}
		records = append(records,
			spanconfig.Record{
				Target: spanconfig.MakeTargetFromSpan(roachpb.Span{Key: span.Key, EndKey: span.EndKey}),
				Config: subzoneSpanConfig,
			},
		)

		prevEndKey = span.EndKey
	}

	// If the last subzone span doesn't cover the entire table's keyspace then
	// we cover the remaining key range with the table's zone configuration.
	if !prevEndKey.Equal(tableEndKey) {
		records = append(records,
			spanconfig.Record{
				Target: spanconfig.MakeTargetFromSpan(roachpb.Span{Key: prevEndKey, EndKey: tableEndKey}),
				Config: tableSpanConfig,
			},
		)
	}
	return records, nil
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
	desc, err := descsCol.GetImmutableDescriptorByID(ctx, txn, id, descLookupFlags)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil, nil // the descriptor has been deleted; nothing to do here
		}
		return nil, err
	}
	if s.knobs.ExcludeDroppedDescriptorsFromLookup && desc.Dropped() {
		return nil, nil // we're excluding this descriptor; nothing to do here
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

// maybeGeneratePseudoTableRecords generates span configs for
// pseudo table ID key spans, if applicable.
func (s *SQLTranslator) maybeGeneratePseudoTableRecords(
	ctx context.Context, txn *kv.Txn, ids descpb.IDs,
) ([]spanconfig.Record, error) {
	if !s.codec.ForSystemTenant() {
		return nil, nil
	}

	for _, id := range ids {
		if id != keys.SystemDatabaseID && id != keys.RootNamespaceID {
			continue // nothing to do
		}

		// We have special handling for the system database (and RANGE DEFAULT,
		// which the system database inherits from). The system config span
		// infrastructure generates splits along (empty) pseudo table
		// boundaries[1] -- we do the same. Not doing so is safe, but this helps
		// reduce the differences between the two subsystems which has practical
		// implications for our bootstrap code and tests that bake in
		// assumptions about these splits. While the two systems exist
		// side-by-side, it's easier to just minimize these differences (it also
		// removes the tiny re-splitting costs when switching between them). We
		// can get rid of this special handling once the system config span is
		// removed (#70560).
		//
		// [1]: Consider the liveness range [/System/NodeLiveness,
		//      /System/NodeLivenessMax). It's identified using the pseudo ID 22
		//      (i.e. keys.LivenessRangesID). Because we're using a pseudo ID,
		//      what of [/Table/22-/Table/23)? This is a keyspan with no
		//      contents, yet one the system config span splits along to create
		//      an empty range. It's precisely this "feature" we're looking to
		//      emulate. As for what config to apply over said range -- we do as
		//      the system config span does, applying the config for the system
		//      database.
		zone, err := sql.GetHydratedZoneConfigForDatabase(ctx, txn, s.codec, keys.SystemDatabaseID)
		if err != nil {
			return nil, err
		}
		tableSpanConfig := zone.AsSpanConfig()
		var records []spanconfig.Record
		for _, pseudoTableID := range keys.PseudoTableIDs {
			tableStartKey := s.codec.TablePrefix(pseudoTableID)
			tableEndKey := tableStartKey.PrefixEnd()
			records = append(records, spanconfig.Record{
				Target: spanconfig.MakeTargetFromSpan(roachpb.Span{
					Key:    tableStartKey,
					EndKey: tableEndKey,
				}),
				Config: tableSpanConfig,
			})
		}

		return records, nil
	}

	return nil, nil
}

func (s *SQLTranslator) maybeGenerateScratchRangeRecord(
	ctx context.Context, txn *kv.Txn, ids descpb.IDs,
) (spanconfig.Record, error) {
	if !s.knobs.ConfigureScratchRange || !s.codec.ForSystemTenant() {
		return spanconfig.Record{}, nil // nothing to do
	}

	for _, id := range ids {
		if id != keys.RootNamespaceID {
			continue // nothing to do
		}

		zone, err := sql.GetHydratedZoneConfigForDatabase(ctx, txn, s.codec, keys.RootNamespaceID)
		if err != nil {
			return spanconfig.Record{}, err
		}

		return spanconfig.Record{
			Target: spanconfig.MakeTargetFromSpan(roachpb.Span{
				Key:    keys.ScratchRangeMin,
				EndKey: keys.ScratchRangeMax,
			}),
			Config: zone.AsSpanConfig(),
		}, nil
	}

	return spanconfig.Record{}, nil
}
