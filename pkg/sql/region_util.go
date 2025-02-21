// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/zone"
	"github.com/cockroachdb/cockroach/pkg/sql/parser/statements"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/regions"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

// LiveClusterRegions is a set representing regions that are live in
// a given cluster.
type LiveClusterRegions map[catpb.RegionName]struct{}

// IsActive returns whether the given region is a live region.
func (s *LiveClusterRegions) IsActive(region catpb.RegionName) bool {
	_, ok := (*s)[region]
	return ok
}

func (s *LiveClusterRegions) toStrings() []string {
	ret := make([]string, 0, len(*s))
	for region := range *s {
		ret = append(ret, string(region))
	}
	sort.Slice(ret, func(i, j int) bool {
		return ret[i] < ret[j]
	})
	return ret
}

func (p *planner) getLiveClusterRegions(ctx context.Context) (LiveClusterRegions, error) {
	return GetLiveClusterRegions(ctx, p)
}

// GetLiveClusterRegions returns a set of live region names in the cluster.
// A region name is deemed active if there is at least one alive node
// in the cluster in with locality set to a given region.
func GetLiveClusterRegions(ctx context.Context, p PlanHookState) (LiveClusterRegions, error) {
	// Non-admin users can't access the crdb_internal.kv_node_status table, which
	// this query hits, so we must override the user here.
	regions, err := p.InternalSQLTxn().Regions().GetRegions(ctx)
	if err != nil {
		return nil, err
	}
	return regionsResponseToLiveClusterRegions(regions), nil
}

func regionsResponseToLiveClusterRegions(regions *serverpb.RegionsResponse) LiveClusterRegions {
	var ret LiveClusterRegions = make(map[catpb.RegionName]struct{}, len(regions.Regions))
	for region := range regions.Regions {
		ret[catpb.RegionName(region)] = struct{}{}
	}
	return ret
}

// CheckClusterRegionIsLive checks whether a region supplied is one of the
// currently active cluster regions.
func CheckClusterRegionIsLive(
	liveClusterRegions LiveClusterRegions, region catpb.RegionName,
) error {
	if !liveClusterRegions.IsActive(region) {
		return errors.WithHintf(
			pgerror.Newf(
				pgcode.InvalidName,
				"region %q does not exist",
				region,
			),
			"valid regions: %s",
			strings.Join(liveClusterRegions.toStrings(), ", "),
		)
	}
	return nil
}

// TestingConvertRegionToZoneConfig converts a given region config into a zone
// configuration, ensuring the result is fully hydrated. Refer to the
// zoneConfigForMultiRegionDatabase function for details on how the conversion
// is made. Note that this should only be used for testing purposes.
func TestingConvertRegionToZoneConfig(
	regionConfig multiregion.RegionConfig,
) (zonepb.ZoneConfig, error) {
	zc, err := zoneConfigForMultiRegionDatabase(regionConfig)

	// Hardcode settings based on DefaultZoneConfig() to ensure that generated
	// zone configuration is fully hydrated for AsSpanConfig() conversion.
	defaultZoneConfig := zonepb.DefaultZoneConfig()
	zc.RangeMinBytes = defaultZoneConfig.RangeMinBytes
	zc.RangeMaxBytes = defaultZoneConfig.RangeMaxBytes
	zc.GC = defaultZoneConfig.GC
	zc.NullVoterConstraintsIsEmpty = defaultZoneConfig.NullVoterConstraintsIsEmpty
	return zc, err
}

// zoneConfigForMultiRegionDatabase generates a ZoneConfig stub for a
// multi-region database such that at least one replica (voting or non-voting)
// is constrained to each region defined within the given `regionConfig` and
// some voting replicas are constrained to the primary region of the database
// depending on its prescribed survivability goal.
//
// For instance, for a database with 4 regions: A (primary), B, C, D and region
// survivability. This method will generate a ZoneConfig object representing
// the following attributes:
// num_replicas = 5
// num_voters = 5
// constraints = '{"+region=A": 1,"+region=B": 1,"+region=C": 1,"+region=D": 1}'
// voter_constraints = '{"+region=A": 2}'
// lease_preferences = [["+region=A"]]
//
// See SynthesizeVoterConstraints() for explanation on why `voter_constraints`
// are set the way they are.
func zoneConfigForMultiRegionDatabase(
	regionConfig multiregion.RegionConfig,
) (zonepb.ZoneConfig, error) {
	numVoters, numReplicas := regions.GetNumVotersAndNumReplicas(regionConfig)

	constraints, err := regions.SynthesizeReplicaConstraints(regionConfig.Regions(), regionConfig.Placement())
	if err != nil {
		return zonepb.ZoneConfig{}, err
	}

	voterConstraints, err := regions.SynthesizeVoterConstraints(regionConfig.PrimaryRegion(), regionConfig)
	if err != nil {
		return zonepb.ZoneConfig{}, err
	}

	leasePreferences := regions.SynthesizeLeasePreferences(regionConfig.PrimaryRegion(), regionConfig.SecondaryRegion())

	zc := zonepb.ZoneConfig{
		NumReplicas:                 &numReplicas,
		NumVoters:                   &numVoters,
		Constraints:                 constraints,
		VoterConstraints:            voterConstraints,
		LeasePreferences:            leasePreferences,
		InheritedConstraints:        false,
		NullVoterConstraintsIsEmpty: true,
		InheritedLeasePreferences:   false,
	}

	// The validation of the extended zone config is done here.
	return regionConfig.ExtendZoneConfigWithRegionalIn(zc, regionConfig.PrimaryRegion())
}

// zoneConfigForMultiRegionTable generates a ZoneConfig stub for a
// regional-by-table or global table in a multi-region database.
//
// At the table/partition level, the only attributes that are set are
// `num_voters`, `voter_constraints`, and `lease_preferences`. We expect that
// the attributes `num_replicas` and `constraints` will be inherited from the
// database level zone config.
//
// This function can return a nil zonepb.ZoneConfig, meaning no table level zone
// configuration is required.
//
// Relevant multi-region configured fields (as defined in
// `zonepb.MultiRegionZoneConfigFields`) will be overwritten by the calling function
// into an existing ZoneConfig.
func zoneConfigForMultiRegionTable(
	localityConfig catpb.LocalityConfig, regionConfig multiregion.RegionConfig,
) (zonepb.ZoneConfig, error) {
	zc := *zonepb.NewZoneConfig()

	switch l := localityConfig.Locality.(type) {
	case *catpb.LocalityConfig_Global_:
		// Enable non-blocking transactions.
		zc.GlobalReads = proto.Bool(true)

		if !regionConfig.GlobalTablesInheritDatabaseConstraints() {
			// For GLOBAL tables, we want non-voters in all regions for fast reads, so
			// we always use a DEFAULT placement config, even if the database is using
			// RESTRICTED placement.
			regionConfig = regionConfig.WithPlacementDefault()

			numVoters, numReplicas := regions.GetNumVotersAndNumReplicas(regionConfig)
			zc.NumVoters = &numVoters
			zc.NumReplicas = &numReplicas

			constraints, err := regions.SynthesizeReplicaConstraints(regionConfig.Regions(), regionConfig.Placement())
			if err != nil {
				return zonepb.ZoneConfig{}, err
			}
			zc.Constraints = constraints
			zc.InheritedConstraints = false

			voterConstraints, err := regions.SynthesizeVoterConstraints(regionConfig.PrimaryRegion(), regionConfig)
			if err != nil {
				return zonepb.ZoneConfig{}, err
			}
			zc.VoterConstraints = voterConstraints
			zc.NullVoterConstraintsIsEmpty = true
			zc.LeasePreferences = regions.SynthesizeLeasePreferences(regionConfig.PrimaryRegion(), "" /* secondaryRegion */)
			zc.InheritedLeasePreferences = false

			zc, err = regionConfig.ExtendZoneConfigWithGlobal(zc)
			if err != nil {
				return zonepb.ZoneConfig{}, err
			}
		}
		// Inherit lease preference from the database. We do
		// nothing here because `NewZoneConfig()` already marks the field as
		// 'inherited'.
		return zc, nil
	case *catpb.LocalityConfig_RegionalByTable_:
		affinityRegion := regionConfig.PrimaryRegion()
		if l.RegionalByTable.Region != nil {
			affinityRegion = *l.RegionalByTable.Region
		}
		if l.RegionalByTable.Region == nil && !regionConfig.IsMemberOfExplicitSuperRegion(affinityRegion) {
			// If we don't have an explicit affinity region, use the same
			// configuration as the database and return a blank zcfg here.
			return zc, nil
		}

		numVoters, numReplicas := regions.GetNumVotersAndNumReplicas(regionConfig)
		zc.NumVoters = &numVoters

		if regionConfig.IsMemberOfExplicitSuperRegion(affinityRegion) {
			err := regions.AddConstraintsForSuperRegion(&zc, regionConfig, affinityRegion)
			if err != nil {
				return zonepb.ZoneConfig{}, err
			}
		} else if !regionConfig.RegionalInTablesInheritDatabaseConstraints(affinityRegion) {
			// If the database constraints can't be inherited to serve as the
			// constraints for this table, define the constraints ourselves.
			zc.NumReplicas = &numReplicas

			constraints, err := regions.SynthesizeReplicaConstraints(regionConfig.Regions(), regionConfig.Placement())
			if err != nil {
				return zonepb.ZoneConfig{}, err
			}
			zc.Constraints = constraints
			zc.InheritedConstraints = false
		}

		// If the table has a user-specified affinity region, use it.
		voterConstraints, err := regions.SynthesizeVoterConstraints(affinityRegion, regionConfig)
		if err != nil {
			return zonepb.ZoneConfig{}, err
		}
		zc.VoterConstraints = voterConstraints
		zc.NullVoterConstraintsIsEmpty = true
		zc.LeasePreferences = regions.SynthesizeLeasePreferences(affinityRegion, "" /* secondaryRegion */)
		zc.InheritedLeasePreferences = false

		return regionConfig.ExtendZoneConfigWithRegionalIn(zc, affinityRegion)

	case *catpb.LocalityConfig_RegionalByRow_:
		// We purposely do not set anything here at table level - this should be done at
		// partition level instead.
		return zc, nil
	default:
		return zonepb.ZoneConfig{}, errors.AssertionFailedf(
			"unexpected unknown locality type %T", localityConfig.Locality)
	}
}

// applyZoneConfigForMultiRegionTableOption is an option that can be passed into
// applyZoneConfigForMultiRegionTable.
type applyZoneConfigForMultiRegionTableOption func(
	zoneConfig zonepb.ZoneConfig,
	regionConfig multiregion.RegionConfig,
	table catalog.TableDescriptor,
) (hasNewSubzones bool, newZoneConfig zonepb.ZoneConfig, err error)

// applyZoneConfigForMultiRegionTableOptionNewIndexes applies table zone configs
// for a newly added index which requires partitioning of individual indexes.
func applyZoneConfigForMultiRegionTableOptionNewIndexes(
	indexIDs ...descpb.IndexID,
) applyZoneConfigForMultiRegionTableOption {
	return func(
		zoneConfig zonepb.ZoneConfig,
		regionConfig multiregion.RegionConfig,
		table catalog.TableDescriptor,
	) (hasNewSubzones bool, newZoneConfig zonepb.ZoneConfig, err error) {
		for _, indexID := range indexIDs {
			for _, region := range regionConfig.Regions() {
				zc, err := regions.ZoneConfigForMultiRegionPartition(region, regionConfig)
				if err != nil {
					return false, zoneConfig, err
				}
				zoneConfig.SetSubzone(zonepb.Subzone{
					IndexID:       uint32(indexID),
					PartitionName: string(region),
					Config:        zc,
				})
			}
		}
		return true, zoneConfig, nil
	}
}

// dropZoneConfigsForMultiRegionIndexes drops the zone configs for all
// the indexes defined on a multi-region table. This function is used to clean
// up zone configs when transitioning from REGIONAL BY ROW.
func dropZoneConfigsForMultiRegionIndexes(
	indexIDs ...descpb.IndexID,
) applyZoneConfigForMultiRegionTableOption {
	return func(
		zoneConfig zonepb.ZoneConfig,
		regionConfig multiregion.RegionConfig,
		table catalog.TableDescriptor,
	) (hasNewSubzones bool, newZoneConfig zonepb.ZoneConfig, err error) {
		// Clear all multi-region fields of the subzones. If this leaves them
		// empty, they will automatically be removed.
		zoneConfig.ClearFieldsOfAllSubzones(zonepb.MultiRegionZoneConfigFields)

		// Strip placeholder status and spans if there are no more subzones.
		if len(zoneConfig.Subzones) == 0 && zoneConfig.IsSubzonePlaceholder() {
			zoneConfig.NumReplicas = nil
			zoneConfig.SubzoneSpans = nil
		}
		return false, zoneConfig, nil
	}
}

// applyZoneConfigForMultiRegionTableOptionTableNewConfig applies table zone
// configs on the entire table with the given new locality config.
func applyZoneConfigForMultiRegionTableOptionTableNewConfig(
	newConfig catpb.LocalityConfig,
) applyZoneConfigForMultiRegionTableOption {
	return func(
		zc zonepb.ZoneConfig,
		regionConfig multiregion.RegionConfig,
		table catalog.TableDescriptor,
	) (bool, zonepb.ZoneConfig, error) {
		localityZoneConfig, err := zoneConfigForMultiRegionTable(
			newConfig,
			regionConfig,
		)
		if err != nil {
			return false, zonepb.ZoneConfig{}, err
		}
		zc.CopyFromZone(localityZoneConfig, zonepb.MultiRegionZoneConfigFields)
		return false, zc, nil
	}
}

// ApplyZoneConfigForMultiRegionTableOptionTableAndIndexes applies table zone configs
// on the entire table as well as its indexes, replacing multi-region related zone
// configuration fields.
var ApplyZoneConfigForMultiRegionTableOptionTableAndIndexes = func(
	zc zonepb.ZoneConfig,
	regionConfig multiregion.RegionConfig,
	table catalog.TableDescriptor,
) (bool, zonepb.ZoneConfig, error) {
	localityConfig := *table.GetLocalityConfig()
	localityZoneConfig, err := zoneConfigForMultiRegionTable(
		localityConfig,
		regionConfig,
	)
	if err != nil {
		return false, zonepb.ZoneConfig{}, err
	}

	// Wipe out the subzone multi-region fields before we copy over the
	// multi-region fields to the zone config down below. We have to do this to
	// handle the case where users have set a zone config on an index and we're
	// ALTERing to a table locality that doesn't lay down index zone
	// configurations (e.g. GLOBAL or REGIONAL BY TABLE). Since the user will
	// have to override to perform the ALTER, we want to wipe out the index
	// zone config so that the user won't have to override again the next time
	// the want to ALTER the table locality.
	zc.ClearFieldsOfAllSubzones(zonepb.MultiRegionZoneConfigFields)

	zc.CopyFromZone(localityZoneConfig, zonepb.MultiRegionZoneConfigFields)

	hasNewSubzones := table.IsLocalityRegionalByRow()
	if hasNewSubzones {
		for _, region := range regionConfig.Regions() {
			subzoneConfig, err := regions.ZoneConfigForMultiRegionPartition(region, regionConfig)
			if err != nil {
				return false, zc, err
			}
			for _, idx := range table.NonDropIndexes() {
				zc.SetSubzone(zonepb.Subzone{
					IndexID:       uint32(idx.GetID()),
					PartitionName: string(region),
					Config:        subzoneConfig,
				})
			}
		}
	}
	return hasNewSubzones, zc, nil
}

// applyZoneConfigForMultiRegionTableOptionRemoveGlobalZoneConfig signals
// to remove the global zone configuration for a given table.
var applyZoneConfigForMultiRegionTableOptionRemoveGlobalZoneConfig = func(
	zc zonepb.ZoneConfig,
	regionConfig multiregion.RegionConfig,
	table catalog.TableDescriptor,
) (bool, zonepb.ZoneConfig, error) {
	zc.CopyFromZone(*zonepb.NewZoneConfig(), zonepb.MultiRegionZoneConfigFields)
	return false, zc, nil
}

func prepareZoneConfigForMultiRegionTable(
	ctx context.Context,
	txn descs.Txn,
	execCfg *ExecutorConfig,
	regionConfig multiregion.RegionConfig,
	table catalog.TableDescriptor,
	opts ...applyZoneConfigForMultiRegionTableOption,
) (*zoneConfigUpdate, error) {
	tableID := table.GetID()
	currentZoneConfigWithRaw, err := txn.Descriptors().GetZoneConfig(ctx, txn.KV(), tableID)
	if err != nil {
		return nil, err
	}
	if currentZoneConfigWithRaw == nil {
		currentZoneConfigWithRaw = zone.NewZoneConfigWithRawBytes(zonepb.NewZoneConfig(), nil)
	}
	newZoneConfig := *currentZoneConfigWithRaw.ZoneConfigProto()

	var hasNewSubzones bool
	for _, opt := range opts {
		newHasNewSubzones, modifiedNewZoneConfig, err := opt(
			newZoneConfig,
			regionConfig,
			table,
		)
		if err != nil {
			return nil, err
		}
		hasNewSubzones = newHasNewSubzones || hasNewSubzones
		newZoneConfig = modifiedNewZoneConfig
	}

	if regionConfig.HasSecondaryRegion() {

		var newLeasePreferences []zonepb.LeasePreference
		switch {
		case table.IsLocalityRegionalByTable():
			localityConfig := table.TableDesc().LocalityConfig.GetRegionalByTable()
			if region := localityConfig.Region; region != nil {
				newLeasePreferences = regions.SynthesizeLeasePreferences(*region, regionConfig.SecondaryRegion())
			} else {
				newLeasePreferences = regions.SynthesizeLeasePreferences(regionConfig.PrimaryRegion(), regionConfig.SecondaryRegion())
			}
		default:
			newLeasePreferences = regions.SynthesizeLeasePreferences(regionConfig.PrimaryRegion(), regionConfig.SecondaryRegion())
		}
		newZoneConfig.LeasePreferences = newLeasePreferences
	}

	// Mark the NumReplicas as 0 if we have subzones but no other features
	// in the zone config. This signifies a placeholder.
	// Note we do not use hasNewSubzones here as there may be existing subzones
	// on the zone config which may still be a placeholder.
	if regions.IsPlaceholderZoneConfigForMultiRegion(newZoneConfig) {
		newZoneConfig.NumReplicas = proto.Int32(0)
	}

	// Determine if we're rewriting or deleting the zone configuration.
	newZoneConfigIsEmpty := newZoneConfig.Equal(zonepb.NewZoneConfig())
	currentZoneConfigIsEmpty := currentZoneConfigWithRaw.ZoneConfigProto().Equal(zonepb.NewZoneConfig())
	rewriteZoneConfig := !newZoneConfigIsEmpty
	deleteZoneConfig := newZoneConfigIsEmpty && !currentZoneConfigIsEmpty

	if deleteZoneConfig {
		return &zoneConfigUpdate{id: tableID}, nil
	}
	if !rewriteZoneConfig {
		return nil, nil
	}

	if err := newZoneConfig.Validate(); err != nil {
		return nil, pgerror.Wrap(
			err,
			pgcode.CheckViolation,
			"could not validate zone config",
		)
	}
	if err := newZoneConfig.ValidateTandemFields(); err != nil {
		return nil, pgerror.Wrap(
			err,
			pgcode.CheckViolation,
			"could not validate zone config",
		)
	}
	return prepareZoneConfigWrites(
		ctx, execCfg, tableID, table, &newZoneConfig, currentZoneConfigWithRaw.GetRawBytesInStorage(), hasNewSubzones,
	)
}

// ApplyZoneConfigForMultiRegionTable applies zone config settings based
// on the options provided.
func ApplyZoneConfigForMultiRegionTable(
	ctx context.Context,
	txn descs.Txn,
	execCfg *ExecutorConfig,
	kvTrace bool,
	regionConfig multiregion.RegionConfig,
	table catalog.TableDescriptor,
	opts ...applyZoneConfigForMultiRegionTableOption,
) error {
	update, err := prepareZoneConfigForMultiRegionTable(ctx, txn, execCfg, regionConfig, table, opts...)
	if update == nil || err != nil {
		return err
	}
	_, err = writeZoneConfigUpdate(ctx, txn, kvTrace, update)
	return err
}

// generateAndValidateZoneConfigForMultiRegionDatabase returns a validated
// zone config generated by the region config.
func generateAndValidateZoneConfigForMultiRegionDatabase(
	ctx context.Context,
	regionProvider descs.RegionProvider,
	execConfig *ExecutorConfig,
	regionConfig multiregion.RegionConfig,
	currentZoneConfig *zonepb.ZoneConfig,
	validateLocalities bool,
) (zonepb.ZoneConfig, error) {
	// Build a zone config based on the RegionConfig information.
	dbZoneConfig := zonepb.ZoneConfig{}
	var err error
	// Validation for the updated zone config is done here.
	dbZoneConfig, err = zoneConfigForMultiRegionDatabase(regionConfig)
	if err != nil {
		return zonepb.ZoneConfig{}, err
	}

	// Finally revalidate everything. Validate only the completeZone config.
	if err := dbZoneConfig.Validate(); err != nil {
		return zonepb.ZoneConfig{}, pgerror.Wrap(err, pgcode.CheckViolation, "could not validate zone config")
	}

	if err := zonepb.ValidateNoRepeatKeysInZone(&dbZoneConfig); err != nil {
		return zonepb.ZoneConfig{}, err
	}

	if err := validateZoneAttrsAndLocalities(ctx, regionProvider, execConfig, currentZoneConfig, &dbZoneConfig); err != nil {
		// If we are validating localities this is fatal, otherwise let's log any
		// errors as warnings.
		if validateLocalities {
			return zonepb.ZoneConfig{}, err
		}
		log.Warningf(ctx, "ignoring locality validation error for DB zone config %v", err)
		err = nil
	}

	return dbZoneConfig, nil
}

// ApplyZoneConfigFromDatabaseRegionConfig applies a zone configuration to the
// database using the information in the supplied RegionConfig.
func ApplyZoneConfigFromDatabaseRegionConfig(
	ctx context.Context,
	dbID descpb.ID,
	regionConfig multiregion.RegionConfig,
	txn descs.Txn,
	execConfig *ExecutorConfig,
	validateLocalities bool,
	kvTrace bool,
) error {
	currentZone := zonepb.NewZoneConfig()
	if currentZoneConfigWithRaw, err := txn.Descriptors().GetZoneConfig(
		ctx, txn.KV(), dbID,
	); err != nil {
		return err
	} else if currentZoneConfigWithRaw != nil {
		currentZone = currentZoneConfigWithRaw.ZoneConfigProto()
	}

	// Build a zone config based on the RegionConfig information.
	dbZoneConfig, err := generateAndValidateZoneConfigForMultiRegionDatabase(ctx, txn.Regions(), execConfig, regionConfig, currentZone, validateLocalities)
	if err != nil {
		return err
	}

	return applyZoneConfigForMultiRegionDatabase(
		ctx,
		dbID,
		dbZoneConfig,
		txn,
		execConfig,
		kvTrace,
	)
}

// discardMultiRegionFieldsForDatabaseZoneConfig resets the multi-region zone
// config fields for a multi-region database.
func discardMultiRegionFieldsForDatabaseZoneConfig(
	ctx context.Context, dbID descpb.ID, txn descs.Txn, execConfig *ExecutorConfig, kvTrace bool,
) error {
	// Merge with an empty zone config.
	return applyZoneConfigForMultiRegionDatabase(
		ctx,
		dbID,
		*zonepb.NewZoneConfig(),
		txn,
		execConfig,
		kvTrace,
	)
}

func applyZoneConfigForMultiRegionDatabase(
	ctx context.Context,
	dbID descpb.ID,
	mergeZoneConfig zonepb.ZoneConfig,
	txn descs.Txn,
	execConfig *ExecutorConfig,
	kvTrace bool,
) error {
	currentZoneConfigWithRaw, err := txn.Descriptors().GetZoneConfig(ctx, txn.KV(), dbID)
	if err != nil {
		return err
	}
	if currentZoneConfigWithRaw == nil {
		currentZoneConfigWithRaw = zone.NewZoneConfigWithRawBytes(zonepb.NewZoneConfig(), nil)
	}
	newZoneConfig := *zonepb.NewZoneConfig()
	newZoneConfig = *currentZoneConfigWithRaw.ZoneConfigProto()
	newZoneConfig.CopyFromZone(
		mergeZoneConfig,
		zonepb.MultiRegionZoneConfigFields,
	)
	// If the new zone config is the same as a blank zone config, delete it.
	if newZoneConfig.Equal(zonepb.NewZoneConfig()) {
		_, err = writeZoneConfigUpdate(
			ctx,
			txn,
			kvTrace,
			&zoneConfigUpdate{id: dbID, zoneConfig: nil},
		)
		return err
	}
	if _, err := writeZoneConfig(
		ctx,
		txn,
		dbID,
		nil, /* table */
		&newZoneConfig,
		currentZoneConfigWithRaw.GetRawBytesInStorage(),
		execConfig,
		false, /* hasNewSubzones */
		kvTrace,
	); err != nil {
		return err
	}
	return nil
}

type refreshZoneConfigOptions struct {
	filterFunc func(tb *tabledesc.Mutable) bool
}

type refreshZoneConfigOption func(options *refreshZoneConfigOptions)

// refreshZoneConfigsForTables loops through all of the tables in the
// specified database and refreshes the zone configs for all tables.
func (p *planner) refreshZoneConfigsForTables(
	ctx context.Context, desc catalog.DatabaseDescriptor, refreshOpts ...refreshZoneConfigOption,
) error {
	opts := refreshZoneConfigOptions{
		filterFunc: func(_ *tabledesc.Mutable) bool { return true },
	}
	for _, f := range refreshOpts {
		f(&opts)
	}

	regionConfig, err := SynthesizeRegionConfig(ctx, p.txn, desc.GetID(), p.Descriptors())
	if err != nil {
		return err
	}
	// System databases will have regional by row tables configured
	// as survive zone.
	var surviveZoneCfg *multiregion.RegionConfig
	if desc.GetID() == keys.SystemDatabaseID {
		tempCfg, err := SynthesizeRegionConfig(ctx, p.txn, desc.GetID(), p.Descriptors(), multiregion.SynthesizeRegionConfigOptionForceSurvivalZone)
		if err != nil {
			return err
		}
		surviveZoneCfg = &tempCfg
	}

	return p.forEachMutableTableInDatabase(
		ctx,
		desc,
		func(ctx context.Context, scName string, tbDesc *tabledesc.Mutable) error {
			if opts.filterFunc(tbDesc) {
				targetRegionCfg := &regionConfig
				// On system databases we will force a survive zone config
				// for regional by row tables.
				if surviveZoneCfg != nil && tbDesc.IsLocalityRegionalByRow() {
					targetRegionCfg = surviveZoneCfg
				}
				return ApplyZoneConfigForMultiRegionTable(
					ctx,
					p.InternalSQLTxn(),
					p.ExecCfg(),
					p.extendedEvalCtx.Tracing.KVTracingEnabled(),
					*targetRegionCfg,
					tbDesc,
					ApplyZoneConfigForMultiRegionTableOptionTableAndIndexes,
				)
			}
			return nil
		},
	)
}

// refreshZoneConfigsForTablesWithValidation is basically the same as
// p.refreshZoneConfigsForTables(), except that here we first validate the
// new zone config for all tables before truly write each of the updates.
func (p *planner) refreshZoneConfigsForTablesWithValidation(
	ctx context.Context, desc catalog.DatabaseDescriptor, refreshOpts ...refreshZoneConfigOption,
) error {
	opts := refreshZoneConfigOptions{
		filterFunc: func(_ *tabledesc.Mutable) bool { return true },
	}
	for _, f := range refreshOpts {
		f(&opts)
	}

	regionConfig, err := SynthesizeRegionConfig(ctx, p.txn, desc.GetID(), p.Descriptors())
	if err != nil {
		return err
	}

	var zoneConfigUpdates []*zoneConfigUpdate

	// For each table in the database, we check validate the updated zone config,
	// and if there's no error, we add the zone config update in zoneConfigUpdates.
	if err := p.forEachMutableTableInDatabase(
		ctx,
		desc,
		func(ctx context.Context, scName string, tbDesc *tabledesc.Mutable) error {
			if opts.filterFunc(tbDesc) {
				update, err := prepareZoneConfigForMultiRegionTable(
					ctx,
					p.InternalSQLTxn(),
					p.ExecCfg(),
					regionConfig,
					tbDesc,
					ApplyZoneConfigForMultiRegionTableOptionTableAndIndexes,
				)
				if update == nil || err != nil {
					return err
				}
				zoneConfigUpdates = append(zoneConfigUpdates, update)
			}
			return nil
		},
	); err != nil {
		return err
	}
	// With all tables scanned and validated for the updated zone config, we now
	// can finally write the changes.
	// TODO(janexing): if any write failed, do we roll back? Same question to the
	// original p.forEachMutableTableInDatabase().
	for _, update := range zoneConfigUpdates {
		if _, err := writeZoneConfigUpdate(
			ctx,
			p.InternalSQLTxn(),
			p.ExtendedEvalContext().Tracing.KVTracingEnabled(),
			update,
		); err != nil {
			return err
		}
	}
	return nil

}

// WithOnlyGlobalTables modifies an refreshZoneConfigOptions to only apply to
// global tables.
func WithOnlyGlobalTables(opts *refreshZoneConfigOptions) {
	opts.filterFunc = func(tb *tabledesc.Mutable) bool {
		return tb.IsLocalityGlobal()
	}
}

// WithOnlyRegionalTablesAndGlobalTables modifies an refreshZoneConfigOptions to
// only apply to global tables and regional tables.
func WithOnlyRegionalTablesAndGlobalTables(opts *refreshZoneConfigOptions) {
	opts.filterFunc = func(tb *tabledesc.Mutable) bool {
		return tb.IsLocalityGlobal() || tb.IsLocalityRegionalByTable()
	}
}

// maybeInitializeMultiRegionDatabase initializes a multi-region database if
// there is a region config on the database descriptor and serves as a
// pass-through otherwise.
// Initializing a multi-region database involves creating the multi-region enum
// seeded with the given regionNames and applying the database-level zone
// configurations.
func (p *planner) maybeInitializeMultiRegionDatabase(
	ctx context.Context, desc *dbdesc.Mutable, regionConfig *multiregion.RegionConfig,
) error {
	// If the database is not a multi-region database, there's no work to be done.
	if !desc.IsMultiRegion() {
		return nil
	}

	// Create the multi-region enum.
	regionLabels := make(tree.EnumValueList, 0, len(regionConfig.Regions()))
	for _, regionName := range regionConfig.Regions() {
		regionLabels = append(regionLabels, tree.EnumValue(regionName))
	}

	if err := p.createEnumWithID(
		ctx,
		p.EvalContext(),
		regionConfig.RegionEnumID(),
		regionLabels,
		desc,
		tree.NewQualifiedTypeName(desc.Name, catconstants.PublicSchemaName, tree.RegionEnum),
		EnumTypeMultiRegion,
	); err != nil {
		return err
	}

	// Create the database-level zone configuration.
	if err := ApplyZoneConfigFromDatabaseRegionConfig(
		ctx,
		desc.ID,
		*regionConfig,
		p.InternalSQLTxn(),
		p.execCfg,
		true, /* validateLocalities */
		p.extendedEvalCtx.Tracing.KVTracingEnabled(),
	); err != nil {
		return err
	}

	return nil
}

// ValidateAllMultiRegionZoneConfigsInCurrentDatabase is part of the eval.DatabaseCatalog interface.
func (p *planner) ValidateAllMultiRegionZoneConfigsInCurrentDatabase(ctx context.Context) error {
	dbDesc, err := p.Descriptors().ByNameWithLeased(p.txn).Get().Database(ctx, p.CurrentDatabase())
	if err != nil {
		return err
	}

	if err := p.checkPrivilegesForMultiRegionOp(ctx, dbDesc); err != nil {
		return err
	}

	if !dbDesc.IsMultiRegion() {
		return nil
	}
	regionConfig, err := SynthesizeRegionConfig(
		ctx,
		p.txn,
		dbDesc.GetID(),
		p.Descriptors(),
		multiregion.SynthesizeRegionConfigOptionForValidation,
	)
	if err != nil {
		return err
	}
	return p.validateAllMultiRegionZoneConfigsInDatabase(
		ctx,
		dbDesc,
		&zoneConfigForMultiRegionValidatorValidation{
			zoneConfigForMultiRegionValidatorExistingMultiRegionObject: zoneConfigForMultiRegionValidatorExistingMultiRegionObject{
				regionConfig: regionConfig,
			},
		},
	)
}

// ResetMultiRegionZoneConfigsForTable takes a table ID and resets that table's
// zone configurations to match what would have originally been set by the
// multi-region syntax.
func (p *planner) ResetMultiRegionZoneConfigsForTable(
	ctx context.Context, id int64, forceZoneSurvival bool,
) error {
	desc, err := p.Descriptors().MutableByID(p.txn).Table(ctx, descpb.ID(id))
	if err != nil {
		return errors.Wrapf(err, "error resolving referenced table ID %d", id)
	}

	if err := p.checkPrivilegesForMultiRegionOp(ctx, desc); err != nil {
		return err
	}

	// If the table is not a multi-region table, there's no work to be done
	// here.
	if desc.LocalityConfig == nil {
		return nil
	}
	var opts []multiregion.SynthesizeRegionConfigOption
	// System databases will have regional by row tables configured
	// as survive zone.
	if forceZoneSurvival {
		opts = []multiregion.SynthesizeRegionConfigOption{
			multiregion.SynthesizeRegionConfigOptionForceSurvivalZone,
		}
	}
	regionConfig, err := SynthesizeRegionConfig(ctx, p.txn, desc.GetParentID(), p.Descriptors(), opts...)
	if err != nil {
		return err
	}

	return ApplyZoneConfigForMultiRegionTable(
		ctx,
		p.InternalSQLTxn(),
		p.ExecCfg(),
		p.extendedEvalCtx.Tracing.KVTracingEnabled(),
		regionConfig,
		desc,
		ApplyZoneConfigForMultiRegionTableOptionTableAndIndexes,
	)
}

// ResetMultiRegionZoneConfigsForDatabase takes a database ID and resets that
// database's zone configuration to match what would have originally been set by
// the multi-region syntax.
func (p *planner) ResetMultiRegionZoneConfigsForDatabase(ctx context.Context, id int64) error {
	dbDesc, err := p.Descriptors().ByIDWithoutLeased(p.txn).WithoutNonPublic().Get().Database(ctx, descpb.ID(id))
	if err != nil {
		return err
	}

	if err := p.checkPrivilegesForMultiRegionOp(ctx, dbDesc); err != nil {
		return err
	}

	if !dbDesc.IsMultiRegion() {
		// Nothing to do.
		return nil
	}

	regionConfig, err := SynthesizeRegionConfig(ctx, p.txn, dbDesc.GetID(), p.Descriptors())
	if err != nil {
		return err
	}

	// Update the database's zone configuration.
	if err := ApplyZoneConfigFromDatabaseRegionConfig(
		ctx,
		dbDesc.GetID(),
		regionConfig,
		p.InternalSQLTxn(),
		p.execCfg,
		true, /* validateLocalities */
		p.extendedEvalCtx.Tracing.KVTracingEnabled(),
	); err != nil {
		return err
	}
	return nil
}

func (p *planner) validateAllMultiRegionZoneConfigsInDatabase(
	ctx context.Context,
	dbDesc catalog.DatabaseDescriptor,
	zoneConfigForMultiRegionValidator zoneConfigForMultiRegionValidator,
) error {
	var ids []descpb.ID
	if err := p.forEachMutableTableInDatabase(
		ctx,
		dbDesc,
		func(ctx context.Context, scName string, tbDesc *tabledesc.Mutable) error {
			ids = append(ids, tbDesc.GetID())
			return nil
		},
	); err != nil {
		return err
	}
	ids = append(ids, dbDesc.GetID())

	zoneConfigs, err := p.Descriptors().GetZoneConfigs(ctx, p.Txn(), ids...)
	if err != nil {
		return err
	}

	var dbZoneConfig *zonepb.ZoneConfig
	if zc := zoneConfigs[dbDesc.GetID()]; zc != nil {
		dbZoneConfig = zc.ZoneConfigProto()
	}
	if err := p.validateZoneConfigForMultiRegionDatabase(
		dbDesc,
		dbZoneConfig,
		zoneConfigForMultiRegionValidator,
	); err != nil {
		return err
	}

	return p.forEachMutableTableInDatabase(
		ctx,
		dbDesc,
		func(ctx context.Context, scName string, tbDesc *tabledesc.Mutable) error {
			var tbZoneConfig *zonepb.ZoneConfig
			if zc := zoneConfigs[tbDesc.GetID()]; zc != nil {
				tbZoneConfig = zc.ZoneConfigProto()
			}
			return p.validateZoneConfigForMultiRegionTable(
				dbDesc,
				tbDesc,
				tbZoneConfig,
				zoneConfigForMultiRegionValidator,
			)
		},
	)
}

// CurrentDatabaseRegionConfig is part of the eval.RegionOperator interface.
// CurrentDatabaseRegionConfig uses the cache to synthesize the RegionConfig
// and as such is intended for DML use. It returns nil
// if the current database is not multi-region enabled.
func (p *planner) CurrentDatabaseRegionConfig(
	ctx context.Context,
) (eval.DatabaseRegionConfig, error) {
	dbDesc, err := p.Descriptors().ByNameWithLeased(p.txn).Get().Database(ctx, p.CurrentDatabase())
	if err != nil {
		return nil, err
	}

	if !dbDesc.IsMultiRegion() {
		return nil, nil
	}

	return SynthesizeRegionConfig(
		ctx,
		p.txn,
		dbDesc.GetID(),
		p.Descriptors(),
		multiregion.SynthesizeRegionConfigOptionUseCache,
	)
}

// SynthesizeRegionConfig returns a RegionConfig representing the user
// configured state of a multi-region database by coalescing state from both
// the database descriptor and multi-region type descriptor. By default, it
// avoids the cache and is intended for use by DDL statements.
func SynthesizeRegionConfig(
	ctx context.Context,
	txn *kv.Txn,
	dbID descpb.ID,
	descsCol *descs.Collection,
	opts ...multiregion.SynthesizeRegionConfigOption,
) (multiregion.RegionConfig, error) {
	return regions.SynthesizeRegionConfigInTxn(ctx, txn, dbID, descsCol, opts...)
}

// GetLocalityRegionEnumPhysicalRepresentation returns the physical
// representation of the given locality stored in the multi-region enum type
// associated with dbID. If the given locality isn't found, the physical
// representation of the primary region in dbID will be returned instead.
// This returns an ErrNotMultiRegionDatabase error if the database isn't
// multi-region.
func GetLocalityRegionEnumPhysicalRepresentation(
	ctx context.Context, db descs.DB, dbID descpb.ID, locality roachpb.Locality,
) ([]byte, error) {
	var enumReps map[catpb.RegionName][]byte
	var primaryRegion catpb.RegionName
	if err := db.DescsTxn(ctx, func(
		ctx context.Context, txn descs.Txn,
	) error {
		enumReps, primaryRegion = nil, "" // reset for retry
		var err error
		enumReps, primaryRegion, err = GetRegionEnumRepresentations(
			ctx, txn.KV(), dbID, txn.Descriptors(),
		)
		return err
	}); err != nil {
		return nil, err
	}

	// The primary region will be used if no region was provided through the
	// locality flag.
	currentRegion, _ := locality.Find("region")
	if enumValue, ok := enumReps[catpb.RegionName(currentRegion)]; ok {
		return enumValue, nil
	}
	if enumValue, ok := enumReps[primaryRegion]; ok {
		return enumValue, nil
	}
	// This shouldn't be the case since if a primary region is defined for the
	// database, there should exist a corresponding enum member value.
	return nil, errors.AssertionFailedf("primary region not found")
}

// GetRegionEnumRepresentations returns representations stored in the
// multi-region enum type associated with dbID, and the primary region of it.
// An ErrNotMultiRegionDatabase error will be returned if the database isn't
// multi-region.
func GetRegionEnumRepresentations(
	ctx context.Context, txn *kv.Txn, dbID descpb.ID, descsCol *descs.Collection,
) (enumReps map[catpb.RegionName][]byte, primaryRegion catpb.RegionName, err error) {
	dbDesc, regionEnumDesc, err := getDBAndRegionEnumDescs(
		ctx, txn, dbID, descsCol, false /* useCache */, false /* includeOffline */)
	if err != nil {
		return nil, "", err
	}

	enumReps = make(map[catpb.RegionName][]byte)
	for ord := 0; ord < regionEnumDesc.NumEnumMembers(); ord++ {
		if regionEnumDesc.IsMemberReadOnly(ord) {
			continue
		}
		enumReps[catpb.RegionName(
			regionEnumDesc.GetMemberLogicalRepresentation(ord),
		)] = regionEnumDesc.GetMemberPhysicalRepresentation(ord)
	}
	return enumReps, dbDesc.GetRegionConfig().PrimaryRegion, nil
}

// getDBAndRegionEnumDescs returns descriptors for both the database and
// multi-region enum type. If the database isn't multi-region, an
// ErrNotMultiRegionDatabase error will be returned.
func getDBAndRegionEnumDescs(
	ctx context.Context,
	txn *kv.Txn,
	dbID descpb.ID,
	descsCol *descs.Collection,
	useCache bool,
	includeOffline bool,
) (dbDesc catalog.DatabaseDescriptor, regionEnumDesc catalog.RegionEnumTypeDescriptor, _ error) {
	var b descs.ByIDGetterBuilder
	if useCache {
		b = descsCol.ByIDWithLeased(txn)
	} else {
		b = descsCol.ByIDWithoutLeased(txn)
	}
	if !includeOffline {
		b = b.WithoutOffline()
	}
	g := b.Get()
	dbDesc, err := g.Database(ctx, dbID)
	if err != nil {
		return nil, nil, err
	}
	if !dbDesc.IsMultiRegion() {
		return nil, nil, regions.ErrNotMultiRegionDatabase
	}
	regionEnumID, err := dbDesc.MultiRegionEnumID()
	if err != nil {
		return nil, nil, err
	}
	typeDesc, err := g.Type(ctx, regionEnumID)
	if err != nil {
		return nil, nil, err
	}
	regionEnumDesc = typeDesc.AsRegionEnumTypeDescriptor()
	if regionEnumDesc == nil {
		return nil, nil, errors.AssertionFailedf(
			"expected region enum type, not %s for type %q (%d)",
			typeDesc.GetKind(), typeDesc.GetName(), typeDesc.GetID())
	}
	return dbDesc, regionEnumDesc, nil
}

// blockDiscardOfZoneConfigForMultiRegionObject determines if discarding the
// zone configuration of a multi-region table, index or partition should be
// blocked. We only block the discard if the multi-region abstractions have
// created the zone configuration. Note that this function relies on internal
// knowledge of which table locality patterns write zone configurations. We do
// things this way to avoid having to read the zone configurations directly and
// do a more explicit comparison (with a generated zone configuration). If, down
// the road, the rules around writing zone configurations change, the tests in
// multi_region_zone_configs will fail and this function will need updating.
func blockDiscardOfZoneConfigForMultiRegionObject(
	zs tree.ZoneSpecifier, tblDesc catalog.TableDescriptor,
) (bool, error) {
	isIndex := zs.TableOrIndex.Index != ""
	isPartition := zs.Partition != ""

	if isPartition {
		// Multi-region abstractions only set partition-level zone configs for
		// REGIONAL BY ROW tables.
		if tblDesc.IsLocalityRegionalByRow() {
			return true, nil
		}
	} else if isIndex {
		// Multi-region will never set a zone config on an index, so no need to
		// error if the user wants to drop the index zone config.
		return false, nil
	} else {
		// It's a table zone config that the user is trying to discard. This
		// should only be present on GLOBAL and REGIONAL BY TABLE tables in a
		// specified region.
		if tblDesc.IsLocalityGlobal() {
			return true, nil
		} else if tblDesc.IsLocalityRegionalByTable() {
			if tblDesc.GetLocalityConfig().GetRegionalByTable().Region != nil &&
				tree.Name(*tblDesc.GetLocalityConfig().GetRegionalByTable().Region) !=
					tree.PrimaryRegionNotSpecifiedName {
				return true, nil
			}
		} else if tblDesc.IsLocalityRegionalByRow() {
			// For REGIONAL BY ROW tables, no need to error if we're setting a
			// table level zone config.
			return false, nil
		} else {
			return false, errors.AssertionFailedf(
				"unknown table locality: %v",
				tblDesc.GetLocalityConfig(),
			)
		}
	}
	return false, nil
}

// CheckZoneConfigChangePermittedForMultiRegion checks if a zone config
// change is permitted for a multi-region database, table, index or partition.
// The change is permitted iff it is not modifying a protected multi-region
// field of the zone configs (as defined by zonepb.MultiRegionZoneConfigFields).
func (p *planner) CheckZoneConfigChangePermittedForMultiRegion(
	ctx context.Context, zs tree.ZoneSpecifier, options tree.KVOptions,
) error {
	// If the user has specified that they're overriding, then the world is
	// their oyster.
	if p.SessionData().OverrideMultiRegionZoneConfigEnabled {
		// Note that we increment the telemetry counter unconditionally here.
		// It's possible that this will lead to over-counting as the user may
		// have left the override on and is now updating a zone configuration
		// that is not protected by the multi-region abstractions. To get finer
		// grained counting however, would be more difficult to code, and may
		// not even prove to be that valuable, so we have decided to live with
		// the potential for over-counting.
		telemetry.Inc(sqltelemetry.OverrideMultiRegionZoneConfigurationUser)
		return nil
	}

	var err error
	var tblDesc catalog.TableDescriptor
	isDB := false
	// Check if what we're altering is a multi-region entity.
	if zs.Database != "" {
		isDB = true
		dbDesc, err := p.Descriptors().ByNameWithLeased(p.txn).Get().Database(ctx, string(zs.Database))
		if err != nil {
			return err
		}
		if dbDesc.GetRegionConfig() == nil {
			// Not a multi-region database, we're done here.
			return nil
		}
	} else {
		// We're dealing with a table, index, or partition zone configuration
		// change.  Get the table descriptor so we can determine if this is a
		// multi-region table/index/partition.
		tblDesc, err = p.resolveTableForZone(ctx, &zs)
		if err != nil {
			return err
		}
		if tblDesc == nil || tblDesc.GetLocalityConfig() == nil {
			// Not a multi-region table, we're done here.
			return nil
		}
	}

	hint := "to override this error, SET override_multi_region_zone_config = true and reissue the command"

	// The request is to discard the zone configuration. Error in cases where
	// the zone configuration being discarded was created by the multi-region
	// abstractions.
	if options == nil {
		needToError := false
		// Determine if this zone config that we're trying to discard is
		// supposed to be there.
		if isDB {
			needToError = true
		} else {
			needToError, err = blockDiscardOfZoneConfigForMultiRegionObject(zs, tblDesc)
			if err != nil {
				return err
			}
		}

		if needToError {
			// User is trying to update a zone config value that's protected for
			// multi-region databases. Return the constructed error.
			err := errors.WithDetail(errors.Newf(
				"attempting to discard the zone configuration of a multi-region entity"),
				"discarding a multi-region zone configuration may result in sub-optimal performance or behavior",
			)
			return errors.WithHint(err, hint)
		}
	}

	// This is clearly an n^2 operation, but since there are only a single
	// digit number of zone config keys, it's likely faster to do it this way
	// than incur the memory allocation of creating a map.
	for _, opt := range options {
		for _, cfg := range zonepb.MultiRegionZoneConfigFields {
			if opt.Key == cfg {
				// User is trying to update a zone config value that's protected for
				// multi-region databases. Return the constructed error.
				err := errors.Newf("attempting to modify protected field %q of a multi-region zone configuration",
					string(opt.Key),
				)
				return errors.WithHint(err, hint)
			}
		}
	}

	return nil
}

// zoneConfigForMultiRegionValidator is an interface representing
// actions to take when validating a zone config for multi-region
// purposes.
type zoneConfigForMultiRegionValidator interface {
	getExpectedDatabaseZoneConfig() (zonepb.ZoneConfig, error)
	getExpectedTableZoneConfig(desc catalog.TableDescriptor) (zonepb.ZoneConfig, error)
	transitioningRegions() catpb.RegionNames

	newMismatchFieldError(descType string, descName string, mismatch zonepb.DiffWithZoneMismatch) error
	newMissingSubzoneError(descType string, descName string, mismatch zonepb.DiffWithZoneMismatch) error
	newExtraSubzoneError(descType string, descName string, mismatch zonepb.DiffWithZoneMismatch) error
}

// zoneConfigForMultiRegionValidatorSetInitialRegion implements
// interface zoneConfigForMultiRegionValidator.
type zoneConfigForMultiRegionValidatorSetInitialRegion struct{}

var _ zoneConfigForMultiRegionValidator = (*zoneConfigForMultiRegionValidatorSetInitialRegion)(nil)

func (v *zoneConfigForMultiRegionValidatorSetInitialRegion) getExpectedDatabaseZoneConfig() (
	zonepb.ZoneConfig,
	error,
) {
	// For set initial region, we want no multi-region fields to be set.
	return *zonepb.NewZoneConfig(), nil
}

func (v *zoneConfigForMultiRegionValidatorSetInitialRegion) transitioningRegions() catpb.RegionNames {
	// There are no transitioning regions at setup time.
	return nil
}

func (v *zoneConfigForMultiRegionValidatorSetInitialRegion) getExpectedTableZoneConfig(
	desc catalog.TableDescriptor,
) (zonepb.ZoneConfig, error) {
	// For set initial region, we want no multi-region fields to be set.
	return *zonepb.NewZoneConfig(), nil
}

func (v *zoneConfigForMultiRegionValidatorSetInitialRegion) wrapErr(err error) error {
	// We currently do not allow "inherit from parent" behavior, so one must
	// discard the zone config before continuing.
	// COPY FROM PARENT copies the value but does not inherit.
	// This can be replaced with the override session variable hint when it is
	// available.
	return errors.WithHintf(
		err,
		"discard the zone config using CONFIGURE ZONE DISCARD before continuing",
	)
}

func (v *zoneConfigForMultiRegionValidatorSetInitialRegion) newMismatchFieldError(
	descType string, descName string, mismatch zonepb.DiffWithZoneMismatch,
) error {
	return v.wrapErr(
		pgerror.Newf(
			pgcode.InvalidObjectDefinition,
			"zone configuration for %s %s has field %q set which will be overwritten when setting the the initial PRIMARY REGION (expected=%s actual=%s)",
			descType,
			descName,
			mismatch.Field,
			mismatch.Expected,
			mismatch.Actual,
		),
	)
}

func (v *zoneConfigForMultiRegionValidatorSetInitialRegion) newMissingSubzoneError(
	descType string, descName string, _ zonepb.DiffWithZoneMismatch,
) error {
	// There can never be a missing subzone as we only compare against
	// blank zone configs.
	return errors.AssertionFailedf(
		"unexpected missing subzone for %s %s",
		descType,
		descName,
	)
}

func (v *zoneConfigForMultiRegionValidatorSetInitialRegion) newExtraSubzoneError(
	descType string, descName string, mismatch zonepb.DiffWithZoneMismatch,
) error {
	return v.wrapErr(
		pgerror.Newf(
			pgcode.InvalidObjectDefinition,
			"zone configuration for %s %s has field %q set which will be overwritten when setting the initial PRIMARY REGION (expected=%s actual=%s)",
			descType,
			descName,
			mismatch.Field,
			mismatch.Expected,
			mismatch.Actual,
		),
	)
}

// zoneConfigForMultiRegionValidatorExistingMultiRegionObject partially implements
// the zoneConfigForMultiRegionValidator interface.
type zoneConfigForMultiRegionValidatorExistingMultiRegionObject struct {
	regionConfig multiregion.RegionConfig
}

func (v *zoneConfigForMultiRegionValidatorExistingMultiRegionObject) getExpectedDatabaseZoneConfig() (
	zonepb.ZoneConfig,
	error,
) {
	return zoneConfigForMultiRegionDatabase(v.regionConfig)
}

func (v *zoneConfigForMultiRegionValidatorExistingMultiRegionObject) getExpectedTableZoneConfig(
	desc catalog.TableDescriptor,
) (zonepb.ZoneConfig, error) {
	_, expectedZoneConfig, err := ApplyZoneConfigForMultiRegionTableOptionTableAndIndexes(
		*zonepb.NewZoneConfig(),
		v.regionConfig,
		desc,
	)
	if err != nil {
		return zonepb.ZoneConfig{}, err
	}
	return expectedZoneConfig, err
}

func (v *zoneConfigForMultiRegionValidatorExistingMultiRegionObject) transitioningRegions() catpb.RegionNames {
	return v.regionConfig.TransitioningRegions()
}

// zoneConfigForMultiRegionValidatorModifiedByUser implements
// interface zoneConfigForMultiRegionValidator.
type zoneConfigForMultiRegionValidatorModifiedByUser struct {
	zoneConfigForMultiRegionValidatorExistingMultiRegionObject
}

var _ zoneConfigForMultiRegionValidator = (*zoneConfigForMultiRegionValidatorModifiedByUser)(nil)

func (v *zoneConfigForMultiRegionValidatorModifiedByUser) newMismatchFieldError(
	descType string, descName string, mismatch zonepb.DiffWithZoneMismatch,
) error {
	return v.wrapErr(
		pgerror.Newf(
			pgcode.InvalidObjectDefinition,
			"attempting to update zone configuration for %s %s which contains modified field %q (expected=%s actual=%s)",
			descType,
			descName,
			mismatch.Field,
			mismatch.Expected,
			mismatch.Actual,
		),
	)
}

func (v *zoneConfigForMultiRegionValidatorModifiedByUser) wrapErr(err error) error {
	err = errors.WithDetail(
		err,
		"the attempted operation will overwrite a user modified field",
	)
	return errors.WithHint(
		err,
		"to proceed with the overwrite, SET override_multi_region_zone_config = true, "+
			"and reissue the statement",
	)
}

func (v *zoneConfigForMultiRegionValidatorModifiedByUser) newMissingSubzoneError(
	descType string, descName string, _ zonepb.DiffWithZoneMismatch,
) error {
	return v.wrapErr(
		pgerror.Newf(
			pgcode.InvalidObjectDefinition,
			"attempting to update zone config which is missing an expected zone configuration for %s %s",
			descType,
			descName,
		),
	)
}

func (v *zoneConfigForMultiRegionValidatorModifiedByUser) newExtraSubzoneError(
	descType string, descName string, mismatch zonepb.DiffWithZoneMismatch,
) error {
	return v.wrapErr(
		pgerror.Newf(
			pgcode.InvalidObjectDefinition,
			"attempting to update zone config which contains an extra zone configuration for %s %s with field %s populated (expected=%s actual=%s)",
			descType,
			descName,
			mismatch.Field,
			mismatch.Expected,
			mismatch.Actual,
		),
	)
}

// zoneConfigForMultiRegionValidatorValidation implements
// interface zoneConfigForMultiRegionValidator.
type zoneConfigForMultiRegionValidatorValidation struct {
	zoneConfigForMultiRegionValidatorExistingMultiRegionObject
}

var _ zoneConfigForMultiRegionValidator = (*zoneConfigForMultiRegionValidatorValidation)(nil)

func (v *zoneConfigForMultiRegionValidatorValidation) newMismatchFieldError(
	descType string, descName string, mismatch zonepb.DiffWithZoneMismatch,
) error {
	return pgerror.Newf(
		pgcode.InvalidObjectDefinition,
		"zone configuration for %s %s contains incorrectly configured field %q (expected=%s actual=%s)",
		descType,
		descName,
		mismatch.Field,
		mismatch.Expected,
		mismatch.Actual,
	)
}

func (v *zoneConfigForMultiRegionValidatorValidation) newMissingSubzoneError(
	descType string, descName string, _ zonepb.DiffWithZoneMismatch,
) error {
	return pgerror.Newf(
		pgcode.InvalidObjectDefinition,
		"missing zone configuration for %s %s",
		descType,
		descName,
	)
}

func (v *zoneConfigForMultiRegionValidatorValidation) newExtraSubzoneError(
	descType string, descName string, mismatch zonepb.DiffWithZoneMismatch,
) error {
	return pgerror.Newf(
		pgcode.InvalidObjectDefinition,
		"extraneous zone configuration for %s %s with field %s populated (expected=%s actual=%s)",
		descType,
		descName,
		mismatch.Field,
		mismatch.Expected,
		mismatch.Actual,
	)
}

// validateZoneConfigForMultiRegionDatabaseWasNotModifiedByUser validates that
// the zone configuration was not modified by the user. The function is intended
// to be called in cases where a multi-region operation will overwrite the
// database zone configuration and we wish to warn the user about that before
// it occurs (and require the FORCE option to proceed).
func (p *planner) validateZoneConfigForMultiRegionDatabaseWasNotModifiedByUser(
	ctx context.Context, dbDesc catalog.DatabaseDescriptor,
) error {
	// If the user is overriding, our work here is done.
	if p.SessionData().OverrideMultiRegionZoneConfigEnabled {
		telemetry.Inc(sqltelemetry.OverrideMultiRegionDatabaseZoneConfigurationSystem)
		return nil
	}
	currentZoneConfig, err := p.Descriptors().GetZoneConfig(ctx, p.Txn(), dbDesc.GetID())
	if err != nil {
		return err
	}
	if currentZoneConfig == nil {
		currentZoneConfig = zone.NewZoneConfigWithRawBytes(zonepb.NewZoneConfig(), nil)
	}

	regionConfig, err := SynthesizeRegionConfig(
		ctx,
		p.txn,
		dbDesc.GetID(),
		p.Descriptors(),
		multiregion.SynthesizeRegionConfigOptionForValidation,
	)
	if err != nil {
		return err
	}
	return p.validateZoneConfigForMultiRegionDatabase(
		dbDesc,
		currentZoneConfig.ZoneConfigProto(),
		&zoneConfigForMultiRegionValidatorModifiedByUser{
			zoneConfigForMultiRegionValidatorExistingMultiRegionObject: zoneConfigForMultiRegionValidatorExistingMultiRegionObject{
				regionConfig: regionConfig,
			},
		},
	)
}

// validateZoneConfigForMultiRegionDatabase validates that the zone config
// for the databases matches as the multi-region database definition.
func (p *planner) validateZoneConfigForMultiRegionDatabase(
	dbDesc catalog.DatabaseDescriptor,
	currentZoneConfig *zonepb.ZoneConfig,
	zoneConfigForMultiRegionValidator zoneConfigForMultiRegionValidator,
) error {
	if currentZoneConfig == nil ||
		// If this is the system database, and it has the configuration we inject
		// for the system database at startup, we should treat it as though it has
		// no zone configs set.
		(dbDesc.GetID() == keys.SystemDatabaseID &&
			currentZoneConfig.Equal(zonepb.DefaultSystemZoneConfigRef())) {
		currentZoneConfig = zonepb.NewZoneConfig()
	}

	expectedZoneConfig, err := zoneConfigForMultiRegionValidator.getExpectedDatabaseZoneConfig()
	if err != nil {
		return err
	}

	same, mismatch, err := currentZoneConfig.DiffWithZone(
		expectedZoneConfig,
		zonepb.MultiRegionZoneConfigFields,
	)
	if err != nil {
		return err
	}
	if !same {
		dbName := tree.Name(dbDesc.GetName())
		return zoneConfigForMultiRegionValidator.newMismatchFieldError(
			"database",
			dbName.String(),
			mismatch,
		)
	}
	return nil
}

// validateZoneConfigForMultiRegionTableWasNotModifiedByUser validates that
// the table's zone configuration was not modified by the user. The function is
// intended to be called in cases where a multi-region operation will overwrite
// the table's (or index's/partition's) zone configuration and we wish to warn
// the user about that before it occurs (and require the
// override_multi_region_zone_config session variable to be set).
func (p *planner) validateZoneConfigForMultiRegionTableWasNotModifiedByUser(
	ctx context.Context, dbDesc catalog.DatabaseDescriptor, desc *tabledesc.Mutable,
) error {
	// If the user is overriding, or this is not a multi-region table our work here
	// is done.
	if p.SessionData().OverrideMultiRegionZoneConfigEnabled || desc.GetLocalityConfig() == nil {
		telemetry.Inc(sqltelemetry.OverrideMultiRegionTableZoneConfigurationSystem)
		return nil
	}
	currentZoneConfig, err := p.Descriptors().GetZoneConfig(ctx, p.Txn(), desc.GetID())
	if err != nil {
		return err
	}
	if currentZoneConfig == nil {
		currentZoneConfig = zone.NewZoneConfigWithRawBytes(zonepb.NewZoneConfig(), nil)
	}

	regionConfig, err := SynthesizeRegionConfig(
		ctx,
		p.txn,
		dbDesc.GetID(),
		p.Descriptors(),
		multiregion.SynthesizeRegionConfigOptionForValidation,
	)
	if err != nil {
		return err
	}

	return p.validateZoneConfigForMultiRegionTable(
		dbDesc,
		desc,
		currentZoneConfig.ZoneConfigProto(),
		&zoneConfigForMultiRegionValidatorModifiedByUser{
			zoneConfigForMultiRegionValidatorExistingMultiRegionObject: zoneConfigForMultiRegionValidatorExistingMultiRegionObject{
				regionConfig: regionConfig,
			},
		},
	)
}

// validateZoneConfigForMultiRegionTableOptions validates that
// the multi-region fields of the table's zone configuration
// matches what is expected for the given table.
func (p *planner) validateZoneConfigForMultiRegionTable(
	dbDesc catalog.DatabaseDescriptor,
	desc catalog.TableDescriptor,
	currentZoneConfig *zonepb.ZoneConfig,
	zoneConfigForMultiRegionValidator zoneConfigForMultiRegionValidator,
) error {
	if currentZoneConfig == nil {
		currentZoneConfig = zonepb.NewZoneConfig()
	}

	tableName := tree.Name(desc.GetName())

	expectedZoneConfig, err := zoneConfigForMultiRegionValidator.getExpectedTableZoneConfig(
		desc,
	)
	if err != nil {
		return err
	}

	// When there is a transition to/from REGIONAL BY ROW, the new indexes
	// being set up will have zone configs which mismatch with the old
	// table locality config. As we validate against the old table locality
	// config (as the new indexes are not swapped in yet), exclude these
	// indexes from any zone configuration validation.
	regionalByRowNewIndexes := make(map[uint32]struct{})
	for _, mut := range desc.AllMutations() {
		if pkSwap := mut.AsPrimaryKeySwap(); pkSwap != nil {
			if pkSwap.HasLocalityConfig() {
				_ = pkSwap.ForEachNewIndexIDs(func(id descpb.IndexID) error {
					regionalByRowNewIndexes[uint32(id)] = struct{}{}
					if idx := catalog.FindCorrespondingTemporaryIndexByID(desc, id); idx != nil {
						regionalByRowNewIndexes[uint32(idx.GetID())] = struct{}{}
					}
					return nil
				})

			}
			// There can only be one pkSwap at a time, so break now.
			break
		}
	}

	// Some transitioning subzones may remain on the zone configuration until it is cleaned up
	// at a later step. Remove these as well as the regional by row new indexes.
	subzoneIndexIDsToDiff := make(map[uint32]tree.Name, len(desc.NonDropIndexes()))
	for _, idx := range desc.NonDropIndexes() {
		if _, ok := regionalByRowNewIndexes[uint32(idx.GetID())]; !ok {
			subzoneIndexIDsToDiff[uint32(idx.GetID())] = tree.Name(idx.GetName())
		}
	}

	// Do not compare partitioning for these regions, as they may be in a
	// transitioning state.
	transitioningRegions := make(map[string]struct{}, len(zoneConfigForMultiRegionValidator.transitioningRegions()))
	for _, transitioningRegion := range zoneConfigForMultiRegionValidator.transitioningRegions() {
		transitioningRegions[string(transitioningRegion)] = struct{}{}
	}

	// We only want to compare against the list of subzones on active indexes
	// and partitions, so filter the subzone list based on the
	// subzoneIndexIDsToDiff computed above.
	filteredCurrentZoneConfigSubzones := currentZoneConfig.Subzones[:0]
	for _, c := range currentZoneConfig.Subzones {
		if c.PartitionName != "" {
			if _, ok := transitioningRegions[c.PartitionName]; ok {
				continue
			}
		}
		if _, ok := subzoneIndexIDsToDiff[c.IndexID]; !ok {
			continue
		}
		filteredCurrentZoneConfigSubzones = append(filteredCurrentZoneConfigSubzones, c)
	}
	currentZoneConfig.Subzones = filteredCurrentZoneConfigSubzones
	// Strip the placeholder status if there are no active subzones on the current
	// zone config.
	if len(filteredCurrentZoneConfigSubzones) == 0 && currentZoneConfig.IsSubzonePlaceholder() {
		currentZoneConfig.NumReplicas = nil
	}

	// Remove regional by row new indexes and transitioning partitions from the expected zone config.
	// These will be incorrect as ApplyZoneConfigForMultiRegionTableOptionTableAndIndexes
	// will apply the existing locality config on them instead of the
	// new locality config.
	filteredExpectedZoneConfigSubzones := expectedZoneConfig.Subzones[:0]
	for _, c := range expectedZoneConfig.Subzones {
		if c.PartitionName != "" {
			if _, ok := transitioningRegions[c.PartitionName]; ok {
				continue
			}
		}
		if _, ok := regionalByRowNewIndexes[c.IndexID]; ok {
			continue
		}
		filteredExpectedZoneConfigSubzones = append(
			filteredExpectedZoneConfigSubzones,
			c,
		)
	}
	expectedZoneConfig.Subzones = filteredExpectedZoneConfigSubzones

	// Mark the expected NumReplicas as 0 if we have a placeholder
	// and the current zone config is also a placeholder.
	// The latter check is required as in cases where non-multiregion fields
	// are set on the current zone config, the expected zone config needs
	// the placeholder marked so that DiffWithZone does not error when
	// num_replicas is expectedly different.
	// e.g. if current zone config has gc.ttlseconds set, then we
	// do not fudge num replicas to be equal to 0 -- otherwise the
	// check fails when num_replicas is different, but that is
	// expected as the current zone config is no longer a placeholder.
	if currentZoneConfig.IsSubzonePlaceholder() && regions.IsPlaceholderZoneConfigForMultiRegion(expectedZoneConfig) {
		expectedZoneConfig.NumReplicas = proto.Int32(0)
	}

	regionConfig := dbDesc.GetRegionConfig()
	var leasePreferences []zonepb.LeasePreference
	if regionConfig != nil {
		if regionConfig.SecondaryRegion != "" {
			switch {
			case desc.IsLocalityRegionalByTable():
				rbt := desc.GetLocalityConfig().GetRegionalByTable()
				if rbt.Region != nil {
					leasePreferences = regions.SynthesizeLeasePreferences(*rbt.Region, regionConfig.SecondaryRegion)
				} else {
					leasePreferences = regions.SynthesizeLeasePreferences(regionConfig.PrimaryRegion, regionConfig.SecondaryRegion)
				}
			default:
				leasePreferences = regions.SynthesizeLeasePreferences(regionConfig.PrimaryRegion, regionConfig.SecondaryRegion)
			}

			expectedZoneConfig.LeasePreferences = leasePreferences
		}
	}

	// Compare the two zone configs to see if anything is amiss.
	same, mismatch, err := currentZoneConfig.DiffWithZone(
		expectedZoneConfig,
		zonepb.MultiRegionZoneConfigFields,
	)
	if err != nil {
		return err
	}
	if !same {
		descType := "table"
		name := tableName.String()
		if mismatch.IndexID != 0 {
			indexName, ok := subzoneIndexIDsToDiff[mismatch.IndexID]
			if !ok {
				return errors.AssertionFailedf(
					"unexpected unknown index id %d on table %s (mismatch %#v)",
					mismatch.IndexID,
					tableName,
					mismatch,
				)
			}

			if mismatch.PartitionName != "" {
				descType = "partition"
				partitionName := tree.Name(mismatch.PartitionName)
				name = fmt.Sprintf(
					"%s of %s@%s",
					partitionName.String(),
					tableName.String(),
					indexName.String(),
				)
			} else {
				descType = "index"
				name = fmt.Sprintf("%s@%s", tableName.String(), indexName.String())
			}
		}

		if mismatch.IsMissingSubzone {
			return zoneConfigForMultiRegionValidator.newMissingSubzoneError(
				descType,
				name,
				mismatch,
			)
		}
		if mismatch.IsExtraSubzone {
			return zoneConfigForMultiRegionValidator.newExtraSubzoneError(
				descType,
				name,
				mismatch,
			)
		}

		return zoneConfigForMultiRegionValidator.newMismatchFieldError(
			descType,
			name,
			mismatch,
		)
	}

	return nil
}

// checkNoRegionalByRowChangeUnderway checks that no REGIONAL BY ROW
// tables are undergoing a schema change that affect their partitions
// and no tables are transitioning to or from REGIONAL BY ROW.
func (p *planner) checkNoRegionalByRowChangeUnderway(
	ctx context.Context, dbDesc catalog.DatabaseDescriptor,
) error {
	// forEachTableDesc touches all the table keys, which prevents a race
	// with ADD/REGION committing at the same time as the user transaction.
	return p.forEachMutableTableInDatabase(
		ctx,
		dbDesc,
		func(ctx context.Context, scName string, table *tabledesc.Mutable) error {
			wrapErr := func(err error, detailSuffix string) error {
				return errors.WithHintf(
					errors.WithDetailf(
						err,
						"table %s.%s %s",
						tree.Name(scName),
						tree.Name(table.GetName()),
						detailSuffix,
					),
					"cancel the existing job or try again when the change is complete",
				)
			}
			for _, mut := range table.AllMutations() {
				// Disallow any locality related swaps.
				if pkSwap := mut.AsPrimaryKeySwap(); pkSwap != nil {
					if lcSwap := pkSwap.PrimaryKeySwapDesc().LocalityConfigSwap; lcSwap != nil {
						return wrapErr(
							pgerror.Newf(
								pgcode.ObjectNotInPrerequisiteState,
								"cannot perform database region changes while a REGIONAL BY ROW transition is underway",
							),
							"is currently transitioning to or from REGIONAL BY ROW",
						)
					}
					return wrapErr(
						pgerror.Newf(
							pgcode.ObjectNotInPrerequisiteState,
							"cannot perform database region changes while a ALTER PRIMARY KEY is underway",
						),
						"is currently undergoing an ALTER PRIMARY KEY change",
					)
				}
			}
			// Disallow index changes for REGIONAL BY ROW tables.
			// We do this on the second loop, as ALTER PRIMARY KEY may push
			// CREATE/DROP INDEX before the ALTER PRIMARY KEY mutation itself.
			// We should catch ALTER PRIMARY KEY before this ADD/DROP INDEX.
			for _, mut := range table.AllMutations() {
				if table.IsLocalityRegionalByRow() {
					if idx := mut.AsIndex(); idx != nil {
						return wrapErr(
							pgerror.Newf(
								pgcode.ObjectNotInPrerequisiteState,
								"cannot perform database region changes while an index is being created or dropped on a REGIONAL BY ROW table",
							),
							fmt.Sprintf("is currently modifying index %s", tree.Name(idx.GetName())),
						)
					}
				}
			}
			return nil
		},
	)
}

// checkNoRegionChangeUnderway checks whether the regions on the current
// database are currently being modified.
func (p *planner) checkNoRegionChangeUnderway(
	ctx context.Context, dbID descpb.ID, op string,
) error {
	// SynthesizeRegionConfig touches the type descriptor row, which
	// prevents a race with a racing conflicting schema change.
	r, err := SynthesizeRegionConfig(
		ctx,
		p.txn,
		dbID,
		p.Descriptors(),
	)
	if err != nil {
		return err
	}
	if len(r.TransitioningRegions()) > 0 {
		return errors.WithDetailf(
			errors.WithHintf(
				pgerror.Newf(
					pgcode.ObjectNotInPrerequisiteState,
					"cannot %s while a region is being added or dropped on the database",
					op,
				),
				"cancel the job which is adding or dropping the region or try again later",
			),
			"region %s is currently being added or dropped",
			r.TransitioningRegions()[0],
		)
	}
	return nil
}

// GetMultiregionConfig is part of the eval.Planner interface.
func (p *planner) GetMultiregionConfig(
	ctx context.Context, databaseID descpb.ID,
) (interface{}, bool) {

	regionConfig, err := SynthesizeRegionConfig(
		ctx,
		p.txn,
		databaseID,
		p.Descriptors(),
		multiregion.SynthesizeRegionConfigOptionUseCache,
	)

	if err != nil {
		return nil /* regionConfig */, false
	}
	return &regionConfig, true
}

// IsANSIDML is part of the eval.Planner interface.
func (p *planner) IsANSIDML() bool {
	return statements.IsANSIDML(p.stmt.AST)
}

// EnforceHomeRegion is part of the eval.Planner interface.
func (p *planner) EnforceHomeRegion() bool {
	return p.EvalContext().SessionData().EnforceHomeRegion && statements.IsANSIDML(p.stmt.AST)
}

// GetRangeDescIterator is part of the eval.Planner interface.
func (p *planner) GetRangeDescIterator(
	ctx context.Context, span roachpb.Span,
) (rangedesc.Iterator, error) {
	return p.ExecCfg().RangeDescIteratorFactory.NewIterator(ctx, span)
}

// GetRangeDescByID is part of the eval.Planner interface.
func (p *planner) GetRangeDescByID(
	ctx context.Context, rangeID roachpb.RangeID,
) (rangeDesc roachpb.RangeDescriptor, _ error) {
	execCfg := p.execCfg
	tenantSpan := execCfg.Codec.TenantSpan()
	rangeDescIterator, err := execCfg.RangeDescIteratorFactory.NewIterator(ctx, tenantSpan)
	if err != nil {
		return rangeDesc, err
	}
	for rangeDescIterator.Valid() {
		rangeDesc = rangeDescIterator.CurRangeDescriptor()
		if rangeDesc.RangeID == rangeID {
			break
		}
		rangeDescIterator.Next()
	}
	if !rangeDescIterator.Valid() {
		return rangeDesc, errors.Newf("range with ID %d not found", rangeID)
	}
	return rangeDesc, nil
}

func (p *planner) setSystemDatabaseSurvival(ctx context.Context) error {
	// Retrieve the system database descriptor and ensure it supports
	// multi-region
	systemDB, err := p.Descriptors().MutableByID(p.txn).Database(ctx, keys.SystemDatabaseID)
	if err != nil {
		return err
	}

	// Alter the survivability goal on the system database, if we are on
	// a new enough version.
	if err := p.alterDatabaseSurvivalGoal(ctx,
		systemDB, tree.SurvivalGoalRegionFailure, "alter system database to survive region failure only"); err != nil {
		return err
	}

	return nil
}

// systemTableLocalityChangeJobName is the job name used when the locality is converted
// to regional by row.
const systemTableLocalityChangeJobName = "convert system.%s to %s locality"

// optimizeSystemDatabase configures some tables in the system data as
// global and regional by row. The locality changes reduce how long it
// takes a server to start up in a multi-region deployment.
func (p *planner) optimizeSystemDatabase(ctx context.Context) error {
	systemTables := systemschema.MakeSystemTables()
	globalTables := map[string]struct{}{
		"users":                  {},
		"zones":                  {},
		"privileges":             {},
		"comments":               {},
		"role_options":           {},
		"role_members":           {},
		"database_role_settings": {},
		"settings":               {},
		"descriptor":             {},
		"namespace":              {},
		"table_statistics":       {},
		"web_sessions":           {},
		"region_liveness":        {},
	}

	rbrTables := map[string]struct{}{
		"sqlliveness":   {},
		"sql_instances": {},
		"lease":         {},
	}

	// Configure all ohter tables are region tables, with the system database
	// primary region as the region.
	regionTables := make([]string, 0, len(systemTables))
	for _, table := range systemTables {
		if !table.IsTable() {
			continue
		}
		name := table.GetName()
		if _, ok := rbrTables[name]; ok {
			continue
		}
		if _, ok := globalTables[name]; ok {
			continue
		}
		regionTables = append(regionTables, name)
	}

	// Retrieve the system database descriptor and ensure it supports
	// multi-region
	systemDB, err := p.Descriptors().MutableByID(p.txn).Database(ctx, keys.SystemDatabaseID)
	if err != nil {
		return err
	}
	regionEnumID, err := systemDB.MultiRegionEnumID()
	if err != nil {
		return errors.Wrap(err, "system database is not multi-region")
	}
	enumTypeDesc, err := p.Descriptors().MutableByID(p.txn).Type(ctx, regionEnumID)
	if err != nil {
		return err
	}

	// Convert the enum descriptor into a type
	enumTypeName := tree.MakeQualifiedTypeName(
		catconstants.SystemDatabaseName, catconstants.PublicSchemaName, enumTypeDesc.GetName(),
	)
	enumType, err := typedesc.HydratedTFromDesc(ctx, &enumTypeName, enumTypeDesc, p)
	if err != nil {
		return err
	}

	getDescriptor := func(name string) (*tabledesc.Mutable, error) {
		tableName := tree.MakeTableNameWithSchema(
			catconstants.SystemDatabaseName, catconstants.PublicSchemaName, tree.Name(name),
		)
		required := true
		_, desc, err := resolver.ResolveMutableExistingTableObject(
			ctx, p, &tableName, required, tree.ResolveRequireTableDesc)
		return desc, err
	}

	applyLocalityChange := func(desc *tabledesc.Mutable, locality string) error {
		if err := p.ResetMultiRegionZoneConfigsForTable(ctx, int64(desc.GetID()), locality == "regional by row"); err != nil {
			return err
		}

		jobName := fmt.Sprintf(systemTableLocalityChangeJobName, desc.GetName(), locality)
		return p.writeSchemaChange(ctx, desc, descpb.InvalidMutationID, jobName)
	}

	getMutableColumn := func(table *tabledesc.Mutable, name string) (*descpb.ColumnDescriptor, error) {
		for i := range table.Columns {
			if table.Columns[i].Name == name {
				return &table.Columns[i], nil
			}
		}
		return nil, errors.Newf("%s column not found", name)
	}

	partitionByRegion := func(table *tabledesc.Mutable) error {
		// System databases will have regional by row tables configured
		// as survive zone.
		regionConfig, err := SynthesizeRegionConfig(
			ctx, p.txn, table.GetParentID(), p.Descriptors(), multiregion.SynthesizeRegionConfigOptionForceSurvivalZone,
		)
		if err != nil {
			return err
		}
		partitionAllBy := multiregion.PartitionByForRegionalByRow(
			regionConfig,
			"crdb_region",
		)

		unexpectedColumns, partitioning, err := CreatePartitioning(
			ctx,
			p.ExecCfg().Settings,
			p.EvalContext(),
			table,
			table.PrimaryIndex,
			partitionAllBy,
			nil, /*do not allow implicit columns, the existing crdb_region column must be used*/
			true /*allow implicit partitioning */)
		if err != nil {
			return err
		}
		if 0 < len(unexpectedColumns) {
			return errors.AssertionFailedf("unexpected implicit partitioning columns for table %s", table.GetName())
		}
		tabledesc.UpdateIndexPartitioning(&table.PrimaryIndex, true, nil, partitioning)
		table.PartitionAllBy = true
		return nil
	}

	// Transforms the crdb_region type into a proper enum.
	setCrdbRegionColumnType := func(descriptor *tabledesc.Mutable) (tree.Name, error) {
		// Change crdb_region type to the multi-region enum
		column, err := getMutableColumn(descriptor, "crdb_region")
		if err != nil {
			return "", err
		}
		column.Type = enumType

		// Add a back reference to the table
		backReferenceJob := fmt.Sprintf("add back ref on mr-enum for system table %s", descriptor.GetName())
		if err = p.addTypeBackReference(ctx, regionEnumID, descriptor.GetID(), backReferenceJob); err != nil {
			return "", err
		}
		return tree.Name(column.Name), nil
	}

	// Configure global system tables
	for tableName := range globalTables {
		descriptor, err := getDescriptor(tableName)
		// Some system tables only come into effect once the
		// version changes, so skip over those.
		if sqlerrors.IsUndefinedRelationError(err) {
			continue
		}
		if err != nil {
			return err
		}

		// Set the type of crdb_region column for the liveness table.
		if tableName == "region_liveness" {
			_, err = setCrdbRegionColumnType(descriptor)
			if err != nil {
				return err
			}
		}

		descriptor.SetTableLocalityGlobal()

		if err := applyLocalityChange(descriptor, "global"); err != nil {
			return err
		}
	}

	// Configure by region tables next.
	primaryRegionName, err := systemDB.PrimaryRegionName()
	if err != nil {
		return err
	}
	for _, tableName := range regionTables {
		descriptor, err := getDescriptor(tableName)
		// Some system tables only come into effect once the
		// version changes, so skip over those.
		if sqlerrors.IsUndefinedRelationError(err) {
			continue
		}
		if err != nil {
			return err
		}
		descriptor.SetTableLocalityRegionalByTable(tree.Name(primaryRegionName))
		if err := applyLocalityChange(descriptor, "global"); err != nil {
			return err
		}
	}

	// Configure regional by row system tables
	for tableName := range rbrTables {
		descriptor, err := getDescriptor(tableName)
		if err != nil {
			return err
		}

		// Change crdb_region type to the multi-region enum
		columName, err := setCrdbRegionColumnType(descriptor)
		if err != nil {
			return err
		}
		descriptor.SetTableLocalityRegionalByRow(columName)
		if err := partitionByRegion(descriptor); err != nil {
			return err
		}

		if err := applyLocalityChange(descriptor, "regional by row"); err != nil {
			return err
		}

		// Delete statistics for the table because the statistics materialize
		// the column type for `crdb_region` and the column type is changing
		// from bytes to an enum.
		if _, err := p.InternalSQLTxn().Exec(ctx, "delete-stats", p.txn,
			`DELETE FROM system.table_statistics WHERE "tableID" = $1;`,
			descriptor.GetID(),
		); err != nil {
			return errors.Wrap(err, "unable to delete statistics")
		}
	}

	return nil
}

// zoneConfigValidator implements descs.ZoneConfigValidator
type zoneConfigValidator struct {
	txn            *kv.Txn
	descs          *descs.Collection
	regionProvider descs.RegionProvider
	execCfg        *ExecutorConfig
}

// newZoneConfigValidator creates a new zone config validator.
func newZoneConfigValidator(
	txn *kv.Txn,
	descs *descs.Collection,
	regionProvider descs.RegionProvider,
	execCfg *ExecutorConfig,
) descs.ZoneConfigValidator {
	return &zoneConfigValidator{
		txn:            txn,
		descs:          descs,
		regionProvider: regionProvider,
		execCfg:        execCfg,
	}
}

// ValidateDbZoneConfig implements descs.ZoneConfigValidator.
func (zv *zoneConfigValidator) ValidateDbZoneConfig(
	ctx context.Context, db catalog.DatabaseDescriptor,
) error {
	regionConfig, err := SynthesizeRegionConfig(
		ctx, zv.txn, db.GetID(), zv.descs,
	)
	if err != nil {
		return err
	}

	currentZone := zonepb.NewZoneConfig()
	if currentZoneConfigWithRaw, err := zv.descs.GetZoneConfig(
		ctx, zv.txn, db.GetID(),
	); err != nil {
		return err
	} else if currentZoneConfigWithRaw != nil {
		currentZone = currentZoneConfigWithRaw.ZoneConfigProto()
	}

	_, err = generateAndValidateZoneConfigForMultiRegionDatabase(ctx,
		zv.regionProvider,
		zv.execCfg,
		regionConfig,
		currentZone,
		true, /*validateLocalities*/
	)
	if err != nil {
		return err
	}
	return nil
}
