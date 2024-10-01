// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregion

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// ValidateTableLocalityConfig validates whether the descriptor's locality
// config is valid under the given database.
func ValidateTableLocalityConfig(
	desc catalog.TableDescriptor, db catalog.DatabaseDescriptor, vdg catalog.ValidationDescGetter,
) error {

	lc := desc.GetLocalityConfig()
	if lc == nil {
		if db.IsMultiRegion() {
			return pgerror.Newf(
				pgcode.InvalidTableDefinition,
				"database %s is multi-region enabled, but table %s has no locality set",
				db.GetName(),
				desc.GetName(),
			)
		}
		// Nothing to validate for non-multi-region databases.
		return nil
	}

	if !db.IsMultiRegion() {
		s := tree.NewFmtCtx(tree.FmtSimple)
		var locality string
		// Formatting the table locality config should never fail; if it does, the
		// error message is more clear if we construct a dummy locality here.
		if err := FormatTableLocalityConfig(lc, s); err != nil {
			locality = "INVALID LOCALITY"
		}
		locality = s.String()
		return pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"database %s is not multi-region enabled, but table %s has locality %s set",
			db.GetName(),
			desc.GetName(),
			locality,
		)
	}

	regionsEnumID, err := db.MultiRegionEnumID()
	if err != nil {
		return err
	}
	typeDesc, err := vdg.GetTypeDescriptor(regionsEnumID)
	if err != nil {
		return errors.Wrapf(err, "multi-region enum with ID %d does not exist", regionsEnumID)
	}
	regionsEnumDesc := typeDesc.AsRegionEnumTypeDescriptor()
	if regionsEnumDesc == nil {
		return errors.AssertionFailedf("expected region enum type, not %s for type %q (%d)",
			typeDesc.GetKind(), typeDesc.GetName(), typeDesc.GetID())
	}
	if regionsEnumDesc.Dropped() {
		return errors.AssertionFailedf("multi-region enum type %q (%d) is dropped",
			regionsEnumDesc.GetName(), regionsEnumDesc.GetID())
	}

	// Check non-table items have a correctly set locality.
	if desc.IsSequence() {
		if !desc.IsLocalityRegionalByTable() {
			return errors.AssertionFailedf(
				"expected sequence %s to have locality REGIONAL BY TABLE",
				desc.GetName(),
			)
		}
	}
	if desc.IsView() {
		if desc.MaterializedView() {
			if !desc.IsLocalityGlobal() {
				return errors.AssertionFailedf(
					"expected materialized view %s to have locality GLOBAL",
					desc.GetName(),
				)
			}
		} else {
			if !desc.IsLocalityRegionalByTable() {
				return errors.AssertionFailedf(
					"expected view %s to have locality REGIONAL BY TABLE",
					desc.GetName(),
				)
			}
		}
	}

	// REGIONAL BY TABLE tables homed in the primary region should include a
	// reference to the multi-region type descriptor and a corresponding
	// backreference. All other patterns should only contain a reference if there
	// is an explicit column which uses the multi-region type descriptor as its
	// *types.T. While the specific cases are validated below, we search for the
	// region enum ID in the references list just once, up top here.
	typeIDs, typeIDsReferencedByColumns, err := desc.GetAllReferencedTypeIDs(db, vdg.GetTypeDescriptor)
	if err != nil {
		return err
	}
	regionEnumIDReferenced := false
	for _, typeID := range typeIDs {
		if typeID == regionsEnumID {
			regionEnumIDReferenced = true
			break
		}
	}
	columnTypesTypeIDs := catalog.MakeDescriptorIDSet(typeIDsReferencedByColumns...)
	switch lc := lc.Locality.(type) {
	case *catpb.LocalityConfig_Global_:
		if regionEnumIDReferenced {
			if !columnTypesTypeIDs.Contains(regionsEnumID) {
				return errors.AssertionFailedf(
					"expected no region Enum ID to be referenced by a GLOBAL TABLE: %q"+
						" but found: %d",
					desc.GetName(),
					regionsEnumDesc.GetID(),
				)
			}
		}
	case *catpb.LocalityConfig_RegionalByRow_:
		if !desc.IsPartitionAllBy() {
			return errors.AssertionFailedf("expected REGIONAL BY ROW table to have PartitionAllBy set")
		}
		// For REGIONAL BY ROW tables, ensure partitions in the PRIMARY KEY match
		// the database descriptor. Ensure each public region has a partition,
		// and each transitioning region name to possibly have a partition.
		// We do validation that ensures all index partitions are the same on
		// PARTITION ALL BY.
		var regions, transitioningRegions catpb.RegionNames
		regionNames := make(map[catpb.RegionName]struct{})
		transitioningRegionNames := make(map[catpb.RegionName]struct{}, len(regions))
		_ = regionsEnumDesc.ForEachRegion(func(name catpb.RegionName, transition descpb.TypeDescriptor_EnumMember_Direction) error {
			if transition == descpb.TypeDescriptor_EnumMember_NONE {
				regions = append(regions, name)
				regionNames[name] = struct{}{}
			} else {
				transitioningRegions = append(transitioningRegions, name)
				transitioningRegionNames[name] = struct{}{}
			}
			return nil
		})

		part := desc.GetPrimaryIndex().GetPartitioning()
		err = part.ForEachList(func(name string, _ [][]byte, _ catalog.Partitioning) error {
			regionName := catpb.RegionName(name)
			// Any transitioning region names may exist.
			if _, ok := transitioningRegionNames[regionName]; ok {
				return nil
			}
			// If a region is not found in any of the region names, we have an unknown
			// partition.
			if _, ok := regionNames[regionName]; !ok {
				return errors.AssertionFailedf(
					"unknown partition %s on PRIMARY INDEX of table %s",
					name,
					desc.GetName(),
				)
			}
			delete(regionNames, regionName)
			return nil
		})
		if err != nil {
			return err
		}

		// Any regions that are not deleted from the above loop is missing.
		for regionName := range regionNames {
			return errors.AssertionFailedf(
				"missing partition %s on PRIMARY INDEX of table %s",
				regionName,
				desc.GetName(),
			)
		}

	case *catpb.LocalityConfig_RegionalByTable_:

		// Table is homed in an explicit (non-primary) region.
		if lc.RegionalByTable.Region != nil {
			foundRegion := false
			var regions catpb.RegionNames
			_ = regionsEnumDesc.ForEachRegion(func(name catpb.RegionName, transition descpb.TypeDescriptor_EnumMember_Direction) error {
				// Since the partitions and zone configs are only updated when a transaction
				// commits, this must ignore all regions being added (since they will not be
				// reflected in the zone configuration yet), but it must include all region
				// being dropped (since they will not be dropped from the zone configuration
				// until they are fully removed from the type descriptor, again, at the end
				// of the transaction).
				if transition != descpb.TypeDescriptor_EnumMember_ADD {
					regions = append(regions, name)
				}
				return nil
			})
			for _, r := range regions {
				if *lc.RegionalByTable.Region == r {
					foundRegion = true
					break
				}
			}
			if !foundRegion {
				return errors.WithHintf(
					pgerror.Newf(
						pgcode.InvalidTableDefinition,
						`region "%s" has not been added to database "%s"`,
						*lc.RegionalByTable.Region,
						db.DatabaseDesc().Name,
					),
					"available regions: %s",
					strings.Join(regions.ToStrings(), ", "),
				)
			}
			if !regionEnumIDReferenced {
				return errors.AssertionFailedf(
					"expected multi-region enum ID %d to be referenced on REGIONAL BY TABLE: %q locality "+
						"config, but did not find it",
					regionsEnumID,
					desc.GetName(),
				)
			}
		} else {
			if regionEnumIDReferenced {
				// It may be the case that the multi-region type descriptor is used
				// as the type of the table column. Validations should only fail if
				// that is not the case.
				if !columnTypesTypeIDs.Contains(regionsEnumID) {
					return errors.AssertionFailedf(
						"expected no region Enum ID to be referenced by a REGIONAL BY TABLE: %q homed in the "+
							"primary region, but found: %d",
						desc.GetName(),
						regionsEnumDesc.GetID(),
					)
				}
			}
		}
	default:
		return pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"unknown locality level: %T",
			lc,
		)
	}
	return nil
}

// FormatTableLocalityConfig formats the table locality.
func FormatTableLocalityConfig(c *catpb.LocalityConfig, f *tree.FmtCtx) error {
	switch v := c.Locality.(type) {
	case *catpb.LocalityConfig_Global_:
		f.WriteString("GLOBAL")
	case *catpb.LocalityConfig_RegionalByTable_:
		f.WriteString("REGIONAL BY TABLE IN ")
		if v.RegionalByTable.Region != nil {
			region := tree.Name(*v.RegionalByTable.Region)
			f.FormatNode(&region)
		} else {
			f.WriteString("PRIMARY REGION")
		}
	case *catpb.LocalityConfig_RegionalByRow_:
		f.WriteString("REGIONAL BY ROW")
		if v.RegionalByRow.As != nil {
			f.WriteString(" AS ")
			col := tree.Name(*v.RegionalByRow.As)
			f.FormatNode(&col)
		}
	default:
		return errors.Newf("unknown locality: %T", v)
	}
	return nil
}
