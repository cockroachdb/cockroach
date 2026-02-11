// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/errors"
)

// AlterTableLocality implements ALTER TABLE ... LOCALITY ... for the declarative schema changer.
// Using this operation, user can set table locality to GLOBAL, REGIONAL BY TABLE, or REGIONAL BY ROW.
func AlterTableLocality(b BuildCtx, t *tree.AlterTableLocality) {

	// Resolve the table
	elts := b.ResolveTable(t.Name, ResolveParams{
		IsExistenceOptional: t.IfExists,
		RequiredPrivilege:   0, // Will check multi-region privilege separately
	})

	// IF EXISTS was specified, and the table doesn't exist, so this is a no-op.
	if elts == nil && t.IfExists {
		return
	}

	// Get the table element
	tbl := elts.FilterTable().MustGetOneElement()
	tableID := tbl.TableID
	tableName := t.Name.Object()

	// Check user has sufficient privileges for this schema change
	checkPrivilegesForMultiRegionOp(b, tbl, tableName)

	// Ensure database is multi-region and get the enum type ID
	regionEnumTypeID := getDatabaseMultiRegionEnumTypeID(b, tableID)

	// Determine current locality by querying existing locality elements
	targetLocality := t.Locality
	currentLocalityElem, currentLocality := getCurrentTableLocality(b, tableID, tableName)

	if isLocalityRegionalByRow(currentLocality) || isLocalityRegionalByRow(targetLocality) {
		panic(scerrors.NotImplementedErrorf(t, "alter locality to or from RBR not implemented yet"))
	}

	// Validate region name for REGIONAL BY TABLE IN <region>
	if targetLocality.LocalityLevel == tree.LocalityLevelTable &&
		targetLocality.TableRegion != tree.PrimaryRegionNotSpecifiedName {
		validateRegionName(b, tableID, regionEnumTypeID, catpb.RegionName(targetLocality.TableRegion))
	}

	// Increase telemetry stats
	incrementAlterTableLocalityTelemetry(b, currentLocality, targetLocality)

	err := validateZoneConfigForMultiRegionTableWasNotModifiedByUser(b, tableID, tableName, buildLocalityConfig(currentLocality))
	if err != nil {
		panic(err)
	}

	// Drop the old locality element and add a new one
	b.Drop(currentLocalityElem)
	addTargetLocalityElements(b, tableID, regionEnumTypeID, targetLocality)

	// Update the table zone config
	err = configureZoneConfigForNewTableLocality(b, tableID, buildLocalityConfig(targetLocality))
	if err != nil {
		panic(err)
	}

	// Record this table alteration in the event log
	b.LogEventForExistingTarget(tbl)
}

func incrementAlterTableLocalityTelemetry(
	b BuildCtx, currentLocality *tree.Locality, targetLocality *tree.Locality,
) {
	currentLocalityTelemetryName := multiregion.TelemetryNameForLocality(currentLocality)
	targetLocalityTelemetryName := multiregion.TelemetryNameForLocality(targetLocality)
	b.IncrementAlterTableLocalityCounter(currentLocalityTelemetryName, targetLocalityTelemetryName)
}

func getDatabaseMultiRegionEnumTypeID(b BuildCtx, tableID catid.DescID) catid.DescID {
	namespace := mustRetrieveNamespaceElem(b, tableID)
	dbRegionConfig := b.QueryByID(namespace.DatabaseID).FilterDatabaseRegionConfig().MustGetZeroOrOneElement()
	if dbRegionConfig == nil {
		panic(errors.WithHint(
			pgerror.Newf(
				pgcode.InvalidTableDefinition,
				"cannot alter a table's LOCALITY if its database is not multi-region enabled",
			),
			"database must first be multi-region enabled using ALTER DATABASE ... SET PRIMARY REGION <region>",
		))
	}
	return dbRegionConfig.RegionEnumTypeID
}

// getCurrentTableLocality determines the current locality configuration of a table
// by examining its locality elements.
func getCurrentTableLocality(
	b BuildCtx, tableID catid.DescID, tableName string,
) (scpb.Element, *tree.Locality) {
	tblElts := b.QueryByID(tableID)
	// Check for GLOBAL
	if globalElt := tblElts.FilterTableLocalityGlobal().MustGetZeroOrOneElement(); globalElt != nil {
		return globalElt, &tree.Locality{
			LocalityLevel: tree.LocalityLevelGlobal,
		}
	}
	// Check for REGIONAL BY TABLE (primary region)
	if primaryRegionElt := tblElts.FilterTableLocalityPrimaryRegion().MustGetZeroOrOneElement(); primaryRegionElt != nil {
		return primaryRegionElt, &tree.Locality{
			LocalityLevel: tree.LocalityLevelTable,
			TableRegion:   tree.PrimaryRegionNotSpecifiedName,
		}
	}
	// Check for REGIONAL BY TABLE with secondary region
	if secRegionElt := tblElts.FilterTableLocalitySecondaryRegion().MustGetZeroOrOneElement(); secRegionElt != nil {
		return secRegionElt, &tree.Locality{
			LocalityLevel: tree.LocalityLevelTable,
			TableRegion:   tree.Name(secRegionElt.RegionName),
		}
	}
	// Check for REGIONAL BY ROW
	if rbrElt := tblElts.FilterTableLocalityRegionalByRow().MustGetZeroOrOneElement(); rbrElt != nil {
		return rbrElt, &tree.Locality{
			LocalityLevel:       tree.LocalityLevelRow,
			RegionalByRowColumn: tree.Name(rbrElt.As),
		}
	}
	// No locality found - this shouldn't happen for multi-region tables
	panic(pgerror.Newf(pgcode.AssertFailure, "table %s has undefined locality", tableName))
}

func checkPrivilegesForMultiRegionOp(b BuildCtx, tbl *scpb.Table, tableName string) {
	// Unless MultiRegionSystemDatabaseEnabled is true, only secondary tenants may
	// have their system database set up for multi-region operations. Even then,
	// ensure that only the node user may configure the system database.
	if tbl.TableID == keys.SystemDatabaseID {
		multiRegionSystemDatabase := sqlclustersettings.MultiRegionSystemDatabaseEnabled.Get(&b.ClusterSettings().SV)
		if !multiRegionSystemDatabase && b.Codec().ForSystemTenant() {
			panic(pgerror.Newf(pgcode.FeatureNotSupported,
				"modifying the regions of system database is not supported"))
		}
		if u := b.SessionData().User(); !u.IsNodeUser() && !u.IsRootUser() {
			panic(pgerror.Newf(pgcode.InsufficientPrivilege,
				"user %s may not modify the system database", u))
		}
	}

	if !b.CurrentUser().IsAdminRole() {
		err := b.CheckPrivilege(tbl, privilege.CREATE)
		// Wrap an insufficient privileges error a bit better to reflect the lack
		// of ownership as well.
		if err != nil {
			panic(pgerror.Wrapf(err, pgcode.InsufficientPrivilege,
				"user %s must be owner of %s or have %v privilege on relation %s",
				b.SessionData().User(),
				tableName,
				privilege.CREATE,
				tableName,
			))
		}
	}
}

// buildLocalityConfig converts a tree.Locality to catpb.LocalityConfig
func buildLocalityConfig(locality *tree.Locality) catpb.LocalityConfig {
	switch locality.LocalityLevel {
	case tree.LocalityLevelGlobal:
		return catpb.LocalityConfig{
			Locality: &catpb.LocalityConfig_Global_{
				Global: &catpb.LocalityConfig_Global{},
			},
		}
	case tree.LocalityLevelTable:
		if locality.TableRegion == tree.PrimaryRegionNotSpecifiedName {
			// REGIONAL BY TABLE IN PRIMARY REGION
			return catpb.LocalityConfig{
				Locality: &catpb.LocalityConfig_RegionalByTable_{
					RegionalByTable: &catpb.LocalityConfig_RegionalByTable{
						Region: nil, // nil means primary region
					},
				},
			}
		}
		// REGIONAL BY TABLE IN <region>
		region := catpb.RegionName(locality.TableRegion)
		return catpb.LocalityConfig{
			Locality: &catpb.LocalityConfig_RegionalByTable_{
				RegionalByTable: &catpb.LocalityConfig_RegionalByTable{
					Region: &region,
				},
			},
		}
	case tree.LocalityLevelRow:
		// REGIONAL BY ROW
		// Note that changes to/from RBR is not supported in the declarative schema changer yet
		return catpb.LocalityConfig{
			Locality: &catpb.LocalityConfig_RegionalByRow_{
				RegionalByRow: &catpb.LocalityConfig_RegionalByRow{},
			},
		}
	default:
		panic(errors.AssertionFailedf("Unknown locality level: %v", locality.LocalityLevel))
	}
}

// addTargetLocalityElements adds the new locality elements for the target locality configuration.
func addTargetLocalityElements(
	b BuildCtx, tableID catid.DescID, regionEnumTypeID catid.DescID, locality *tree.Locality,
) {
	switch locality.LocalityLevel {
	case tree.LocalityLevelGlobal:
		b.Add(&scpb.TableLocalityGlobal{
			TableID: tableID,
		})
	case tree.LocalityLevelTable:
		if locality.TableRegion == tree.PrimaryRegionNotSpecifiedName {
			// REGIONAL BY TABLE IN PRIMARY REGION
			b.Add(&scpb.TableLocalityPrimaryRegion{
				TableID: tableID,
			})
		} else {
			// REGIONAL BY TABLE IN <region>
			b.Add(&scpb.TableLocalitySecondaryRegion{
				TableID:          tableID,
				RegionEnumTypeID: regionEnumTypeID,
				RegionName:       catpb.RegionName(locality.TableRegion),
			})
		}
	case tree.LocalityLevelRow:
		// Note that changes to/from RBR is not supported in the declarative schema changer yet
		// Determine the column name to use
		colName := locality.RegionalByRowColumn
		if colName == tree.RegionalByRowRegionNotSpecifiedName {
			colName = tree.RegionalByRowRegionDefaultColName
		}
		b.Add(&scpb.TableLocalityRegionalByRow{
			TableID: tableID,
			As:      string(colName),
		})
	default:
		panic(errors.AssertionFailedf("Unknown locality level: %v", locality.LocalityLevel))
	}
}

func isLocalityRegionalByRow(locality *tree.Locality) bool {
	return locality.LocalityLevel == tree.LocalityLevelRow
}

// validateRegionName checks if the specified region name exists in the database's
// multi-region configuration.
func validateRegionName(
	b BuildCtx, tableID, regionEnumTypeID catid.DescID, regionName catpb.RegionName,
) {
	// Query all enum values for the region enum type
	enumValues := b.QueryByID(regionEnumTypeID).FilterEnumTypeValue()

	// Collect all valid region names
	var validRegions []string
	enumValues.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.EnumTypeValue) {
		validRegions = append(validRegions, e.LogicalRepresentation)
	})

	// Check if the specified region exists
	foundRegion := false
	for _, r := range validRegions {
		if regionName == catpb.RegionName(r) {
			foundRegion = true
			break
		}
	}

	// If region not found, return error with hint
	if !foundRegion {
		namespace := mustRetrieveNamespaceElem(b, tableID)
		dbName := mustRetrieveNamespaceElem(b, namespace.DatabaseID).Name
		panic(errors.WithHintf(
			pgerror.Newf(
				pgcode.InvalidTableDefinition,
				`region "%s" has not been added to database "%s"`,
				regionName,
				dbName,
			),
			"available regions: %s",
			strings.Join(validRegions, ", "),
		))
	}
}
