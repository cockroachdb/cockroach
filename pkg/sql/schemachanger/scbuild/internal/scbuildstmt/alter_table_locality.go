// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdecomp"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
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

	// Ensure that the table is not in the middle of a region change.
	if isLocalityRegionalByRow(targetLocality) || isLocalityRegionalByRow(currentLocality) {
		panicIfRegionChangeUnderway(b, "perform this locality change", "", tbl.TableID)
	}

	// add or create region column when switching to regional by row
	if isLocalityRegionalByRow(targetLocality) {
		createOrVerifyRegionColumn(b, tableID, tableName, regionEnumTypeID, currentLocality, targetLocality, tbl, t)
	}

	// alter the primary key if needed
	if isLocalityRegionalByRow(targetLocality) || isLocalityRegionalByRow(currentLocality) {
		alterPrimaryKeyForRegionalByRowTable(b, tableID, tbl, t, currentLocality, targetLocality)
	}

	// Update the table zone config
	err = configureZoneConfigForNewTableLocality(b, tableID, buildLocalityConfig(targetLocality))
	if err != nil {
		panic(err)
	}

	// Post-processing: deflate redundant indexes and rewrite tentative IDs to real IDs
	maybeDropRedundantPrimaryIndexes(b, tableID)
	maybeRewriteTempIDsInPrimaryIndexes(b, tableID)

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

func getDatabasePrimaryRegionName(b BuildCtx, tableID catid.DescID) catpb.RegionName {
	namespace := mustRetrieveNamespaceElem(b, tableID)
	regionConfig, err := b.SynthesizeRegionConfig(b, namespace.DatabaseID)
	if err != nil {
		panic(err)
	}
	return regionConfig.PrimaryRegion()
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

// alterPrimaryKeyForRegionalByRowTable handles the index changes when transitioning
// to or from REGIONAL BY ROW.
func alterPrimaryKeyForRegionalByRowTable(
	b BuildCtx,
	tableID catid.DescID,
	tbl *scpb.Table,
	t *tree.AlterTableLocality,
	currentLocality *tree.Locality,
	targetLocality *tree.Locality,
) {
	if currentLocality.LocalityLevel == targetLocality.LocalityLevel &&
		explicitRegionColName(currentLocality.RegionalByRowColumn) == explicitRegionColName(targetLocality.RegionalByRowColumn) {
		// AlterTableLocality might be called to explicitly specify "crdb_region"
		// as the RBR region column or to reset subzone configs. Must avoid calling
		// alterPrimaryKey in that case.
		return
	}

	// Step 1: Get current primary key columns
	primaryIndexElem := mustRetrieveCurrentPrimaryIndexElement(b, tableID)

	// Step 2: Build columns list with an empty slot for a new region column prepended
	cols := retrieveNonImplicitIndexColumns(b, tableID, primaryIndexElem.IndexID)

	// Step 3: Get current PK name and sharding
	currentPKName := mustRetrieveIndexNameElem(b, tableID, primaryIndexElem.IndexID)
	var sharded *tree.ShardedIndexDef
	if primaryIndexElem.Sharding != nil {
		// remove shard column for the keys
		cols = cols[1:]
		// pass non nil sharding definition to alter primary key to recreate the sharding
		sharded = &tree.ShardedIndexDef{
			ShardBuckets: tree.NewDInt(tree.DInt(primaryIndexElem.Sharding.ShardBuckets)),
		}
	}

	// Step 4: Build the alterPrimaryKeySpec
	partSpec := getTablePartitioningSpec(b, tableID, targetLocality)
	spec := alterPrimaryKeySpec{
		n:            t,
		Columns:      cols,
		Sharded:      sharded,
		Name:         tree.Name(currentPKName.Name),
		Partitioning: &partSpec,
	}

	// Step 5: Call alterPrimaryKey
	// This will trigger recreation of all secondary indexes with new partitioning
	alterPrimaryKey(b, nil, tbl, nil, spec)
	b.EvalCtx().ClientNoticeSender.BufferClientNotice(
		b,
		pgnotice.Newf(
			"LOCALITY changes will be finalized asynchronously; "+
				"further schema changes on this table may be restricted until the job completes"),
	)
}

func getTablePartitioningSpec(
	b BuildCtx, tableID catid.DescID, targetLocality *tree.Locality,
) partitioningSpec {
	if !isLocalityRegionalByRow(targetLocality) {
		// return an empty partitioning spec to remove RBR partitioning
		return partitioningSpec{}
	}
	// Create a multiregion partitioning spec for the RBR table
	namespace := mustRetrieveNamespaceElem(b, tableID)
	regionConfig, err := b.SynthesizeRegionConfig(b, namespace.DatabaseID)
	if err != nil {
		panic(err)
	}
	newColName := explicitRegionColName(targetLocality.RegionalByRowColumn)
	allowedNewColNames := []tree.Name{newColName}
	partitionBy := multiregion.PartitionByForRegionalByRow(regionConfig, newColName)
	colName, err := findColumnByNameOnTable(b, tableID, newColName, allowedNewColNames)
	implicitCols := []*scpb.ColumnName{colName}
	if err != nil {
		panic(err)
	}
	return partitioningSpec{
		PartitionBy:           partitionBy,
		NewImplicitColumns:    implicitCols,
		AllowedNewColumnNames: allowedNewColNames,
	}
}

func retrieveNonImplicitIndexColumns(
	b BuildCtx, tableID catid.DescID, indexID catid.IndexID,
) tree.IndexElemList {
	keyColumns := mustRetrieveKeyIndexColumns(b, tableID, indexID)
	cols := make(tree.IndexElemList, len(keyColumns))
	i := 0
	for _, keyCol := range keyColumns {
		colName := mustRetrieveColumnName(b, tableID, keyCol.ColumnID)
		if keyCol.Implicit {
			continue
		}
		cols[i] = tree.IndexElem{
			Column:    tree.Name(colName.Name),
			Direction: getIndexColDir(keyCol),
		}
		i += 1
	}
	cols = cols[:i]
	return cols
}

func getIndexColDir(col *scpb.IndexColumn) tree.Direction {
	switch col.Direction {
	case catenumpb.IndexColumn_ASC:
		return tree.Ascending
	case catenumpb.IndexColumn_DESC:
		return tree.Descending
	}
	panic(pgerror.Newf(pgcode.AssertFailure, "invalid index direction %s", col.Direction))
}

func createOrVerifyRegionColumn(
	b BuildCtx,
	tableID catid.DescID,
	tableName string,
	regionEnumTypeID catid.DescID,
	currentLocality *tree.Locality,
	targetLocality *tree.Locality,
	tbl *scpb.Table,
	t *tree.AlterTableLocality,
) {
	// Check if the region column exists already - if so, use it.
	// Otherwise, if we have no name was specified, implicitly create the
	// crdb_region column.
	createDefaultRegionCol := false
	mayNeedImplicitRegionCol := regionColNameNotSpecified(targetLocality.RegionalByRowColumn)
	newRegionColName := explicitRegionColName(targetLocality.RegionalByRowColumn)

	colElems := b.ResolveColumn(tableID, newRegionColName, ResolveParams{IsExistenceOptional: true})
	regionCol := colElems.FilterColumn().MustGetZeroOrOneElement()
	if regionCol == nil {
		if mayNeedImplicitRegionCol {
			createDefaultRegionCol = true
		} else {
			panic(pgerror.Newf(pgcode.UndefinedColumn, "column %q does not exist", newRegionColName))
		}
	}

	panicIfTableLocalityRegionalByRowUsingConstraint(b, tableID, currentLocality, newRegionColName)

	if !createDefaultRegionCol {
		// If the column is not public, we cannot use it yet.
		checkExistingColIsValidForRegionCol(b, tableID, regionEnumTypeID, regionCol, newRegionColName, tableName)
	} else {
		createDefaultRegionColumn(b, tbl, regionEnumTypeID, t)
	}
}

func panicIfTableLocalityRegionalByRowUsingConstraint(
	b BuildCtx, tableID catid.DescID, currentLocality *tree.Locality, targetColName tree.Name,
) {
	if !isLocalityRegionalByRow(currentLocality) {
		return
	}
	currColName := explicitRegionColName(currentLocality.RegionalByRowColumn)
	rbrUsingConstaint := b.QueryByID(tableID).FilterTableLocalityRegionalByRowUsingConstraint().MustGetZeroOrOneElement()
	if rbrUsingConstaint != nil && currColName != targetColName {
		panic(pgerror.Newf(
			pgcode.InvalidTableDefinition,
			`cannot change the REGIONAL BY ROW column from %s to %s when "%s" is set`,
			currColName, targetColName, catpb.RBRUsingConstraintTableSettingName,
		))
	}
}

func regionColNameNotSpecified(colName tree.Name) bool {
	return colName == tree.PrimaryRegionNotSpecifiedName
}

func explicitRegionColName(regionColName tree.Name) tree.Name {
	if regionColName == tree.RegionalByRowRegionNotSpecifiedName {
		return tree.RegionalByRowRegionDefaultColName
	}
	return regionColName
}

func createDefaultRegionColumn(
	b BuildCtx, tbl *scpb.Table, regionEnumTypeID catid.DescID, t *tree.AlterTableLocality,
) {
	enumOID := catid.TypeIDToOID(regionEnumTypeID)
	primaryRegion := getDatabasePrimaryRegionName(b, tbl.TableID)

	// Generate the ID of the new region column
	newColID := b.NextTableColumnID(tbl)
	colType := &scpb.ColumnType{
		TableID:                 tbl.TableID,
		ColumnID:                newColID,
		TypeT:                   b.ResolveTypeRef(&tree.OIDTypeReference{OID: enumOID}),
		ElementCreationMetadata: scdecomp.NewElementCreationMetadata(b.EvalCtx().Settings.Version.ActiveVersion(b)),
	}

	transientComputeExpr := regionalByRowRegionDefaultExpr(enumOID, tree.Name(primaryRegion))
	defaultExpr := multiregion.RegionalByRowGatewayRegionDefaultExpr(enumOID)
	onUpdateExpr := multiregion.MaybeRegionalByRowOnUpdateExpr(b.EvalCtx(), enumOID)

	// Add the spec for the new column. Use a transient computed expression
	// to initialize the column value to the primary region.
	spec := addColumnSpec{
		tbl: tbl,
		col: &scpb.Column{
			TableID:  tbl.TableID,
			ColumnID: newColID,
			IsHidden: true,
		},
		name: &scpb.ColumnName{
			TableID:  tbl.TableID,
			ColumnID: newColID,
			Name:     string(tree.RegionalByRowRegionDefaultColName),
		},
		def: &scpb.ColumnDefaultExpression{
			TableID:    tbl.TableID,
			ColumnID:   newColID,
			Expression: *wrapRegionalByRowExpression(b, tbl, colType, defaultExpr),
		},
		colType: colType,
		transientCompute: &scpb.ColumnComputeExpression{
			TableID:    tbl.TableID,
			ColumnID:   newColID,
			Expression: *wrapRegionalByRowExpression(b, tbl, colType, transientComputeExpr),
			Usage:      scpb.ColumnComputeExpression_ALTER_TYPE_USING,
		},
		hidden:  true,
		notNull: true,
	}
	if onUpdateExpr != nil {
		spec.onUpdate = &scpb.ColumnOnUpdateExpression{
			TableID:    tbl.TableID,
			ColumnID:   newColID,
			Expression: *wrapRegionalByRowExpression(b, tbl, colType, onUpdateExpr),
		}
	}
	addColumn(b, spec, t)
}

func regionalByRowRegionDefaultExpr(oid oid.Oid, region tree.Name) tree.Expr {
	return &tree.CastExpr{
		Expr:       tree.NewDString(string(region)),
		Type:       &tree.OIDTypeReference{OID: oid},
		SyntaxMode: tree.CastShort,
	}
}

func wrapRegionalByRowExpression(
	b BuildCtx, tbl *scpb.Table, colType *scpb.ColumnType, expr tree.Expr,
) *scpb.Expression {
	typedExpr, _, err := sanitizeColumnExpression(
		b,
		b.SemaCtx(),
		expr,
		colType,
		tree.RegionalByRowRegionDefaultExpr,
	)
	if err != nil {
		panic(err)
	}
	return b.WrapExpression(tbl.TableID, typedExpr)
}

func checkExistingColIsValidForRegionCol(
	b BuildCtx,
	tableID catid.DescID,
	regionEnumTypeID catid.DescID,
	partCol *scpb.Column,
	partColName tree.Name,
	tableName string,
) {
	if partCol == nil {
		panic(colinfo.NewUndefinedColumnError(string(partColName)))
	}

	// If we already have a column with the given name, check it is compatible to be made
	// a PRIMARY KEY.
	colID := getColumnIDFromColumnName(b, tableID, partColName, true /* required */)
	colType := retrieveColumnTypeElem(b, tableID, colID)
	if colType.Type.Oid() != catid.TypeIDToOID(regionEnumTypeID) {
		panic(pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"cannot use column %s for REGIONAL BY ROW table as it does not have the %s type",
			partColName,
			tree.RegionEnum,
		))
	}

	// Check whether the given row is NOT NULL.
	columNotNull := b.QueryByID(tableID).FilterColumnNotNull().Filter(func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.ColumnNotNull) bool {
		return e.ColumnID == partCol.ColumnID
	}).MustGetZeroOrOneElement()
	if columNotNull == nil {
		panic(errors.WithHintf(
			pgerror.Newf(
				pgcode.InvalidTableDefinition,
				"cannot use column %s for REGIONAL BY ROW table as it may contain NULL values",
				partColName,
			),
			"Add the NOT NULL constraint first using ALTER TABLE %s ALTER COLUMN %s SET NOT NULL.",
			tableName,
			partColName,
		))
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
