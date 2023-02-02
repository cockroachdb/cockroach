// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildstmt

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdecomp"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/storageparam"
	"github.com/cockroachdb/cockroach/pkg/sql/storageparam/indexstorageparam"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// CreateIndex implements CREATE INDEX.
func CreateIndex(b BuildCtx, n *tree.CreateIndex) {
	// Resolve the table name and start building the new index element.
	relationElements := b.ResolveRelation(n.Table.ToUnresolvedObjectName(), ResolveParams{
		IsExistenceOptional: false,
		RequiredPrivilege:   privilege.CREATE,
	})
	// We don't support handling zone config related properties for tables, so
	// throw an unsupported error.
	if _, _, tbl := scpb.FindTable(relationElements); tbl != nil {
		fallBackIfZoneConfigExists(b, n, tbl.TableID)
	}
	_, _, partitioning := scpb.FindTablePartitioning(relationElements)
	if partitioning != nil && n.PartitionByIndex != nil &&
		n.PartitionByIndex.ContainsPartitions() {
		panic(pgerror.New(
			pgcode.FeatureNotSupported,
			"cannot define PARTITION BY on an index if the table has a PARTITION ALL BY definition",
		))
	}

	// Inverted indexes do not support hash sharding or unique.
	if n.Inverted {
		if n.Sharded != nil {
			panic(pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes don't support hash sharding"))
		}
		if len(n.Storing) > 0 {
			panic(pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes don't support stored columns"))
		}
		if n.Unique {
			panic(pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes can't be unique"))
		}
		b.IncrementSchemaChangeIndexCounter("inverted_index")
		if len(n.Columns) > 0 {
			b.IncrementSchemaChangeIndexCounter("multi_column_inverted_index")
		}
	}
	var idxSpec indexSpec
	idxSpec.secondary = &scpb.SecondaryIndex{
		Index: scpb.Index{
			IsInverted:     n.Inverted,
			IsConcurrently: n.Concurrently,
			IsNotVisible:   n.NotVisible,
		},
	}
	var relation scpb.Element
	var sourceIndex *scpb.PrimaryIndex
	relationElements.ForEachElementStatus(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.Table:
			n.Table.ObjectNamePrefix = b.NamePrefix(t)
			if descpb.IsVirtualTable(t.TableID) {
				return
			}
			idxSpec.secondary.TableID = t.TableID
			relation = e

		case *scpb.View:
			n.Table.ObjectNamePrefix = b.NamePrefix(t)
			if !t.IsMaterialized {
				return
			}
			if n.Sharded != nil {
				panic(pgerror.New(pgcode.InvalidObjectDefinition,
					"cannot create hash sharded index on materialized view"))
			}
			idxSpec.secondary.TableID = t.ViewID
			relation = e

		case *scpb.TableLocalityGlobal, *scpb.TableLocalityPrimaryRegion, *scpb.TableLocalitySecondaryRegion:
			if n.PartitionByIndex != nil {
				panic(pgerror.New(pgcode.FeatureNotSupported,
					"cannot define PARTITION BY on a new INDEX in a multi-region database",
				))
			}

		case *scpb.TableLocalityRegionalByRow:
			if n.PartitionByIndex != nil {
				panic(pgerror.New(pgcode.FeatureNotSupported,
					"cannot define PARTITION BY on a new INDEX in a multi-region database",
				))
			}
			if n.Sharded != nil {
				panic(pgerror.New(pgcode.FeatureNotSupported, "hash sharded indexes are not compatible with REGIONAL BY ROW tables"))
			}

		case *scpb.PrimaryIndex:
			// TODO(ajwerner): This is too simplistic. We should build a better
			// vocabulary around the possible primary indexes in play. There are
			// at most going to be 3, and at least there is going to be 1. If
			// there are no column set changes, or there's just additions of
			// nullable columns there'll be just one. If there are only either
			// adds or drops, but not both, there will be two, the initial and
			// the final. If there are both adds and drops, then there will be
			// 3, including an intermediate primary index which is keyed on the
			// initial primary key and include the union of all of the added and
			// dropped columns.
			if target == scpb.ToPublic {
				sourceIndex = t
			}
		}
	})
	if idxSpec.secondary.TableID == catid.InvalidDescID || sourceIndex == nil {
		panic(pgerror.Newf(pgcode.WrongObjectType,
			"%q is not a table or materialized view", n.Table.ObjectName))
	}
	if n.Unique {
		idxSpec.secondary.IsUnique = true
	}
	// Resolve the index name and make sure it doesn't exist yet.
	{
		indexElements := b.ResolveIndex(idxSpec.secondary.TableID, n.Name, ResolveParams{
			IsExistenceOptional: true,
			RequiredPrivilege:   privilege.CREATE,
		})
		if _, target, sec := scpb.FindSecondaryIndex(indexElements); sec != nil {
			if n.IfNotExists {
				return
			}
			if target == scpb.ToAbsent {
				panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"index %q being dropped, try again later", n.Name.String()))
			}
			panic(pgerror.Newf(pgcode.DuplicateRelation, "index with name %q already exists", n.Name))
		}
	}
	// Assign the ID here, since we may have added columns
	// and made a new primary key above.
	idxSpec.secondary.SourceIndexID = sourceIndex.IndexID
	idxSpec.secondary.IndexID = nextRelationIndexID(b, relation)
	idxSpec.secondary.TemporaryIndexID = idxSpec.secondary.IndexID + 1
	// Add columns for the secondary index.
	addColumnsForSecondaryIndex(b, n, relation, &idxSpec)
	// If necessary set up the partitioning descriptor for the index.
	maybeAddPartitionDescriptorForIndex(b, n, &idxSpec)
	// If necessary setup a partial predicate
	maybeAddIndexPredicate(b, n, &idxSpec)
	// Picks up any geoconfig parameters, hash sharded one are
	// picked independently.
	maybeApplyStorageParameters(b, n, &idxSpec)
	// Assign the secondary constraint ID now, since we may have added a check
	// constraint earlier.
	if idxSpec.secondary.IsUnique {
		idxSpec.secondary.ConstraintID = b.NextTableConstraintID(idxSpec.secondary.TableID)
	}
	keyIdx := 0
	keySuffixIdx := 0
	keyStoredIdx := 0
	for _, ic := range idxSpec.columns {
		if ic.Kind == scpb.IndexColumn_KEY {
			ic.OrdinalInKind = uint32(keyIdx)
			keyIdx++
		} else if ic.Kind == scpb.IndexColumn_KEY_SUFFIX {
			ic.OrdinalInKind = uint32(keySuffixIdx)
			keySuffixIdx++
		} else if ic.Kind == scpb.IndexColumn_STORED {
			ic.OrdinalInKind = uint32(keyStoredIdx)
			keyStoredIdx++
		} else {
			panic(errors.AssertionFailedf("unknown index column type %s", ic.Kind))
		}
	}
	idxSpec.data = &scpb.IndexData{TableID: idxSpec.secondary.TableID, IndexID: idxSpec.secondary.IndexID}
	idxSpec.apply(b.Add)
	b.LogEventForExistingTarget(idxSpec.secondary)
	// Apply the name once everything else has been created since we need
	// elements to be added, so that getImplicitSecondaryIndexName can make
	// an implicit name if one is required.
	indexName := string(n.Name)
	if indexName == "" {
		numImplicitColumns := 0
		if idxSpec.partitioning != nil {
			numImplicitColumns = int(idxSpec.partitioning.NumImplicitColumns)
		}
		indexName = getImplicitSecondaryIndexName(b,
			screl.GetDescID(relation),
			idxSpec.secondary.IndexID,
			numImplicitColumns,
		)
	}
	b.Add(&scpb.IndexName{
		TableID: idxSpec.secondary.TableID,
		IndexID: idxSpec.secondary.IndexID,
		Name:    indexName,
	})
	// Construct the temporary objects from the index spec, since these will
	// be transient.
	tempIdxSpec := makeTempIndexSpec(idxSpec)
	tempIdxSpec.apply(b.AddTransient)
	// If the concurrent option is added emit a warning.
	if n.Concurrently {
		b.EvalCtx().ClientNoticeSender.BufferClientNotice(b,
			pgnotice.Newf("CONCURRENTLY is not required as all indexes are created concurrently"),
		)
	}
}

func nextRelationIndexID(b BuildCtx, relation scpb.Element) catid.IndexID {
	switch t := relation.(type) {
	case *scpb.Table:
		return b.NextTableIndexID(t)
	case *scpb.View:
		return b.NextViewIndexID(t)
	default:
		panic(errors.AssertionFailedf("unexpected relation element of type %T", relation))
	}
}

func newUndefinedOpclassError(opclass tree.Name) error {
	return pgerror.Newf(pgcode.UndefinedObject, "operator class %q does not exist", opclass)
}

// checkColumnAccessibilityForIndex validate that any columns that are explicitly referenced in a column for storage or
// as a key are either accessible and not system columns.
func checkColumnAccessibilityForIndex(colName string, columnElts ElementResultSet, store bool) {
	_, _, column := scpb.FindColumn(columnElts)
	if column.IsInaccessible {
		panic(pgerror.Newf(
			pgcode.UndefinedColumn,
			"column %q is inaccessible and cannot be referenced",
			colName))
	}

	if column.IsSystemColumn {
		if store {
			panic(pgerror.Newf(
				pgcode.FeatureNotSupported,
				"index cannot store system column %s",
				colName))
		} else {
			panic(pgerror.Newf(
				pgcode.FeatureNotSupported,
				"cannot index system column %s",
				colName))
		}
	}
}

// processColNodeType validates that the given type for a column is properly
// indexable (including for inverted indexes). Additionally, the OpClass if one
// is specified is validated, which is used to determine how values will be
// compared/stored within the index.
func processColNodeType(
	b BuildCtx,
	n *tree.CreateIndex,
	indexSpec *indexSpec,
	colName string,
	columnNode tree.IndexElem,
	columnType *scpb.ColumnType,
	lastColIdx bool,
) catpb.InvertedIndexColumnKind {
	invertedKind := catpb.InvertedIndexColumnKind_DEFAULT
	// OpClass are only allowed for the last column of an inverted index.
	if columnNode.OpClass != "" && (!lastColIdx || !n.Inverted) {
		panic(pgerror.New(pgcode.DatatypeMismatch,
			"operator classes are only allowed for the last column of an inverted index"))
	}
	// Disallow descending last columns in inverted indexes.
	if n.Inverted && columnNode.Direction == tree.Descending {
		panic(pgerror.New(pgcode.FeatureNotSupported,
			"the last column in an inverted index cannot have the DESC option"))
	}
	if n.Inverted && lastColIdx {
		switch columnType.Type.Family() {
		case types.ArrayFamily:
			switch columnNode.OpClass {
			case "array_ops", "":
			default:
				panic(newUndefinedOpclassError(columnNode.OpClass))
			}
		case types.JsonFamily:
			switch columnNode.OpClass {
			case "jsonb_ops", "":
			case "jsonb_path_ops":
				panic(unimplemented.NewWithIssue(81115, "operator class \"jsonb_path_ops\" is not supported"))
			default:
				panic(newUndefinedOpclassError(columnNode.OpClass))
			}
		case types.GeometryFamily:
			if columnNode.OpClass != "" {
				panic(newUndefinedOpclassError(columnNode.OpClass))
			}
			config, err := geoindex.GeometryIndexConfigForSRID(columnType.Type.GeoSRIDOrZero())
			if err != nil {
				panic(err)
			}
			indexSpec.secondary.GeoConfig = config
			b.IncrementSchemaChangeIndexCounter("geometry_inverted")
		case types.GeographyFamily:
			if columnNode.OpClass != "" {
				panic(newUndefinedOpclassError(columnNode.OpClass))
			}
			if columnNode.OpClass != "" {
				panic(newUndefinedOpclassError(columnNode.OpClass))
			}
			indexSpec.secondary.GeoConfig = geoindex.DefaultGeographyIndexConfig()
			b.IncrementSchemaChangeIndexCounter("geography_inverted")
		case types.StringFamily:
			// Check the opclass of the last column in the list, which is the column
			// we're going to inverted index.
			switch columnNode.OpClass {
			case "gin_trgm_ops", "gist_trgm_ops":
				if !b.EvalCtx().Settings.Version.IsActive(b, clusterversion.V22_2TrigramInvertedIndexes) {
					panic(pgerror.Newf(pgcode.FeatureNotSupported,
						"version %v must be finalized to create trigram inverted indexes",
						clusterversion.ByKey(clusterversion.V22_2TrigramInvertedIndexes)))
				}
			case "":
				panic(errors.WithHint(
					pgerror.New(pgcode.UndefinedObject, "data type text has no default operator class for access method \"gin\""),
					"You must specify an operator class for the index (did you mean gin_trgm_ops?)"))
			default:
				panic(newUndefinedOpclassError(columnNode.OpClass))
			}
			invertedKind = catpb.InvertedIndexColumnKind_TRIGRAM
			b.IncrementSchemaChangeIndexCounter("trigram_inverted")

		}
		relationElts := b.QueryByID(indexSpec.secondary.TableID)
		scpb.ForEachIndexColumn(relationElts, func(current scpb.Status, target scpb.TargetStatus, e *scpb.IndexColumn) {
			if target == scpb.ToPublic &&
				e.IndexID == indexSpec.secondary.SourceIndexID &&
				e.ColumnID == columnType.ColumnID &&
				e.Kind == scpb.IndexColumn_KEY {
				panic(unimplemented.NewWithIssuef(84405,
					"primary key column %s cannot be present in an inverted index",
					colName,
				))
			}
		})
	}
	// Only certain column types are supported for inverted indexes.
	if n.Inverted && lastColIdx &&
		!colinfo.ColumnTypeIsInvertedIndexable(columnType.Type) {
		colNameForErr := colName
		if columnNode.Expr != nil {
			colNameForErr = columnNode.Expr.String()
		}
		panic(tabledesc.NewInvalidInvertedColumnError(colNameForErr,
			columnType.Type.String()))
	} else if (!n.Inverted || !lastColIdx) &&
		!colinfo.ColumnTypeIsIndexable(columnType.Type) {
		// Otherwise, check if the column type is indexable.
		panic(unimplemented.NewWithIssueDetailf(35730,
			columnType.Type.DebugString(),
			"column %s is of type %s and thus is not indexable",
			colName,
			columnType.Type))
	}
	return invertedKind
}

// maybeAddPartitionDescriptorForIndex adds a partitioning descriptor and
// any implicit columns needed to support it.
func maybeAddPartitionDescriptorForIndex(b BuildCtx, n *tree.CreateIndex, idxSpec *indexSpec) {
	tableID := idxSpec.secondary.TableID
	relationElts := b.QueryByID(tableID)
	// If partition by all is required, then setup the information
	// for that.
	_, _, partitioning := scpb.FindTablePartitioning(relationElts)
	if partitioning != nil {
		// Update AST to reflect the partition information.
		n.PartitionByIndex = &tree.PartitionByIndex{
			PartitionBy: &tree.PartitionBy{},
		}
		var indexImplicitCols []*scpb.IndexColumn
		scpb.ForEachIndexColumn(relationElts, func(current scpb.Status, target scpb.TargetStatus, e *scpb.IndexColumn) {
			if e.IndexID == idxSpec.secondary.SourceIndexID && e.Implicit {
				indexImplicitCols = append(indexImplicitCols, e)
			}
		})
		sort.Slice(indexImplicitCols, func(i, j int) bool {
			return indexImplicitCols[i].OrdinalInKind < indexImplicitCols[j].OrdinalInKind
		})

		for _, ic := range indexImplicitCols {
			columnName := mustRetrieveColumnNameElem(b, ic.TableID, ic.ColumnID)
			n.PartitionByIndex.Fields = append(n.PartitionByIndex.Fields, tree.Name(columnName.Name))
		}
		// Recover the rest from the partitioning data.
		scpb.ForEachIndexPartitioning(relationElts, func(current scpb.Status, target scpb.TargetStatus, e *scpb.IndexPartitioning) {
			if e.IndexID == idxSpec.secondary.SourceIndexID {
				idxSpec.partitioning = protoutil.Clone(e).(*scpb.IndexPartitioning)
			}
		})
	}
	if n.PartitionByIndex.ContainsPartitions() || idxSpec.partitioning != nil {
		// Detect if the partitioning overlaps with any of the secondary index
		// columns.
		{
			implicitColumns := make(map[catid.ColumnID]struct{})
			_, _, primaryIdx := scpb.FindPrimaryIndex(relationElts)
			scpb.ForEachIndexColumn(relationElts, func(current scpb.Status, target scpb.TargetStatus, e *scpb.IndexColumn) {
				if e.IndexID == primaryIdx.IndexID {
					if e.Implicit {
						implicitColumns[e.ColumnID] = struct{}{}
					}
				}
			})
			for _, col := range idxSpec.columns {
				if _, ok := implicitColumns[col.ColumnID]; !col.Implicit && ok {
					panic(pgerror.New(
						pgcode.FeatureNotSupported,
						`hash sharded indexes cannot include implicit partitioning columns from "PARTITION ALL BY" or "LOCALITY REGIONAL BY ROW"`,
					))
				}
			}
		}
		if idxSpec.partitioning == nil {
			idxSpec.partitioning = &scpb.IndexPartitioning{
				TableID: idxSpec.secondary.TableID,
				IndexID: idxSpec.secondary.IndexID,
				PartitioningDescriptor: b.IndexPartitioningDescriptor(n.Name.String(),
					&idxSpec.secondary.Index, idxSpec.columns, n.PartitionByIndex.PartitionBy),
			}
		} else {
			idxSpec.partitioning.IndexID = idxSpec.secondary.IndexID
		}
		var columnsToPrepend []*scpb.IndexColumn
		for _, field := range n.PartitionByIndex.Fields {
			// Resolve the column first.
			ers := b.ResolveColumn(tableID, field, ResolveParams{RequiredPrivilege: privilege.CREATE})
			_, _, fieldColumn := scpb.FindColumn(ers)
			// If it already in the index we are done.
			if fieldColumn.ColumnID == idxSpec.columns[0].ColumnID {
				break
			}
			newIndexColumn := &scpb.IndexColumn{
				IndexID:   idxSpec.secondary.IndexID,
				TableID:   fieldColumn.TableID,
				ColumnID:  fieldColumn.ColumnID,
				Kind:      scpb.IndexColumn_KEY,
				Direction: catenumpb.IndexColumn_ASC,
				Implicit:  true,
			}
			// Check if the column is already a suffix, then we should
			// move it out.
			for pos, otherIC := range idxSpec.columns {
				if otherIC.ColumnID == fieldColumn.ColumnID {
					idxSpec.columns = append(idxSpec.columns[:pos], idxSpec.columns[pos+1:]...)
					break
				}
			}
			columnsToPrepend = append(columnsToPrepend, newIndexColumn)
		}
		idxSpec.columns = append(columnsToPrepend, idxSpec.columns...)
		if n.Inverted {
			b.IncrementSchemaChangeIndexCounter("partitioned_inverted")
		}
	}
	// Warn against creating a non-partitioned index on a partitioned table,
	// which is undesirable in most cases.
	// Avoid the warning if we have PARTITION ALL BY as all indexes will implicitly
	// have relevant partitioning columns prepended at the front.
	scpb.ForEachIndexPartitioning(b, func(current scpb.Status, target scpb.TargetStatus, e *scpb.IndexPartitioning) {
		if target == scpb.ToPublic &&
			e.IndexID == idxSpec.secondary.SourceIndexID && e.TableID == idxSpec.secondary.TableID &&
			e.NumColumns > 0 &&
			partitioning == nil {
			b.EvalCtx().ClientNoticeSender.BufferClientNotice(
				b,
				errors.WithHint(
					pgnotice.Newf("creating non-partitioned index on partitioned table may not be performant"),
					"Consider modifying the index such that it is also partitioned.",
				),
			)
		}
	})
}

// addColumnsForSecondaryIndex updates the index spec to add columns needed
// for a secondary index.
func addColumnsForSecondaryIndex(
	b BuildCtx, n *tree.CreateIndex, relation scpb.Element, idxSpec *indexSpec,
) {
	tableID := screl.GetDescID(relation)
	relationElements := b.QueryByID(tableID)

	// Check that the index creation spec is sane.
	columnRefs := map[string]struct{}{}
	columnExprRefs := map[string]struct{}{}
	for _, columnNode := range n.Columns {
		// Detect if either the same column or expression repeats twice in this
		// index definition. For example:
		// 1) CREATE INDEX idx ON t(i, i)
		// 2) CREATE INDEX idx ON t(lower(i), j, lower(i)).
		if columnNode.Expr == nil {
			colName := columnNode.Column.Normalize()
			if _, found := columnRefs[colName]; found {
				panic(pgerror.Newf(pgcode.InvalidObjectDefinition,
					"index %q contains duplicate column %q", n.Name, colName))
			}
			columnRefs[colName] = struct{}{}
		} else {
			colExpr := columnNode.Expr.String()
			if _, found := columnExprRefs[colExpr]; found {
				panic(pgerror.Newf(pgcode.InvalidObjectDefinition,
					"index %q contains duplicate expression", n.Name))
			}
			columnExprRefs[colExpr] = struct{}{}
		}
	}
	for _, storingNode := range n.Storing {
		colName := storingNode.String()
		if _, found := columnRefs[colName]; found {
			panic(pgerror.Newf(pgcode.InvalidObjectDefinition,
				"index %q already contains column %q", n.Name, colName))
		}
		columnRefs[colName] = struct{}{}
	}
	// Set key column IDs and directions.
	keyColNames := make([]string, len(n.Columns))
	var keyColIDs catalog.TableColSet
	lastColumnIdx := len(n.Columns) - 1
	expressionTelemtryCounted := false
	for i, columnNode := range n.Columns {
		colName := columnNode.Column
		if columnNode.Expr != nil {
			tbl := relation.(*scpb.Table)
			colNameStr := maybeCreateVirtualColumnForIndex(b, &n.Table, tbl, columnNode.Expr, n.Inverted, i == len(n.Columns)-1)
			colName = tree.Name(colNameStr)
			if !expressionTelemtryCounted {
				b.IncrementSchemaChangeIndexCounter("expression")
				expressionTelemtryCounted = true
			}
		}
		colElts := b.ResolveColumn(tableID, colName, ResolveParams{
			RequiredPrivilege: privilege.CREATE,
		})
		_, _, column := scpb.FindColumn(colElts)
		_, _, columnType := scpb.FindColumnType(colElts)
		invertedKind := processColNodeType(b, n, idxSpec, string(colName), columnNode, columnType, i == lastColumnIdx)
		// Column should be accessible.
		if columnNode.Expr == nil {
			checkColumnAccessibilityForIndex(string(colName), colElts, false)
		}
		keyColNames[i] = string(colName)
		direction := catenumpb.IndexColumn_ASC
		if columnNode.Direction == tree.Descending {
			direction = catenumpb.IndexColumn_DESC
		}
		ic := &scpb.IndexColumn{
			TableID:       idxSpec.secondary.TableID,
			IndexID:       idxSpec.secondary.IndexID,
			ColumnID:      column.ColumnID,
			OrdinalInKind: uint32(i),
			Kind:          scpb.IndexColumn_KEY,
			Direction:     direction,
			InvertedKind:  invertedKind,
		}
		idxSpec.columns = append(idxSpec.columns, ic)
		keyColIDs.Add(column.ColumnID)
	}
	// Set the key suffix column IDs.
	// We want to find the key column IDs
	var keySuffixColumns []*scpb.IndexColumn
	scpb.ForEachIndexColumn(relationElements, func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.IndexColumn,
	) {
		if e.IndexID != idxSpec.secondary.SourceIndexID || keyColIDs.Contains(e.ColumnID) ||
			e.Kind != scpb.IndexColumn_KEY {
			return
		}
		// Check if the column name was duplicated from the STORING clause, in which
		// case this isn't allowed.
		// Note: The column IDs for the key suffix columns are derived by finding
		// columns in the primary index, which  are not covered by the key columns
		// within the index. We checked against the key columns vs the storing ones,
		// earlier so this covers any extra columns.
		columnName := mustRetrieveColumnNameElem(b, e.TableID, e.ColumnID)
		if _, found := columnRefs[columnName.Name]; found {
			panic(pgerror.Newf(pgcode.InvalidObjectDefinition,
				"index %q already contains column %q", n.Name, columnName.Name))
		}
		columnRefs[columnName.Name] = struct{}{}
		keySuffixColumns = append(keySuffixColumns, e)
	})
	sort.Slice(keySuffixColumns, func(i, j int) bool {
		return keySuffixColumns[i].OrdinalInKind < keySuffixColumns[j].OrdinalInKind
	})
	for i, c := range keySuffixColumns {
		ic := &scpb.IndexColumn{
			TableID:       idxSpec.secondary.TableID,
			IndexID:       idxSpec.secondary.IndexID,
			ColumnID:      c.ColumnID,
			OrdinalInKind: uint32(i),
			Kind:          scpb.IndexColumn_KEY_SUFFIX,
			Direction:     c.Direction,
			Implicit:      c.Implicit,
		}
		idxSpec.columns = append(idxSpec.columns, ic)
	}

	// Set the storing column IDs.
	for i, storingNode := range n.Storing {
		colElts := b.ResolveColumn(tableID, storingNode, ResolveParams{
			RequiredPrivilege: privilege.CREATE,
		})
		_, _, column := scpb.FindColumn(colElts)
		checkColumnAccessibilityForIndex(storingNode.String(), colElts, true)
		c := &scpb.IndexColumn{
			TableID:       idxSpec.secondary.TableID,
			IndexID:       idxSpec.secondary.IndexID,
			ColumnID:      column.ColumnID,
			OrdinalInKind: uint32(i),
			Kind:          scpb.IndexColumn_STORED,
		}
		idxSpec.columns = append(idxSpec.columns, c)
	}
	// Set up sharding.
	if n.Sharded != nil {
		b.IncrementSchemaChangeIndexCounter("hash_sharded")
		buckets, err := tabledesc.EvalShardBucketCount(b, b.SemaCtx(), b.EvalCtx(), n.Sharded.ShardBuckets, n.StorageParams)
		if err != nil {
			panic(err)
		}
		shardColName := maybeCreateAndAddShardCol(b, int(buckets), relation.(*scpb.Table), keyColNames, n)
		idxSpec.secondary.Sharding = &catpb.ShardedDescriptor{
			IsSharded:    true,
			Name:         shardColName,
			ShardBuckets: buckets,
			ColumnNames:  keyColNames,
		}
		colElts := b.ResolveColumn(tableID, tree.Name(shardColName), ResolveParams{
			RequiredPrivilege: privilege.CREATE,
		})
		_, _, column := scpb.FindColumn(colElts)
		indexColumn := &scpb.IndexColumn{
			IndexID:       idxSpec.secondary.IndexID,
			TableID:       column.TableID,
			ColumnID:      column.ColumnID,
			OrdinalInKind: 0,
			Kind:          scpb.IndexColumn_KEY,
			Direction:     catenumpb.IndexColumn_ASC,
		}
		// Remove the sharded column if it's there in the primary
		// index already, before adding it.
		for pos, ic := range idxSpec.columns {
			if ic.ColumnID == column.ColumnID {
				idxSpec.columns = append(idxSpec.columns[:pos], idxSpec.columns[pos+1:]...)
				break
			}
		}
		idxSpec.columns = append([]*scpb.IndexColumn{indexColumn}, idxSpec.columns...)
	}
}

// maybeCreateAndAddShardCol adds a new hidden computed shard column (or its mutation) to
// `desc`, if one doesn't already exist for the given index column set and number of shard
// buckets.
func maybeCreateAndAddShardCol(
	b BuildCtx, shardBuckets int, tbl *scpb.Table, colNames []string, n tree.NodeFormatter,
) (shardColName string) {
	shardColName = tabledesc.GetShardColumnName(colNames, int32(shardBuckets))
	elts := b.QueryByID(tbl.TableID)
	// TODO(ajwerner): In what ways is the column referenced by
	//  existingShardColID allowed to differ from the newly made shard column?
	//  Should there be some validation of the existing shard column?
	var existingShardColID catid.ColumnID
	scpb.ForEachColumnName(elts, func(_ scpb.Status, target scpb.TargetStatus, name *scpb.ColumnName) {
		if target == scpb.ToPublic && name.Name == shardColName {
			existingShardColID = name.ColumnID
		}
	})
	scpb.ForEachColumn(elts, func(_ scpb.Status, _ scpb.TargetStatus, col *scpb.Column) {
		if col.ColumnID == existingShardColID && !col.IsHidden {
			// The user managed to reverse-engineer our crazy shard column name, so
			// we'll return an error here rather than try to be tricky.
			panic(pgerror.Newf(pgcode.DuplicateColumn,
				"column %s already specified; can't be used for sharding", shardColName))
		}
	})
	if existingShardColID != 0 {
		return shardColName
	}
	expr := schemaexpr.MakeHashShardComputeExpr(colNames, shardBuckets)
	parsedExpr, err := parser.ParseExpr(*expr)
	if err != nil {
		panic(err)
	}
	shardColID := b.NextTableColumnID(tbl)
	spec := addColumnSpec{
		tbl: tbl,
		col: &scpb.Column{
			TableID:        tbl.TableID,
			ColumnID:       shardColID,
			IsHidden:       true,
			PgAttributeNum: catid.PGAttributeNum(shardColID),
		},
		name: &scpb.ColumnName{
			TableID:  tbl.TableID,
			ColumnID: shardColID,
			Name:     shardColName,
		},
		colType: &scpb.ColumnType{
			TableID:                 tbl.TableID,
			ColumnID:                shardColID,
			TypeT:                   scpb.TypeT{Type: types.Int},
			ComputeExpr:             b.WrapExpression(tbl.TableID, parsedExpr),
			IsVirtual:               true,
			IsNullable:              false,
			ElementCreationMetadata: scdecomp.NewElementCreationMetadata(b.EvalCtx().Settings.Version.ActiveVersion(b)),
		},
		notNull: true,
	}
	addColumn(b, spec, n)
	// Create a new check constraint for the hash sharded index column.
	checkConstraintBucketValues := strings.Builder{}
	checkConstraintBucketValues.WriteString(fmt.Sprintf("%q IN (", shardColName))
	for bucket := 0; bucket < shardBuckets; bucket++ {
		checkConstraintBucketValues.WriteString(strconv.Itoa(bucket))
		if bucket != shardBuckets-1 {
			checkConstraintBucketValues.WriteString(",")
		}
	}
	checkConstraintBucketValues.WriteString(")")
	chkConstraintExpr, err := parser.ParseExpr(checkConstraintBucketValues.String())
	if err != nil {
		panic(errors.WithSecondaryError(
			errors.AssertionFailedf("check constraint expression: %q failed to parse",
				checkConstraintBucketValues.String()),
			err))
	}
	shardCheckConstraint := &scpb.CheckConstraint{
		TableID:      tbl.TableID,
		ConstraintID: b.NextTableConstraintID(tbl.TableID),
		Expression: scpb.Expression{
			Expr:                catpb.Expression(checkConstraintBucketValues.String()),
			ReferencedColumnIDs: []catid.ColumnID{shardColID},
		},
		FromHashShardedColumn: true,
	}
	b.Add(shardCheckConstraint)
	shardCheckConstraintName := &scpb.ConstraintWithoutIndexName{
		TableID:      tbl.TableID,
		ConstraintID: shardCheckConstraint.ConstraintID,
		Name:         generateUniqueCheckConstraintName(b, tbl.TableID, chkConstraintExpr),
	}
	b.Add(shardCheckConstraintName)

	return shardColName
}

func maybeCreateVirtualColumnForIndex(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, expr tree.Expr, inverted bool, lastColumn bool,
) string {
	validateColumnIndexableType := func(t *types.T) {
		if t.IsAmbiguous() {
			panic(errors.WithHint(
				pgerror.Newf(
					pgcode.InvalidTableDefinition,
					"type of index element %s is ambiguous",
					expr.String(),
				),
				"consider adding a type cast to the expression",
			))
		}
		// Check if the column type is indexable,
		// non-inverted types.
		if !inverted &&
			!colinfo.ColumnTypeIsIndexable(t) {
			panic(pgerror.Newf(
				pgcode.InvalidTableDefinition,
				"index element %s of type %s is not indexable",
				expr,
				t.Name()))
		}
		// Check if inverted columns are invertible.
		if inverted &&
			!lastColumn &&
			!colinfo.ColumnTypeIsIndexable(t) {
			panic(errors.WithHint(
				pgerror.Newf(
					pgcode.InvalidTableDefinition,
					"index element %s of type %s is not allowed as a prefix column in an inverted index",
					expr.String(),
					t.Name(),
				),
				"see the documentation for more information about inverted indexes: "+docs.URL("inverted-indexes.html"),
			))
		}
		if inverted &&
			lastColumn &&
			!colinfo.ColumnTypeIsInvertedIndexable(t) {
			panic(errors.WithHint(
				pgerror.Newf(
					pgcode.InvalidTableDefinition,
					"index element %s of type %s is not allowed as the last column in an inverted index",
					expr,
					t.Name(),
				),
				"see the documentation for more information about inverted indexes: "+docs.URL("inverted-indexes.html"),
			))
		}
	}
	elts := b.QueryByID(tbl.TableID)
	colName := ""
	// Check if any existing columns can satisfy this expression already, only
	// if it's a virtual column created for an index expression.
	scpb.ForEachColumnType(elts, func(current scpb.Status, target scpb.TargetStatus, e *scpb.ColumnType) {
		column := mustRetrieveColumnElem(b, e.TableID, e.ColumnID)
		if target == scpb.ToPublic && e.ComputeExpr != nil && e.IsVirtual && column.IsInaccessible {
			otherExpr, err := parser.ParseExpr(string(e.ComputeExpr.Expr))
			if err != nil {
				panic(err)
			}
			if otherExpr.String() == expr.String() {
				// We will let the type validation happen like normal, instead of checking here.
				colName = mustRetrieveColumnNameElem(b, e.TableID, e.ColumnID).Name
			}
		}
	})
	if colName != "" {
		return colName
	}
	// Otherwise, we need to create a new column.
	colName = tabledesc.GenerateUniqueName("crdb_internal_idx_expr", func(name string) (found bool) {
		scpb.ForEachColumnName(elts, func(_ scpb.Status, target scpb.TargetStatus, cn *scpb.ColumnName) {
			if target == scpb.ToPublic && cn.Name == name {
				found = true
			}
		})
		return found
	})
	// TODO(postamar): call addColumn instead of building AST.
	d := &tree.ColumnTableDef{
		Name: tree.Name(colName),
	}
	d.Computed.Computed = true
	d.Computed.Virtual = true
	d.Computed.Expr = expr
	d.Nullable.Nullability = tree.Null
	// Infer column type from expression.
	{
		colLookupFn := func(columnName tree.Name) (exists bool, accessible bool, id catid.ColumnID, typ *types.T) {
			scpb.ForEachColumnName(elts, func(_ scpb.Status, target scpb.TargetStatus, cn *scpb.ColumnName) {
				if target == scpb.ToPublic && tree.Name(cn.Name) == columnName {
					id = cn.ColumnID
				}
			})
			if id == 0 {
				return false, false, 0, nil
			}
			scpb.ForEachColumn(elts, func(_ scpb.Status, target scpb.TargetStatus, col *scpb.Column) {
				if target == scpb.ToPublic && col.ColumnID == id {
					exists = true
					accessible = !col.IsInaccessible
				}
			})
			scpb.ForEachColumnType(elts, func(_ scpb.Status, target scpb.TargetStatus, col *scpb.ColumnType) {
				if target == scpb.ToPublic && col.ColumnID == id {
					typ = col.Type
				}
			})
			return exists, accessible, id, typ
		}
		replacedExpr, _, err := schemaexpr.ReplaceColumnVars(expr, colLookupFn)
		if err != nil {
			panic(err)
		}
		typedExpr, err := tree.TypeCheck(b, replacedExpr, b.SemaCtx(), types.Any)
		if err != nil {
			panic(err)
		}
		d.Type = typedExpr.ResolvedType()
		validateColumnIndexableType(typedExpr.ResolvedType())
	}
	alterTableAddColumn(b, tn, tbl, &tree.AlterTableAddColumn{ColumnDef: d})
	// When a virtual column for an index expression gets added for CREATE INDEX
	// it must be inaccessible, so we will manipulate the newly added element to
	// be inaccessible after going through the normal add column code path.
	{
		ers := b.ResolveColumn(tbl.TableID, d.Name, ResolveParams{
			RequiredPrivilege: privilege.CREATE,
		})
		_, _, col := scpb.FindColumn(ers)
		col.IsInaccessible = true
	}
	return colName
}

func maybeAddIndexPredicate(b BuildCtx, n *tree.CreateIndex, idxSpec *indexSpec) {
	if n.Predicate == nil {
		return
	}
	expr := b.PartialIndexPredicateExpression(idxSpec.secondary.TableID, n.Predicate)
	idxSpec.partial = &scpb.SecondaryIndexPartial{
		TableID:    idxSpec.secondary.TableID,
		IndexID:    idxSpec.secondary.IndexID,
		Expression: *expr,
	}
	b.IncrementSchemaChangeIndexCounter("partial")
	if n.Inverted {
		b.IncrementSchemaChangeIndexCounter("partial_inverted")
	}
}

// maybeApplyStorageParameters apply any storage parameters into the index spec,
// this is only used for GeoConfig today.
func maybeApplyStorageParameters(b BuildCtx, n *tree.CreateIndex, idxSpec *indexSpec) {
	if len(n.StorageParams) == 0 {
		return
	}
	dummyIndexDesc := &descpb.IndexDescriptor{}
	if idxSpec.secondary.GeoConfig != nil {
		dummyIndexDesc.GeoConfig = *idxSpec.secondary.GeoConfig
	}
	storageParamSetter := &indexstorageparam.Setter{
		IndexDesc: dummyIndexDesc,
	}
	err := storageparam.Set(b, b.SemaCtx(), b.EvalCtx(), n.StorageParams, storageParamSetter)
	if err != nil {
		panic(err)
	}
	if !dummyIndexDesc.GeoConfig.IsEmpty() {
		idxSpec.secondary.GeoConfig = &dummyIndexDesc.GeoConfig
	} else {
		idxSpec.secondary.GeoConfig = nil
	}
}
