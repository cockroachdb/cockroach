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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
	index := scpb.Index{
		IsUnique:       n.Unique,
		IsInverted:     n.Inverted,
		IsConcurrently: n.Concurrently,
		IsNotVisible:   n.NotVisible,
	}
	var relation scpb.Element
	var source *scpb.PrimaryIndex
	relationElements.ForEachElementStatus(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.Table:
			n.Table.ObjectNamePrefix = b.NamePrefix(t)
			if descpb.IsVirtualTable(t.TableID) {
				return
			}
			index.TableID = t.TableID
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
			index.TableID = t.ViewID
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
				source = t
			}
		}
	})
	if n.Unique {
		index.ConstraintID = b.NextTableConstraintID(index.TableID)
	}
	if index.TableID == catid.InvalidDescID || source == nil {
		panic(pgerror.Newf(pgcode.WrongObjectType,
			"%q is not an indexable table or a materialized view", n.Table.ObjectName))
	}
	// Resolve the index name and make sure it doesn't exist yet.
	{
		indexElements := b.ResolveIndex(index.TableID, n.Name, ResolveParams{
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
	// Check that the index creation spec is sane.
	columnRefs := map[string]struct{}{}
	for _, columnNode := range n.Columns {
		colName := columnNode.Column.String()
		if _, found := columnRefs[colName]; found {
			panic(pgerror.Newf(pgcode.InvalidObjectDefinition,
				"index %q contains duplicate column %q", n.Name, colName))
		}
		columnRefs[colName] = struct{}{}
	}
	for _, storingNode := range n.Storing {
		colName := storingNode.String()
		if _, found := columnRefs[colName]; found {
			panic(pgerror.Newf(pgcode.InvalidObjectDefinition,
				"index %q contains duplicate column %q", n.Name, colName))
		}
		columnRefs[colName] = struct{}{}
	}
	// Set key column IDs and directions.
	keyColNames := make([]string, len(n.Columns))
	var newIndexColumns []*scpb.IndexColumn
	var keyColIDs catalog.TableColSet
	indexID := nextRelationIndexID(b, relation)
	for i, columnNode := range n.Columns {
		colName := columnNode.Column.String()
		if columnNode.Expr != nil {
			tbl, ok := relation.(*scpb.Table)
			if !ok {
				panic(scerrors.NotImplementedErrorf(n,
					"indexing virtual column expressions in materialized views is not supported"))
			}
			colName = createVirtualColumnForIndex(b, &n.Table, tbl, columnNode.Expr)
			relationElements = b.QueryByID(index.TableID)
		}
		var columnID catid.ColumnID
		scpb.ForEachColumnName(relationElements, func(_ scpb.Status, target scpb.TargetStatus, e *scpb.ColumnName) {
			if target == scpb.ToPublic && e.Name == colName {
				columnID = e.ColumnID
			}
		})
		if columnID == 0 {
			panic(colinfo.NewUndefinedColumnError(colName))
		}
		keyColNames[i] = colName
		direction := catpb.IndexColumn_ASC
		if columnNode.Direction == tree.Descending {
			direction = catpb.IndexColumn_DESC
		}
		ic := &scpb.IndexColumn{
			TableID:       index.TableID,
			IndexID:       indexID,
			ColumnID:      columnID,
			OrdinalInKind: uint32(i),
			Kind:          scpb.IndexColumn_KEY,
			Direction:     direction,
		}
		newIndexColumns = append(newIndexColumns, ic)
		keyColIDs.Add(columnID)
	}
	// Set the key suffix column IDs.
	// We want to find the key column IDs
	var keySuffixColumns []*scpb.IndexColumn
	scpb.ForEachIndexColumn(relationElements, func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.IndexColumn,
	) {
		if e.IndexID != source.IndexID || keyColIDs.Contains(e.ColumnID) ||
			e.Kind != scpb.IndexColumn_KEY {
			return
		}
		keySuffixColumns = append(keySuffixColumns, e)
	})
	sort.Slice(keySuffixColumns, func(i, j int) bool {
		return keySuffixColumns[i].OrdinalInKind < keySuffixColumns[j].OrdinalInKind
	})
	for i, c := range keySuffixColumns {
		ic := &scpb.IndexColumn{
			TableID:       index.TableID,
			IndexID:       indexID,
			ColumnID:      c.ColumnID,
			OrdinalInKind: uint32(i),
			Kind:          scpb.IndexColumn_KEY_SUFFIX,
			Direction:     c.Direction,
		}
		newIndexColumns = append(newIndexColumns, ic)
	}

	// Set the storing column IDs.
	for i, storingNode := range n.Storing {
		var columnID catid.ColumnID
		scpb.ForEachColumnName(relationElements, func(_ scpb.Status, target scpb.TargetStatus, e *scpb.ColumnName) {
			if target == scpb.ToPublic && tree.Name(e.Name) == storingNode {
				columnID = e.ColumnID
			}
		})
		if columnID == 0 {
			panic(colinfo.NewUndefinedColumnError(storingNode.String()))
		}
		c := &scpb.IndexColumn{
			TableID:       index.TableID,
			IndexID:       indexID,
			ColumnID:      columnID,
			OrdinalInKind: uint32(i),
			Kind:          scpb.IndexColumn_STORED,
		}
		newIndexColumns = append(newIndexColumns, c)
	}
	// Set up sharding.
	if n.Sharded != nil {
		buckets, err := tabledesc.EvalShardBucketCount(b, b.SemaCtx(), b.EvalCtx(), n.Sharded.ShardBuckets, n.StorageParams)
		if err != nil {
			panic(err)
		}
		shardColName := maybeCreateAndAddShardCol(b, int(buckets), relation.(*scpb.Table), keyColNames, n)
		index.Sharding = &catpb.ShardedDescriptor{
			IsSharded:    true,
			Name:         shardColName,
			ShardBuckets: buckets,
			ColumnNames:  keyColNames,
		}
	}
	// Assign the ID here, since we may have added columns
	// and made a new primary key above.
	index.SourceIndexID = source.IndexID
	index.IndexID = nextRelationIndexID(b, relation)
	for _, ic := range newIndexColumns {
		ic.IndexID = index.IndexID
		b.Add(ic)
	}
	tempIndexID := index.IndexID + 1 // this is enforced below
	index.TemporaryIndexID = tempIndexID
	sec := &scpb.SecondaryIndex{Index: index}
	b.Add(sec)
	b.Add(&scpb.IndexName{
		TableID: index.TableID,
		IndexID: index.IndexID,
		Name:    string(n.Name),
	})
	if n.PartitionByIndex.ContainsPartitions() {
		b.Add(&scpb.IndexPartitioning{
			TableID: index.TableID,
			IndexID: index.IndexID,
			PartitioningDescriptor: b.IndexPartitioningDescriptor(
				&sec.Index, n.PartitionByIndex.PartitionBy,
			),
		})
	}

	temp := &scpb.TemporaryIndex{
		Index:                    protoutil.Clone(sec).(*scpb.SecondaryIndex).Index,
		IsUsingSecondaryEncoding: true,
	}
	temp.TemporaryIndexID = 0
	temp.IndexID = nextRelationIndexID(b, relation)
	if temp.IndexID != tempIndexID {
		panic(errors.AssertionFailedf(
			"assumed temporary index ID %d != %d", tempIndexID, temp.IndexID,
		))
	}
	b.AddTransient(temp)
	for _, ic := range newIndexColumns {
		tic := protoutil.Clone(ic).(*scpb.IndexColumn)
		tic.IndexID = tempIndexID
		b.Add(tic)
	}
	if n.PartitionByIndex.ContainsPartitions() {
		b.Add(&scpb.IndexPartitioning{
			TableID: temp.TableID,
			IndexID: temp.IndexID,
			PartitioningDescriptor: b.IndexPartitioningDescriptor(
				&temp.Index, n.PartitionByIndex.PartitionBy,
			),
		})
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
			TableID:     tbl.TableID,
			ColumnID:    shardColID,
			TypeT:       scpb.TypeT{Type: types.Int4},
			ComputeExpr: b.WrapExpression(tbl.TableID, parsedExpr),
			IsVirtual:   true,
		},
	}
	addColumn(b, spec, n)
	return shardColName
}

func createVirtualColumnForIndex(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, expr tree.Expr,
) string {
	elts := b.QueryByID(tbl.TableID)
	colName := tabledesc.GenerateUniqueName("crdb_internal_idx_expr", func(name string) (found bool) {
		scpb.ForEachColumnName(elts, func(_ scpb.Status, target scpb.TargetStatus, cn *scpb.ColumnName) {
			if target == scpb.ToPublic && cn.Name == name {
				found = true
			}
		})
		return found
	})
	// TODO(postamar): call addColumn instead of building AST.
	d := &tree.ColumnTableDef{
		Name:   tree.Name(colName),
		Hidden: true,
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
	}
	alterTableAddColumn(b, tn, tbl, &tree.AlterTableAddColumn{ColumnDef: d})
	return colName
}
