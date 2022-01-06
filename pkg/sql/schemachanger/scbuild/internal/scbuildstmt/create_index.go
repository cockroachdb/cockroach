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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// CreateIndex implements CREATE INDEX.
func CreateIndex(b BuildCtx, n *tree.CreateIndex) {
	_, rel, idx := b.ResolveIndex(n.Table.ToUnresolvedObjectName(), n.Name, ResolveParams{
		IsExistenceOptional: true,
		RequiredPrivilege:   privilege.CREATE,
	})
	if rel == nil {
		// Table must exist.
		panic(sqlerrors.NewUndefinedRelationError(n.Table.ToUnresolvedObjectName()))
	}
	if idx != nil {
		if n.IfNotExists {
			return
		}
		// Index must not exist.
		if idx.Dropped() {
			panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"index %q being dropped, try again later", n.Name.String()))
		}
		panic(pgerror.Newf(pgcode.DuplicateRelation, "index with name %q already exists", n.Name))
	}

	if rel.MaterializedView() {
		if n.Sharded != nil {
			panic(pgerror.New(pgcode.InvalidObjectDefinition,
				"cannot create hash sharded index on materialized view"))
		}
	}

	if n.PartitionByIndex != nil && rel.GetLocalityConfig() != nil {
		panic(pgerror.New(
			pgcode.FeatureNotSupported,
			"cannot define PARTITION BY on a new INDEX in a multi-region database",
		))
	}
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

	// Setup a secondary index node.
	secondaryIndex := &scpb.SecondaryIndex{
		TableID:            rel.GetID(),
		Unique:             n.Unique,
		KeyColumnIDs:       make([]descpb.ColumnID, 0, len(n.Columns)),
		StoringColumnIDs:   make([]descpb.ColumnID, 0, len(n.Storing)),
		Inverted:           n.Inverted,
		Concurrently:       n.Concurrently,
		KeySuffixColumnIDs: nil,
		ShardedDescriptor:  nil,

		// TODO(ajwerner): If there exists a new primary index due to a column
		// set change in this transaction, we may need this to refer to the
		// new primary index as opposed to the old primary index.
		SourceIndexID: rel.GetPrimaryIndexID(),
	}
	colNames := make([]string, 0, len(n.Columns))
	// Setup the column ID.
	for _, columnNode := range n.Columns {
		// If the column was just added the new schema changer is not supported.
		if b.HasNode(func(status, _ scpb.Status, elem scpb.Element) bool {
			if status != scpb.Status_ABSENT {
				return false
			}
			if col, ok := elem.(*scpb.ColumnName); ok {
				return col.TableID == rel.GetID() && col.Name == columnNode.Column.String()
			}
			return false
		}) {
			panic(scerrors.NotImplementedErrorf(n, "column was added in the current transaction"))
		}
		if columnNode.Expr != nil {
			// TODO(fqazi): We need to deal with columns added in the same
			// transaction here as well.
			_, typ, _, err := schemaexpr.DequalifyAndValidateExpr(
				b,
				rel,
				columnNode.Expr,
				types.Any,
				"index expression",
				b.SemaCtx(),
				tree.VolatilityImmutable,
				&n.Table,
			)
			if err != nil {
				panic(err)
			}
			// Create a new virtual column and add it to the rel
			// descriptor.
			colName := tabledesc.GenerateUniqueName("crdb_internal_idx_expr", func(name string) bool {
				_, err := rel.FindColumnWithName(tree.Name(name))
				return err == nil
			})
			addCol := &tree.AlterTableAddColumn{
				ColumnDef: &tree.ColumnTableDef{
					Name:   tree.Name(colName),
					Type:   typ,
					Hidden: true,
				},
			}
			addCol.ColumnDef.Computed.Computed = true
			addCol.ColumnDef.Computed.Expr = columnNode.Expr
			addCol.ColumnDef.Computed.Virtual = true
			addCol.ColumnDef.Nullable.Nullability = tree.Null

			// Add a new column element
			alterTableAddColumn(b, rel, addCol, &n.Table)
			var addColumn *scpb.ColumnName
			scpb.ForEachColumnName(b, func(_, targetStatus scpb.Status, col *scpb.ColumnName) {
				if targetStatus == scpb.Status_PUBLIC {
					if col.TableID == rel.GetID() && col.Name == colName {
						addColumn = col
					}
				}
			})

			// Set up the index based on the new column.
			colNames = append(colNames, colName)
			secondaryIndex.KeyColumnIDs = append(secondaryIndex.KeyColumnIDs, addColumn.ColumnID)
		}
		if columnNode.Expr == nil {
			column, err := rel.FindColumnWithName(columnNode.Column)
			if err != nil {
				panic(err)
			}
			colNames = append(colNames, column.GetName())
			secondaryIndex.KeyColumnIDs = append(secondaryIndex.KeyColumnIDs, column.GetID())
		}
		// Convert the key column directions.
		switch columnNode.Direction {
		case tree.Ascending, tree.DefaultDirection:
			secondaryIndex.KeyColumnDirections = append(secondaryIndex.KeyColumnDirections, scpb.SecondaryIndex_ASC)
		case tree.Descending:
			secondaryIndex.KeyColumnDirections = append(secondaryIndex.KeyColumnDirections, scpb.SecondaryIndex_DESC)
		default:
			panic(errors.AssertionFailedf("Unknown direction type %s", columnNode.Direction))
		}
	}
	// Setup the storing columns.
	for _, storingNode := range n.Storing {
		column, err := rel.FindColumnWithName(storingNode)
		if err != nil {
			panic(err)
		}
		secondaryIndex.StoringColumnIDs = append(secondaryIndex.StoringColumnIDs, column.GetID())
	}
	if n.Sharded != nil {
		if n.PartitionByIndex.ContainsPartitions() {
			panic(pgerror.New(pgcode.FeatureNotSupported, "sharded indexes don't support partitioning"))
		}
		if rel.IsLocalityRegionalByRow() {
			panic(pgerror.New(pgcode.FeatureNotSupported, "hash sharded indexes are not compatible with REGIONAL BY ROW tables"))
		}
		buckets, err := tabledesc.EvalShardBucketCount(b, b.SemaCtx(), b.EvalCtx(), n.Sharded.ShardBuckets)
		if err != nil {
			panic(err)
		}
		shardColName := tabledesc.GetShardColumnName(colNames, buckets)
		_, err = maybeCreateAndAddShardCol(b, int(buckets), rel, colNames, false)
		if err != nil {
			panic(err)
		}
		secondaryIndex.ShardedDescriptor = &descpb.ShardedDescriptor{
			IsSharded:    true,
			Name:         shardColName,
			ShardBuckets: buckets,
			ColumnNames:  colNames,
		}
	}
	// Assign the ID here, since we may have added columns
	// and made a new primary key above.
	secondaryIndex.IndexID = b.NextIndexID(rel)
	secondaryIndexName := &scpb.IndexName{
		TableID: secondaryIndex.TableID,
		IndexID: secondaryIndex.IndexID,
		Name:    string(n.Name),
	}
	// Convert partitioning information for the execution
	// side of things.
	if n.PartitionByIndex.ContainsPartitions() {
		listPartitions := make([]*scpb.ListPartition, 0, len(n.PartitionByIndex.List))
		for _, partition := range n.PartitionByIndex.List {
			exprs := make([]string, 0, len(partition.Exprs))
			for _, expr := range partition.Exprs {
				exprs = append(exprs, expr.String())
			}
			listPartition := &scpb.ListPartition{
				Name: partition.Name.String(),
				Expr: exprs,
			}
			listPartitions = append(listPartitions, listPartition)
		}
		rangePartitions := make([]*scpb.RangePartitions, 0, len(n.PartitionByIndex.Range))
		for _, partition := range n.PartitionByIndex.Range {
			toExpr := make([]string, 0, len(partition.To))
			for _, expr := range partition.To {
				fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
				fmtCtx.FormatNode(expr)
				toExpr = append(toExpr, fmtCtx.String())
			}
			fromExpr := make([]string, 0, len(partition.From))
			for _, expr := range partition.From {
				fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
				fmtCtx.FormatNode(expr)
				fromExpr = append(fromExpr, fmtCtx.String())
			}
			rangePartition := &scpb.RangePartitions{
				Name: partition.Name.String(),
				To:   toExpr,
				From: fromExpr,
			}
			rangePartitions = append(rangePartitions, rangePartition)
		}
		fields := make([]string, 0, len(n.PartitionByIndex.Fields))
		for _, field := range n.PartitionByIndex.Fields {
			fields = append(fields, field.String())
		}
		partitioning := &scpb.Partitioning{
			TableID:         rel.GetID(),
			IndexID:         secondaryIndex.IndexID,
			Fields:          fields,
			ListPartitions:  listPartitions,
			RangePartitions: rangePartitions,
		}
		b.EnqueueAdd(partitioning)
	}

	// KeySuffixColumnIDs is only populated for indexes using the secondary
	// index encoding. It is the set difference of the primary key minus the
	// index's key.
	colIDs := catalog.MakeTableColSet(secondaryIndex.KeyColumnIDs...)
	for i := 0; i < rel.GetPrimaryIndex().NumKeyColumns(); i++ {
		primaryColID := rel.GetPrimaryIndex().GetKeyColumnID(i)
		if !colIDs.Contains(primaryColID) {
			secondaryIndex.KeySuffixColumnIDs = append(secondaryIndex.KeySuffixColumnIDs, primaryColID)
			colIDs.Add(primaryColID)
		}
	}
	b.EnqueueAdd(secondaryIndex)
	b.EnqueueAdd(secondaryIndexName)
}

// maybeCreateAndAddShardCol adds a new hidden computed shard column (or its mutation) to
// `desc`, if one doesn't already exist for the given index column set and number of shard
// buckets.
func maybeCreateAndAddShardCol(
	b BuildCtx, shardBuckets int, desc catalog.TableDescriptor, colNames []string, isNewTable bool,
) (created bool, err error) {
	shardColDesc, err := makeShardColumnDesc(colNames, shardBuckets)
	if err != nil {
		return false, err
	}
	existingShardCol, err := desc.FindColumnWithName(tree.Name(shardColDesc.Name))
	if err == nil && !existingShardCol.Dropped() {
		// TODO(ajwerner): In what ways is existingShardCol allowed to differ from
		// the newly made shardCol? Should there be some validation of
		// existingShardCol?
		if !existingShardCol.IsHidden() {
			// The user managed to reverse-engineer our crazy shard column name, so
			// we'll return an error here rather than try to be tricky.
			return false, pgerror.Newf(pgcode.DuplicateColumn,
				"column %s already specified; can't be used for sharding", shardColDesc.Name)
		}
		return false, nil
	}
	columnIsUndefined := sqlerrors.IsUndefinedColumnError(err)
	if err != nil && !columnIsUndefined {
		return false, err
	}
	if columnIsUndefined || existingShardCol.Dropped() {
		if isNewTable {
			panic(false)
			//desc.AddColumn(shardColDesc)
		} else {
			shardColDesc.ID = b.NextColumnID(desc)
			column := columnDescToElement(desc,
				*shardColDesc,
				&desc.GetFamilies()[0].Name,
				&desc.GetFamilies()[0].ID)
			b.EnqueueAdd(column)
		}
		if !shardColDesc.Virtual {
			// Replace the primary index
			oldPrimaryIndex, oldPrimaryIndexName := primaryIndexElemFromDescriptor(desc.GetPrimaryIndex().IndexDesc(), desc)
			newPrimaryIndex, newPrimaryIndexName := primaryIndexElemFromDescriptor(desc.GetPrimaryIndex().IndexDesc(), desc)
			newPrimaryIndex.IndexID = b.NextIndexID(desc)
			newPrimaryIndexName.IndexID = newPrimaryIndex.IndexID
			newPrimaryIndexName.Name = tabledesc.PrimaryKeyIndexName(desc.GetName())
			newPrimaryIndex.StoringColumnIDs = append(newPrimaryIndex.StoringColumnIDs, shardColDesc.ID)
			b.EnqueueDrop(oldPrimaryIndex)
			b.EnqueueDrop(oldPrimaryIndexName)
			b.EnqueueAdd(newPrimaryIndex)
			b.EnqueueAdd(newPrimaryIndexName)
		}
		created = true
	}
	return created, nil
}

// makeShardColumnDesc returns a new column descriptor for a hidden computed shard column
// based on all the `colNames`.
func makeShardColumnDesc(colNames []string, buckets int) (*descpb.ColumnDescriptor, error) {
	col := &descpb.ColumnDescriptor{
		Hidden:   true,
		Nullable: false,
		Type:     types.Int4,
	}
	col.Name = tabledesc.GetShardColumnName(colNames, int32(buckets))
	col.ComputeExpr = schemaexpr.MakeHashShardComputeExpr(colNames, buckets)
	return col, nil
}
