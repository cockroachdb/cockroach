// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuild

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// CreatePartitioningCCL is the public hook point for the CCL-licensed
// partitioning creation code.
var CreatePartitioningCCL = func(
	ctx context.Context,
	st *cluster.Settings,
	evalCtx *tree.EvalContext,
	tableDesc *tabledesc.Mutable,
	indexDesc descpb.IndexDescriptor,
	partBy *tree.PartitionBy,
	allowedNewColumnNames []tree.Name,
	allowImplicitPartitioning bool,
) (newImplicitCols []catalog.Column, newPartitioning descpb.PartitioningDescriptor, err error) {
	return nil, descpb.PartitioningDescriptor{}, sqlerrors.NewCCLRequiredError(errors.New(
		"creating or manipulating partitions requires a CCL binary"))
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
	col.ComputeExpr = MakeHashShardComputeExpr(colNames, buckets)
	return col, nil
}

// MakeHashShardComputeExpr creates the serialized computed expression for a hash shard
// column based on the column names and the number of buckets. The expression will be
// of the form:
//
//    mod(fnv32(colNames[0]::STRING)+fnv32(colNames[1])+...,buckets)
//
func MakeHashShardComputeExpr(colNames []string, buckets int) *string {
	unresolvedFunc := func(funcName string) tree.ResolvableFunctionReference {
		return tree.ResolvableFunctionReference{
			FunctionReference: &tree.UnresolvedName{
				NumParts: 1,
				Parts:    tree.NameParts{funcName},
			},
		}
	}
	hashedColumnExpr := func(colName string) tree.Expr {
		return &tree.FuncExpr{
			Func: unresolvedFunc("fnv32"),
			Exprs: tree.Exprs{
				// NB: We have created the hash shard column as NOT NULL so we need
				// to coalesce NULLs into something else. There's a variety of different
				// reasonable choices here. We could pick some outlandish value, we
				// could pick a zero value for each type, or we can do the simple thing
				// we do here, however the empty string seems pretty reasonable. At worst
				// we'll have a collision for every combination of NULLable string
				// columns. That seems just fine.
				&tree.CoalesceExpr{
					Name: "COALESCE",
					Exprs: tree.Exprs{
						&tree.CastExpr{
							Type: types.String,
							Expr: &tree.ColumnItem{ColumnName: tree.Name(colName)},
						},
						tree.NewDString(""),
					},
				},
			},
		}
	}

	// Construct an expression which is the sum of all of the casted and hashed
	// columns.
	var expr tree.Expr
	for i := len(colNames) - 1; i >= 0; i-- {
		c := colNames[i]
		if expr == nil {
			expr = hashedColumnExpr(c)
		} else {
			expr = &tree.BinaryExpr{
				Left:     hashedColumnExpr(c),
				Operator: tree.MakeBinaryOperator(tree.Plus),
				Right:    expr,
			}
		}
	}
	str := tree.Serialize(&tree.FuncExpr{
		Func: unresolvedFunc("mod"),
		Exprs: tree.Exprs{
			expr,
			tree.NewDInt(tree.DInt(buckets)),
		},
	})
	return &str
}

// maybeCreateAndAddShardCol adds a new hidden computed shard column (or its mutation) to
// `desc`, if one doesn't already exist for the given index column set and number of shard
// buckets.
func (b *buildContext) maybeCreateAndAddShardCol(
	shardBuckets int, desc catalog.TableDescriptor, colNames []string, isNewTable bool,
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
			shardColDesc.ID = b.nextColumnID(desc)
			column := &scpb.Column{
				Column:     *shardColDesc,
				TableID:    desc.GetID(),
				FamilyID:   desc.GetFamilies()[0].ID,
				FamilyName: desc.GetFamilies()[0].Name,
			}
			b.addNode(scpb.Target_ADD, column)
		}
		if !shardColDesc.Virtual {
			// Replace the primary index
			oldPrimaryIndex := primaryIndexElemFromDescriptor(desc.GetPrimaryIndex().IndexDesc(), desc)
			newPrimaryIndex := primaryIndexElemFromDescriptor(desc.GetPrimaryIndex().IndexDesc(), desc)
			newPrimaryIndex.IndexId = b.nextIndexID(desc)
			newPrimaryIndex.IndexName = tabledesc.GenerateUniqueName(
				"new_primary_key",
				func(name string) bool {
					// TODO (lucy): Also check the new indexes specified in the targets.
					_, err := desc.FindIndexWithName(name)
					return err == nil
				},
			)
			newPrimaryIndex.StoringColumnIDs = append(newPrimaryIndex.StoringColumnIDs, shardColDesc.ID)
			b.addNode(scpb.Target_DROP, oldPrimaryIndex)
			b.addNode(scpb.Target_ADD, newPrimaryIndex)
		}
		created = true
	}
	return created, nil
}

func (b *buildContext) createIndex(ctx context.Context, n *tree.CreateIndex) {
	// Look up the table first.
	_, table, err := resolver.ResolveExistingTableObject(ctx, b.Res, &n.Table,
		tree.ObjectLookupFlags{
			CommonLookupFlags: tree.CommonLookupFlags{
				Required:       true,
				RequireMutable: true,
			},
		})
	if err != nil {
		panic(err)
	}
	// Detect if the index already exists.
	foundIndex, err := table.FindIndexWithName(n.Name.String())
	if err == nil {
		if foundIndex.Dropped() {
			panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"index %q being dropped, try again later", n.Name.String()))
		}
		if n.IfNotExists {
			return
		}
		panic(sqlerrors.NewRelationAlreadyExistsError(n.Name.String()))
	}

	if table.IsView() && !table.MaterializedView() {
		panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not a table or materialized view", &n.Table))
	}

	if table.MaterializedView() {
		if n.Interleave != nil {
			panic(pgerror.New(pgcode.InvalidObjectDefinition,
				"cannot create interleaved index on materialized view"))
		}
		if n.Sharded != nil {
			panic(pgerror.New(pgcode.InvalidObjectDefinition,
				"cannot create hash sharded index on materialized view"))
		}
	}

	if n.PartitionByIndex != nil && table.GetLocalityConfig() != nil {
		panic(pgerror.New(
			pgcode.FeatureNotSupported,
			"cannot define PARTITION BY on a new INDEX in a multi-region database",
		))
	}

	// Setup an secondary index node.
	secondaryIndex := &scpb.SecondaryIndex{TableID: table.GetID(),
		IndexName:          n.Name.Normalize(),
		Unique:             n.Unique,
		KeyColumnIDs:       make([]descpb.ColumnID, 0, len(n.Columns)),
		StoringColumnIDs:   make([]descpb.ColumnID, 0, len(n.Storing)),
		Inverted:           n.Inverted,
		Concurrently:       n.Concurrently,
		KeySuffixColumnIDs: nil,
		ShardedDescriptor:  nil,
	}
	colNames := make([]string, 0, len(n.Columns))
	// Setup the column ID.
	for _, columnNode := range n.Columns {
		// If the column was just added the new schema changer is not supported.
		if b.checkIfNewColumnExistsByName(table.GetID(), columnNode.Column) {
			panic(&notImplementedError{
				detail: "column was added in the current transaction.",
				n:      n,
			})
		}
		if columnNode.Expr != nil {
			if !b.EvalCtx.SessionData.EnableExpressionIndexes {
				panic(unimplemented.NewWithIssuef(9682, "only simple columns are supported as index elements"))
			}
			_, typ, _, err := schemaexpr.DequalifyAndValidateExpr(
				ctx,
				table,
				columnNode.Expr,
				types.Any,
				"index expression",
				b.SemaCtx,
				tree.VolatilityImmutable,
				&n.Table,
			)
			if err != nil {
				panic(err)
			}
			// Create a new virtual column and add it to the table
			// descriptor.
			colName := tabledesc.GenerateUniqueName("crdb_idx_expr", func(name string) bool {
				_, err := table.FindColumnWithName(tree.Name(name))
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
			b.alterTableAddColumn(ctx, table, addCol, &n.Table)
			var addColumn *scpb.Column = nil
			for _, node := range b.output {
				if node.Target.Column != nil &&
					node.Target.Column.TableID == table.GetID() &&
					node.Target.Column.Column.Name == colName {
					addColumn = node.Target.Column
				}
			}

			// Set up the index based on the new column
			colNames = append(colNames, colName)
			secondaryIndex.KeyColumnIDs = append(secondaryIndex.KeyColumnIDs, addColumn.Column.ID)
			continue

		}
		if columnNode.Expr == nil {
			column, err := table.FindColumnWithName(columnNode.Column)
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
	// Setup the storing columns
	for _, storingNode := range n.Storing {
		column, err := table.FindColumnWithName(storingNode)
		if err != nil {
			panic(err)
		}
		secondaryIndex.StoringColumnIDs = append(secondaryIndex.StoringColumnIDs, column.GetID())
	}
	if n.Sharded != nil {
		if n.PartitionByIndex.ContainsPartitions() {
			panic(pgerror.New(pgcode.FeatureNotSupported, "sharded indexes don't support partitioning"))
		}
		if table.IsLocalityRegionalByRow() {
			panic(pgerror.New(pgcode.FeatureNotSupported, "hash sharded indexes are not compatible with REGIONAL BY ROW tables"))
		}
		if n.Interleave != nil {
			panic(pgerror.New(pgcode.FeatureNotSupported, "interleaved indexes cannot also be hash sharded"))
		}
		buckets, err := tabledesc.EvalShardBucketCount(ctx, b.SemaCtx, b.EvalCtx, n.Sharded.ShardBuckets)
		if err != nil {
			panic(err)
		}
		shardColName := tabledesc.GetShardColumnName(colNames, buckets)
		_, err = b.maybeCreateAndAddShardCol(int(buckets), table, colNames, false)
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
	secondaryIndex.IndexId = b.nextIndexID(table)
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
			TableID:         table.GetID(),
			IndexId:         secondaryIndex.IndexId,
			Fields:          fields,
			ListPartitions:  listPartitions,
			RangePartitions: rangePartitions,
		}
		b.addNode(scpb.Target_ADD,
			partitioning)
	}

	// KeySuffixColumnIDs is only populated for indexes using the secondary
	// index encoding. It is the set difference of the primary key minus the
	// index's key.
	colIDs := catalog.MakeTableColSet(secondaryIndex.KeyColumnIDs...)
	var extraColumnIDs []descpb.ColumnID
	for _, primaryColID := range table.GetPrimaryIndex().IndexDesc().KeyColumnIDs {
		if !colIDs.Contains(primaryColID) {
			extraColumnIDs = append(extraColumnIDs, primaryColID)
			colIDs.Add(primaryColID)
		}
	}
	secondaryIndex.KeySuffixColumnIDs = extraColumnIDs

	b.addNode(scpb.Target_ADD,
		secondaryIndex)
}
