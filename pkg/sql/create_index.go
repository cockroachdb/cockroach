// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type createIndexNode struct {
	n         *tree.CreateIndex
	tableDesc *tabledesc.Mutable
}

// CreateIndex creates an index.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires INDEX on the table.
func (p *planner) CreateIndex(ctx context.Context, n *tree.CreateIndex) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"CREATE INDEX",
	); err != nil {
		return nil, err
	}
	_, tableDesc, err := p.ResolveMutableTableDescriptor(
		ctx, &n.Table, true /*required*/, tree.ResolveRequireTableOrViewDesc,
	)
	if err != nil {
		return nil, err
	}

	if tableDesc.IsView() && !tableDesc.MaterializedView() {
		return nil, pgerror.Newf(pgcode.WrongObjectType, "%q is not a table or materialized view", tableDesc.Name)
	}

	if tableDesc.MaterializedView() {
		if n.Sharded != nil {
			return nil, pgerror.New(pgcode.InvalidObjectDefinition,
				"cannot create hash sharded index on materialized view")
		}
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	if tableDesc.IsLocalityRegionalByRow() {
		if err := p.checkNoRegionChangeUnderway(
			ctx,
			tableDesc.GetParentID(),
			"CREATE INDEX on a REGIONAL BY ROW table",
		); err != nil {
			return nil, err
		}
	}

	return &createIndexNode{tableDesc: tableDesc, n: n}, nil
}

// maybeSetupConstraintForShard adds a check constraint ensuring that the shard
// column's value is within [0..ShardBuckets-1]. This method is called when a
// `CREATE INDEX`/`ALTER PRIMARY KEY` statement is issued for the creation of a
// sharded index that *does not* re-use a pre-existing shard column.
func (p *planner) maybeSetupConstraintForShard(
	ctx context.Context, tableDesc *tabledesc.Mutable, shardCol catalog.Column, buckets int32,
) error {
	// Assign an ID to the newly-added shard column, which is needed for the creation
	// of a valid check constraint.
	version := p.ExecCfg().Settings.Version.ActiveVersion(ctx)
	if err := tableDesc.AllocateIDs(ctx, version); err != nil {
		return err
	}

	ckDef, err := makeShardCheckConstraintDef(int(buckets), shardCol)
	if err != nil {
		return err
	}
	ckBuilder := schemaexpr.MakeCheckConstraintBuilder(ctx, p.tableName, tableDesc, &p.semaCtx)
	ckDesc, err := ckBuilder.Build(ckDef)
	if err != nil {
		return err
	}

	curConstraintInfos, err := tableDesc.GetConstraintInfo()
	if err != nil {
		return err
	}

	// Avoid creating duplicate check constraints.
	for _, info := range curConstraintInfos {
		if info.CheckConstraint != nil && info.CheckConstraint.Expr == ckDesc.Expr {
			return nil
		}
	}

	ckDesc.Validity = descpb.ConstraintValidity_Validating
	tableDesc.AddCheckMutation(ckDesc, descpb.DescriptorMutation_ADD)
	return nil
}

// makeIndexDescriptor creates an index descriptor from a CreateIndex node and optionally
// adds a hidden computed shard column (along with its check constraint) in case the index
// is hash sharded. Note that `tableDesc` will be modified when this method is called for
// a hash sharded index.
func makeIndexDescriptor(
	params runParams, n tree.CreateIndex, tableDesc *tabledesc.Mutable,
) (*descpb.IndexDescriptor, error) {
	if n.Sharded == nil && n.StorageParams.GetVal(`bucket_count`) != nil {
		return nil, pgerror.New(
			pgcode.InvalidParameterValue,
			`"bucket_count" storage param should only be set with "USING HASH" for hash sharded index`,
		)
	}
	// Since we mutate the columns below, we make copies of them
	// here so that on retry we do not attempt to validate the
	// mutated columns.
	columns := make(tree.IndexElemList, len(n.Columns))
	copy(columns, n.Columns)

	// Ensure that the columns we want to index are accessible before trying to
	// create the index. This must be checked before inaccessible columns are
	// created for expression indexes in replaceExpressionElemsWithVirtualCols.
	if err := validateColumnsAreAccessible(tableDesc, columns); err != nil {
		return nil, err
	}

	tn, err := params.p.getQualifiedTableName(params.ctx, tableDesc)
	if err != nil {
		return nil, err
	}

	// Replace expression index elements with hidden virtual computed columns.
	// The virtual columns are added as mutation columns to tableDesc.
	if err := replaceExpressionElemsWithVirtualCols(
		params.ctx,
		tableDesc,
		tn,
		columns,
		n.Inverted,
		false, /* isNewTable */
		params.p.SemaCtx(),
	); err != nil {
		return nil, err
	}

	// Ensure that the columns we want to index exist before trying to create the
	// index.
	if err := validateIndexColumnsExist(tableDesc, columns); err != nil {
		return nil, err
	}

	// Ensure that the index name does not exist before trying to create the index.
	if idx, _ := tableDesc.FindIndexWithName(string(n.Name)); idx != nil {
		if idx.Dropped() {
			return nil, pgerror.Newf(pgcode.DuplicateRelation, "index with name %q already exists and is being dropped, try again later", n.Name)
		}
		return nil, pgerror.Newf(pgcode.DuplicateRelation, "index with name %q already exists", n.Name)
	}

	indexDesc := descpb.IndexDescriptor{
		Name:              string(n.Name),
		Unique:            n.Unique,
		StoreColumnNames:  n.Storing.ToStrings(),
		CreatedExplicitly: true,
		CreatedAtNanos:    params.EvalContext().GetTxnTimestamp(time.Microsecond).UnixNano(),
	}

	if n.Inverted {
		if n.Sharded != nil {
			return nil, pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes don't support hash sharding")
		}

		if len(indexDesc.StoreColumnNames) > 0 {
			return nil, pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes don't support stored columns")
		}

		if n.Unique {
			return nil, pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes can't be unique")
		}

		indexDesc.Type = descpb.IndexDescriptor_INVERTED
		column, err := tableDesc.FindColumnWithName(columns[len(columns)-1].Column)
		if err != nil {
			return nil, err
		}
		switch column.GetType().Family() {
		case types.GeometryFamily:
			config, err := geoindex.GeometryIndexConfigForSRID(column.GetType().GeoSRIDOrZero())
			if err != nil {
				return nil, err
			}
			indexDesc.GeoConfig = *config
		case types.GeographyFamily:
			indexDesc.GeoConfig = *geoindex.DefaultGeographyIndexConfig()
		}
	}

	if n.Sharded != nil {
		if n.PartitionByIndex.ContainsPartitions() {
			return nil, pgerror.New(pgcode.FeatureNotSupported, "sharded indexes don't support explicit partitioning")
		}

		shardCol, newColumns, err := setupShardedIndex(
			params.ctx,
			params.EvalContext(),
			&params.p.semaCtx,
			columns,
			n.Sharded.ShardBuckets,
			tableDesc,
			&indexDesc,
			n.StorageParams,
			false /* isNewTable */)
		if err != nil {
			return nil, err
		}
		columns = newColumns
		if err := params.p.maybeSetupConstraintForShard(
			params.ctx, tableDesc, shardCol, indexDesc.Sharded.ShardBuckets,
		); err != nil {
			return nil, err
		}
	}

	if n.Predicate != nil {
		expr, err := schemaexpr.ValidatePartialIndexPredicate(
			params.ctx, tableDesc, n.Predicate, &n.Table, params.p.SemaCtx(),
		)
		if err != nil {
			return nil, err
		}
		indexDesc.Predicate = expr
	}

	if err := indexDesc.FillColumns(columns); err != nil {
		return nil, err
	}

	if err := paramparse.SetStorageParameters(
		params.ctx,
		params.p.SemaCtx(),
		params.EvalContext(),
		n.StorageParams,
		&paramparse.IndexStorageParamObserver{IndexDesc: &indexDesc},
	); err != nil {
		return nil, err
	}

	// Increment telemetry once a descriptor has been successfully created.
	if indexDesc.Type == descpb.IndexDescriptor_INVERTED {
		telemetry.Inc(sqltelemetry.InvertedIndexCounter)
		if geoindex.IsGeometryConfig(&indexDesc.GeoConfig) {
			telemetry.Inc(sqltelemetry.GeometryInvertedIndexCounter)
		}
		if geoindex.IsGeographyConfig(&indexDesc.GeoConfig) {
			telemetry.Inc(sqltelemetry.GeographyInvertedIndexCounter)
		}
		if indexDesc.IsPartial() {
			telemetry.Inc(sqltelemetry.PartialInvertedIndexCounter)
		}
		if len(indexDesc.KeyColumnNames) > 1 {
			telemetry.Inc(sqltelemetry.MultiColumnInvertedIndexCounter)
		}
	}
	if indexDesc.IsSharded() {
		telemetry.Inc(sqltelemetry.HashShardedIndexCounter)
	}
	if indexDesc.IsPartial() {
		telemetry.Inc(sqltelemetry.PartialIndexCounter)
	}

	return &indexDesc, nil
}

// validateColumnsAreAccessible validates that the columns for an index are
// accessible. This check must be performed before creating inaccessible columns
// for expression indexes with replaceExpressionElemsWithVirtualCols.
func validateColumnsAreAccessible(desc *tabledesc.Mutable, columns tree.IndexElemList) error {
	for _, column := range columns {
		// Skip expression elements.
		if column.Expr != nil {
			continue
		}
		foundColumn, err := desc.FindColumnWithName(column.Column)
		if err != nil {
			return err
		}
		if foundColumn.IsInaccessible() {
			return pgerror.Newf(
				pgcode.UndefinedColumn,
				"column %q is inaccessible and cannot be referenced",
				foundColumn.GetName(),
			)
		}
	}
	return nil
}

// validateIndexColumnsExists validates that the columns for an index exist
// in the table and are not being dropped prior to attempting to add the index.
func validateIndexColumnsExist(desc *tabledesc.Mutable, columns tree.IndexElemList) error {
	for _, column := range columns {
		if column.Expr != nil {
			return errors.AssertionFailedf("index elem expression should have been replaced with a column")
		}
		foundColumn, err := desc.FindColumnWithName(column.Column)
		if err != nil {
			return err
		}
		if foundColumn.Dropped() {
			return colinfo.NewUndefinedColumnError(string(column.Column))
		}
	}
	return nil
}

// replaceExpressionElemsWithVirtualCols replaces each non-nil expression in
// elems with an inaccessible virtual column with the same expression. If
// isNewTable is true, the column is added directly to desc. Otherwise, the
// virtual column is added to desc as a mutation column.
func replaceExpressionElemsWithVirtualCols(
	ctx context.Context,
	desc *tabledesc.Mutable,
	tn *tree.TableName,
	elems tree.IndexElemList,
	isInverted bool,
	isNewTable bool,
	semaCtx *tree.SemaContext,
) error {
	findExistingExprIndexCol := func(expr string) (colName string, ok bool) {
		for _, col := range desc.AllColumns() {
			if col.IsExpressionIndexColumn() && col.GetComputeExpr() == expr {
				return col.GetName(), true
			}
		}
		return "", false
	}

	lastColumnIdx := len(elems) - 1
	for i := range elems {
		elem := &elems[i]
		if elem.Expr != nil {
			// Create a dummy ColumnTableDef to use for validating the
			// expression. The type is Any because it is unknown until
			// validation is performed.
			colDef := &tree.ColumnTableDef{
				Type: types.Any,
			}
			colDef.Computed.Computed = true
			colDef.Computed.Expr = elem.Expr
			colDef.Computed.Virtual = true

			// Validate the expression and resolve its type.
			expr, typ, err := schemaexpr.ValidateComputedColumnExpression(
				ctx,
				desc,
				colDef,
				tn,
				"index element",
				semaCtx,
			)
			if err != nil {
				return err
			}

			// Use an existing expression index column if one exists, rather
			// than creating a new one.
			if existingColName, ok := findExistingExprIndexCol(expr); ok {
				// Set the column name and unset the expression.
				elem.Column = tree.Name(existingColName)
				elem.Expr = nil
				// Increment expression index telemetry.
				telemetry.Inc(sqltelemetry.ExpressionIndexCounter)
				continue
			}

			// The expression type cannot be ambiguous.
			if typ.IsAmbiguous() {
				return errors.WithHint(
					pgerror.Newf(
						pgcode.InvalidTableDefinition,
						"type of index element %s is ambiguous",
						elem.Expr.String(),
					),
					"consider adding a type cast to the expression",
				)
			}

			if !isInverted && !colinfo.ColumnTypeIsIndexable(typ) {
				if colinfo.ColumnTypeIsInvertedIndexable(typ) {
					return errors.WithHint(
						pgerror.Newf(
							pgcode.InvalidTableDefinition,
							"index element %s of type %s is not indexable in a non-inverted index",
							elem.Expr.String(),
							typ.Name(),
						),
						"you may want to create an inverted index instead. See the documentation for inverted indexes: "+docs.URL("inverted-indexes.html"),
					)
				}
				return pgerror.Newf(
					pgcode.InvalidTableDefinition,
					"index element %s of type %s is not indexable",
					elem.Expr.String(),
					typ.Name(),
				)
			}

			if isInverted {
				if i < lastColumnIdx && !colinfo.ColumnTypeIsIndexable(typ) {
					return errors.WithHint(
						pgerror.Newf(
							pgcode.InvalidTableDefinition,
							"index element %s of type %s is not allowed as a prefix column in an inverted index",
							elem.Expr.String(),
							typ.Name(),
						),
						"see the documentation for more information about inverted indexes: "+docs.URL("inverted-indexes.html"),
					)
				}
				if i == lastColumnIdx && !colinfo.ColumnTypeIsInvertedIndexable(typ) {
					return errors.WithHint(
						pgerror.Newf(
							pgcode.InvalidTableDefinition,
							"index element %s of type %s is not allowed as the last column in an inverted index",
							elem.Expr.String(),
							typ.Name(),
						),
						"see the documentation for more information about inverted indexes: "+docs.URL("inverted-indexes.html"),
					)
				}
			}

			// Create a new virtual column and add it to the table descriptor.
			colName := tabledesc.GenerateUniqueName("crdb_internal_idx_expr", func(name string) bool {
				_, err := desc.FindColumnWithName(tree.Name(name))
				return err == nil
			})
			col := &descpb.ColumnDescriptor{
				Name:         colName,
				Inaccessible: true,
				Type:         typ,
				ComputeExpr:  &expr,
				Virtual:      true,
				Nullable:     true,
			}

			// Add the column to the table descriptor. If the table already
			// exists, add it as a mutation column.
			if isNewTable {
				desc.AddColumn(col)
			} else {
				desc.AddColumnMutation(col, descpb.DescriptorMutation_ADD)
			}

			// Set the column name and unset the expression.
			elem.Column = tree.Name(colName)
			elem.Expr = nil

			// Increment expression index telemetry.
			telemetry.Inc(sqltelemetry.ExpressionIndexCounter)
		}
	}

	return nil
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because CREATE INDEX performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *createIndexNode) ReadingOwnWrites() {}

// setupShardedIndex creates a shard column for the given index descriptor. It
// returns the shard column and the new column list for the index. If the shard
// column is new, either of the following happens:
// (1) the column is added to tableDesc if it's a new table;
// (2) a column mutation is added to tableDesc if the table is not new.
func setupShardedIndex(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	semaCtx *tree.SemaContext,
	columns tree.IndexElemList,
	bucketsExpr tree.Expr,
	tableDesc *tabledesc.Mutable,
	indexDesc *descpb.IndexDescriptor,
	storageParams tree.StorageParams,
	isNewTable bool,
) (shard catalog.Column, newColumns tree.IndexElemList, err error) {
	if !isNewTable && tableDesc.IsPartitionAllBy() {
		partitionAllBy, err := partitionByFromTableDesc(evalCtx.Codec, tableDesc)
		if err != nil {
			return nil, nil, err
		}
		if anyColumnIsPartitioningField(columns, partitionAllBy) {
			return nil, nil, pgerror.New(
				pgcode.FeatureNotSupported,
				`hash sharded indexes cannot include implicit partitioning columns from "PARTITION ALL BY" or "LOCALITY REGIONAL BY ROW"`,
			)
		}
	}

	colNames := make([]string, 0, len(columns))
	for _, c := range columns {
		colNames = append(colNames, string(c.Column))
	}
	buckets, err := tabledesc.EvalShardBucketCount(ctx, semaCtx, evalCtx, bucketsExpr, storageParams)
	if err != nil {
		return nil, nil, err
	}
	shardCol, err := maybeCreateAndAddShardCol(int(buckets), tableDesc,
		colNames, isNewTable)

	if err != nil {
		return nil, nil, err
	}
	shardIdxElem := tree.IndexElem{
		Column:    tree.Name(shardCol.GetName()),
		Direction: tree.Ascending,
	}
	newColumns = append(tree.IndexElemList{shardIdxElem}, columns...)
	indexDesc.Sharded = catpb.ShardedDescriptor{
		IsSharded:    true,
		Name:         shardCol.GetName(),
		ShardBuckets: buckets,
		ColumnNames:  colNames,
	}
	return shardCol, newColumns, nil
}

// maybeCreateAndAddShardCol adds a new hidden computed shard column (or its mutation) to
// `desc`, if one doesn't already exist for the given index column set and number of shard
// buckets.
func maybeCreateAndAddShardCol(
	shardBuckets int, desc *tabledesc.Mutable, colNames []string, isNewTable bool,
) (col catalog.Column, err error) {
	shardColDesc, err := makeShardColumnDesc(colNames, shardBuckets)
	if err != nil {
		return nil, err
	}
	existingShardCol, err := desc.FindColumnWithName(tree.Name(shardColDesc.Name))
	if err == nil && !existingShardCol.Dropped() {
		// TODO(ajwerner): In what ways is existingShardCol allowed to differ from
		// the newly made shardCol? Should there be some validation of
		// existingShardCol?
		if !existingShardCol.IsHidden() {
			// The user managed to reverse-engineer our crazy shard column name, so
			// we'll return an error here rather than try to be tricky.
			return nil, pgerror.Newf(pgcode.DuplicateColumn,
				"column %s already specified; can't be used for sharding", shardColDesc.Name)
		}
		return existingShardCol, nil
	}
	columnIsUndefined := sqlerrors.IsUndefinedColumnError(err)
	if err != nil && !columnIsUndefined {
		return nil, err
	}
	if columnIsUndefined || existingShardCol.Dropped() {
		if isNewTable {
			desc.AddColumn(shardColDesc)
		} else {
			desc.AddColumnMutation(shardColDesc, descpb.DescriptorMutation_ADD)
		}
	}
	shardCol, err := desc.FindColumnWithName(tree.Name(shardColDesc.Name))
	return shardCol, err
}

func (n *createIndexNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeCreateCounter("index"))
	foundIndex, err := n.tableDesc.FindIndexWithName(string(n.n.Name))
	if err == nil {
		if foundIndex.Dropped() {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"index %q being dropped, try again later", string(n.n.Name))
		}
		if n.n.IfNotExists {
			return nil
		}
	}

	if n.n.Concurrently {
		params.p.BufferClientNotice(
			params.ctx,
			pgnotice.Newf("CONCURRENTLY is not required as all indexes are created concurrently"),
		)
	}

	// Warn against creating a non-partitioned index on a partitioned table,
	// which is undesirable in most cases.
	// Avoid the warning if we have PARTITION ALL BY as all indexes will implicitly
	// have relevant partitioning columns prepended at the front.
	if n.n.PartitionByIndex == nil &&
		n.tableDesc.GetPrimaryIndex().GetPartitioning().NumColumns() > 0 &&
		!n.tableDesc.IsPartitionAllBy() {
		params.p.BufferClientNotice(
			params.ctx,
			errors.WithHint(
				pgnotice.Newf("creating non-partitioned index on partitioned table may not be performant"),
				"Consider modifying the index such that it is also partitioned.",
			),
		)
	}

	indexDesc, err := makeIndexDescriptor(params, *n.n, n.tableDesc)
	if err != nil {
		return err
	}

	// Increment the counter if this index could be storing data across multiple column families.
	if len(indexDesc.StoreColumnNames) > 1 && len(n.tableDesc.Families) > 1 {
		telemetry.Inc(sqltelemetry.SecondaryIndexColumnFamiliesCounter)
	}

	indexDesc.Version = descpb.PrimaryIndexWithStoredColumnsVersion

	if n.n.PartitionByIndex != nil && n.tableDesc.GetLocalityConfig() != nil {
		return pgerror.New(
			pgcode.FeatureNotSupported,
			"cannot define PARTITION BY on a new INDEX in a multi-region database",
		)
	}

	*indexDesc, err = params.p.configureIndexDescForNewIndexPartitioning(
		params.ctx,
		n.tableDesc,
		*indexDesc,
		n.n.PartitionByIndex,
	)
	if err != nil {
		return err
	}

	if indexDesc.Type == descpb.IndexDescriptor_INVERTED && indexDesc.Partitioning.NumColumns != 0 {
		telemetry.Inc(sqltelemetry.PartitionedInvertedIndexCounter)
	}

	mutationIdx := len(n.tableDesc.Mutations)
	if err := n.tableDesc.AddIndexMutation(params.ctx, indexDesc, descpb.DescriptorMutation_ADD, params.p.ExecCfg().Settings); err != nil {
		return err
	}
	version := params.ExecCfg().Settings.Version.ActiveVersion(params.ctx)
	if err := n.tableDesc.AllocateIDs(params.ctx, version); err != nil {
		return err
	}

	if err := params.p.configureZoneConfigForNewIndexPartitioning(
		params.ctx,
		n.tableDesc,
		*indexDesc,
	); err != nil {
		return err
	}

	// The index name may have changed as a result of
	// AllocateIDs(). Retrieve it for the event log below.
	index := n.tableDesc.Mutations[mutationIdx].GetIndex()
	indexName := index.Name

	mutationID := n.tableDesc.ClusterVersion.NextMutationID
	if err := params.p.writeSchemaChange(
		params.ctx, n.tableDesc, mutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
	); err != nil {
		return err
	}

	// Add all newly created type back references.
	if err := params.p.addBackRefsFromAllTypesInTable(params.ctx, n.tableDesc); err != nil {
		return err
	}

	// Record index creation in the event log. This is an auditable log
	// event and is recorded in the same transaction as the table descriptor
	// update.
	return params.p.logEvent(params.ctx,
		n.tableDesc.ID,
		&eventpb.CreateIndex{
			TableName:  n.n.Table.FQString(),
			IndexName:  indexName,
			MutationID: uint32(mutationID),
		})
}

func (*createIndexNode) Next(runParams) (bool, error) { return false, nil }
func (*createIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (*createIndexNode) Close(context.Context)        {}

// configureIndexDescForNewIndexPartitioning returns a new copy of an index descriptor
// containing modifications needed if partitioning is configured.
func (p *planner) configureIndexDescForNewIndexPartitioning(
	ctx context.Context,
	tableDesc *tabledesc.Mutable,
	indexDesc descpb.IndexDescriptor,
	partitionByIndex *tree.PartitionByIndex,
) (descpb.IndexDescriptor, error) {
	var err error
	if partitionByIndex.ContainsPartitioningClause() || tableDesc.IsPartitionAllBy() {
		var partitionBy *tree.PartitionBy
		if !tableDesc.IsPartitionAllBy() {
			if partitionByIndex.ContainsPartitions() {
				partitionBy = partitionByIndex.PartitionBy
			}
		} else if partitionByIndex.ContainsPartitioningClause() {
			return indexDesc, pgerror.New(
				pgcode.FeatureNotSupported,
				"cannot define PARTITION BY on an index if the table has a PARTITION ALL BY definition",
			)
		} else {
			partitionBy, err = partitionByFromTableDesc(p.ExecCfg().Codec, tableDesc)
			if err != nil {
				return indexDesc, err
			}
		}
		allowImplicitPartitioning := p.EvalContext().SessionData().ImplicitColumnPartitioningEnabled ||
			tableDesc.IsLocalityRegionalByRow()
		if partitionBy != nil {
			newImplicitCols, newPartitioning, err := CreatePartitioning(
				ctx,
				p.ExecCfg().Settings,
				p.EvalContext(),
				tableDesc,
				indexDesc,
				partitionBy,
				nil, /* allowedNewColumnNames */
				allowImplicitPartitioning,
			)
			if err != nil {
				return indexDesc, err
			}
			tabledesc.UpdateIndexPartitioning(&indexDesc, false /* isIndexPrimary */, newImplicitCols, newPartitioning)
		}
	}
	return indexDesc, nil
}

// configureZoneConfigForNewIndexPartitioning configures the zone config for any new index
// in a REGIONAL BY ROW table.
// This *must* be done after the index ID has been allocated.
func (p *planner) configureZoneConfigForNewIndexPartitioning(
	ctx context.Context, tableDesc *tabledesc.Mutable, indexDesc descpb.IndexDescriptor,
) error {
	if indexDesc.ID == 0 {
		return errors.AssertionFailedf("index %s does not have id", indexDesc.Name)
	}
	// For REGIONAL BY ROW tables, correctly configure relevant zone configurations.
	if tableDesc.IsLocalityRegionalByRow() {
		regionConfig, err := SynthesizeRegionConfig(ctx, p.txn, tableDesc.GetParentID(), p.Descriptors())
		if err != nil {
			return err
		}

		indexIDs := []descpb.IndexID{indexDesc.ID}
		if idx := catalog.FindCorrespondingTemporaryIndexByID(tableDesc, indexDesc.ID); idx != nil {
			indexIDs = append(indexIDs, idx.GetID())
		}

		if err := ApplyZoneConfigForMultiRegionTable(
			ctx,
			p.txn,
			p.ExecCfg(),
			regionConfig,
			tableDesc,
			applyZoneConfigForMultiRegionTableOptionNewIndexes(indexIDs...),
		); err != nil {
			return err
		}
	}
	return nil
}

func anyColumnIsPartitioningField(columns tree.IndexElemList, partitionBy *tree.PartitionBy) bool {
	for _, field := range partitionBy.Fields {
		for _, column := range columns {
			if field == column.Column {
				return true
			}
		}
	}
	return false
}
