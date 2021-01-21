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

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
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
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
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
	tableDesc, err := p.ResolveMutableTableDescriptor(
		ctx, &n.Table, true /*required*/, tree.ResolveRequireTableOrViewDesc,
	)
	if err != nil {
		return nil, err
	}

	if tableDesc.IsView() && !tableDesc.MaterializedView() {
		return nil, pgerror.Newf(pgcode.WrongObjectType, "%q is not a table or materialized view", tableDesc.Name)
	}

	if tableDesc.MaterializedView() {
		if n.Interleave != nil {
			return nil, pgerror.New(pgcode.InvalidObjectDefinition,
				"cannot create interleaved index on materialized view")
		}
		if n.Sharded != nil {
			return nil, pgerror.New(pgcode.InvalidObjectDefinition,
				"cannot create hash sharded index on materialized view")
		}
	}

	if err := p.CheckPrivilege(ctx, tableDesc, privilege.CREATE); err != nil {
		return nil, err
	}

	return &createIndexNode{tableDesc: tableDesc, n: n}, nil
}

// setupFamilyAndConstraintForShard adds a newly-created shard column into its appropriate
// family (see comment above GetColumnFamilyForShard) and adds a check constraint ensuring
// that the shard column's value is within [0..ShardBuckets-1]. This method is called when
// a `CREATE INDEX` statement is issued for the creation of a sharded index that *does
// not* re-use a pre-existing shard column.
func (p *planner) setupFamilyAndConstraintForShard(
	ctx context.Context,
	tableDesc *tabledesc.Mutable,
	shardCol *descpb.ColumnDescriptor,
	idxColumns []string,
	buckets int32,
) error {
	family := tabledesc.GetColumnFamilyForShard(tableDesc, idxColumns)
	if family == "" {
		return errors.AssertionFailedf("could not find column family for the first column in the index column set")
	}
	// Assign shard column to the family of the first column in its index set, and do it
	// before `AllocateIDs()` assigns it to the primary column family.
	if err := tableDesc.AddColumnToFamilyMaybeCreate(shardCol.Name, family, false, false); err != nil {
		return err
	}
	// Assign an ID to the newly-added shard column, which is needed for the creation
	// of a valid check constraint.
	if err := tableDesc.AllocateIDs(ctx); err != nil {
		return err
	}

	ckDef, err := makeShardCheckConstraintDef(tableDesc, int(buckets), shardCol)
	if err != nil {
		return err
	}
	info, err := tableDesc.GetConstraintInfo(ctx, nil)
	if err != nil {
		return err
	}

	inuseNames := make(map[string]struct{}, len(info))
	for k := range info {
		inuseNames[k] = struct{}{}
	}

	ckBuilder := schemaexpr.MakeCheckConstraintBuilder(ctx, p.tableName, tableDesc, &p.semaCtx)
	ckName, err := ckBuilder.DefaultName(ckDef.Expr)
	if err != nil {
		return err
	}

	// Avoid creating duplicate check constraints.
	if _, ok := inuseNames[ckName]; !ok {
		ck, err := ckBuilder.Build(ckDef)
		if err != nil {
			return err
		}
		ck.Validity = descpb.ConstraintValidity_Validating
		tableDesc.AddCheckMutation(ck, descpb.DescriptorMutation_ADD)
	}
	return nil
}

// MakeIndexDescriptor creates an index descriptor from a CreateIndex node and optionally
// adds a hidden computed shard column (along with its check constraint) in case the index
// is hash sharded. Note that `tableDesc` will be modified when this method is called for
// a hash sharded index.
func MakeIndexDescriptor(
	params runParams, n *tree.CreateIndex, tableDesc *tabledesc.Mutable,
) (*descpb.IndexDescriptor, error) {
	// Ensure that the columns we want to index exist before trying to create the
	// index.
	if err := validateIndexColumnsExist(tableDesc, n.Columns); err != nil {
		return nil, err
	}

	// Ensure that the index name does not exist before trying to create the index.
	if err := tableDesc.ValidateIndexNameIsUnique(string(n.Name)); err != nil {
		return nil, err
	}
	indexDesc := descpb.IndexDescriptor{
		Name:              string(n.Name),
		Unique:            n.Unique,
		StoreColumnNames:  n.Storing.ToStrings(),
		CreatedExplicitly: true,
	}

	if n.Inverted {
		if n.Interleave != nil {
			return nil, pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes don't support interleaved tables")
		}

		if n.PartitionByIndex.ContainsPartitions() {
			return nil, pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes don't support partitioning")
		}

		if n.Sharded != nil {
			return nil, pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes don't support hash sharding")
		}

		if len(indexDesc.StoreColumnNames) > 0 {
			return nil, pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes don't support stored columns")
		}

		if n.Unique {
			return nil, pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes can't be unique")
		}

		if !params.SessionData().EnableMultiColumnInvertedIndexes && len(n.Columns) > 1 {
			return nil, pgerror.New(pgcode.FeatureNotSupported, "indexing more than one column with an inverted index is not supported")
		}
		indexDesc.Type = descpb.IndexDescriptor_INVERTED
		columnDesc, _, err := tableDesc.FindColumnByName(n.Columns[len(n.Columns)-1].Column)
		if err != nil {
			return nil, err
		}
		switch columnDesc.Type.Family() {
		case types.GeometryFamily:
			config, err := geoindex.GeometryIndexConfigForSRID(columnDesc.Type.GeoSRIDOrZero())
			if err != nil {
				return nil, err
			}
			indexDesc.GeoConfig = *config
			telemetry.Inc(sqltelemetry.GeometryInvertedIndexCounter)
		case types.GeographyFamily:
			indexDesc.GeoConfig = *geoindex.DefaultGeographyIndexConfig()
			telemetry.Inc(sqltelemetry.GeographyInvertedIndexCounter)
		}
		telemetry.Inc(sqltelemetry.InvertedIndexCounter)
	}

	if n.Sharded != nil {
		if n.PartitionByIndex.ContainsPartitions() {
			return nil, pgerror.New(pgcode.FeatureNotSupported, "sharded indexes don't support partitioning")
		}
		if n.Interleave != nil {
			return nil, pgerror.New(pgcode.FeatureNotSupported, "interleaved indexes cannot also be hash sharded")
		}
		shardCol, newColumn, err := setupShardedIndex(
			params.ctx,
			params.EvalContext(),
			&params.p.semaCtx,
			params.SessionData().HashShardedIndexesEnabled,
			&n.Columns,
			n.Sharded.ShardBuckets,
			tableDesc,
			&indexDesc,
			false /* isNewTable */)
		if err != nil {
			return nil, err
		}
		if newColumn {
			if err := params.p.setupFamilyAndConstraintForShard(params.ctx, tableDesc, shardCol,
				indexDesc.Sharded.ColumnNames, indexDesc.Sharded.ShardBuckets); err != nil {
				return nil, err
			}
		}
		telemetry.Inc(sqltelemetry.HashShardedIndexCounter)
	}

	if n.Predicate != nil {
		idxValidator := schemaexpr.MakeIndexPredicateValidator(params.ctx, n.Table, tableDesc, &params.p.semaCtx)
		expr, err := idxValidator.Validate(n.Predicate)
		if err != nil {
			return nil, err
		}
		indexDesc.Predicate = expr
		telemetry.Inc(sqltelemetry.PartialIndexCounter)
	}

	if err := indexDesc.FillColumns(n.Columns); err != nil {
		return nil, err
	}

	if err := paramparse.ApplyStorageParameters(
		params.ctx,
		params.p.SemaCtx(),
		params.EvalContext(),
		n.StorageParams,
		&paramparse.IndexStorageParamObserver{IndexDesc: &indexDesc},
	); err != nil {
		return nil, err
	}
	return &indexDesc, nil
}

// validateIndexColumnsExists validates that the columns for an index exist
// in the table and are not being dropped prior to attempting to add the index.
func validateIndexColumnsExist(desc *tabledesc.Mutable, columns tree.IndexElemList) error {
	for _, column := range columns {
		if column.Expr != nil {
			return unimplemented.NewWithIssuef(9682, "only simple columns are supported as index elements")
		}
		_, dropping, err := desc.FindColumnByName(column.Column)
		if err != nil {
			return err
		}
		if dropping {
			return colinfo.NewUndefinedColumnError(string(column.Column))
		}
	}
	return nil
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because CREATE INDEX performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *createIndexNode) ReadingOwnWrites() {}

var hashShardedIndexesDisabledError = pgerror.Newf(pgcode.FeatureNotSupported,
	"hash sharded indexes require the experimental_enable_hash_sharded_indexes session variable")

func setupShardedIndex(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	semaCtx *tree.SemaContext,
	shardedIndexEnabled bool,
	columns *tree.IndexElemList,
	bucketsExpr tree.Expr,
	tableDesc *tabledesc.Mutable,
	indexDesc *descpb.IndexDescriptor,
	isNewTable bool,
) (shard *descpb.ColumnDescriptor, newColumn bool, err error) {
	if !shardedIndexEnabled {
		return nil, false, hashShardedIndexesDisabledError
	}

	colNames := make([]string, 0, len(*columns))
	for _, c := range *columns {
		colNames = append(colNames, string(c.Column))
	}
	buckets, err := tabledesc.EvalShardBucketCount(ctx, semaCtx, evalCtx, bucketsExpr)
	if err != nil {
		return nil, false, err
	}
	shardCol, newColumn, err := maybeCreateAndAddShardCol(int(buckets), tableDesc,
		colNames, isNewTable)
	if err != nil {
		return nil, false, err
	}
	shardIdxElem := tree.IndexElem{
		Column:    tree.Name(shardCol.Name),
		Direction: tree.Ascending,
	}
	*columns = append(tree.IndexElemList{shardIdxElem}, *columns...)
	indexDesc.Sharded = descpb.ShardedDescriptor{
		IsSharded:    true,
		Name:         shardCol.Name,
		ShardBuckets: buckets,
		ColumnNames:  colNames,
	}
	return shardCol, newColumn, nil
}

// maybeCreateAndAddShardCol adds a new hidden computed shard column (or its mutation) to
// `desc`, if one doesn't already exist for the given index column set and number of shard
// buckets.
func maybeCreateAndAddShardCol(
	shardBuckets int, desc *tabledesc.Mutable, colNames []string, isNewTable bool,
) (col *descpb.ColumnDescriptor, created bool, err error) {
	shardCol, err := makeShardColumnDesc(colNames, shardBuckets)
	if err != nil {
		return nil, false, err
	}
	existingShardCol, dropped, err := desc.FindColumnByName(tree.Name(shardCol.Name))
	if err == nil && !dropped {
		// TODO(ajwerner): In what ways is existingShardCol allowed to differ from
		// the newly made shardCol? Should there be some validation of
		// existingShardCol?
		if !existingShardCol.Hidden {
			// The user managed to reverse-engineer our crazy shard column name, so
			// we'll return an error here rather than try to be tricky.
			return nil, false, pgerror.Newf(pgcode.DuplicateColumn,
				"column %s already specified; can't be used for sharding", shardCol.Name)
		}
		return existingShardCol, false, nil
	}
	columnIsUndefined := sqlerrors.IsUndefinedColumnError(err)
	if err != nil && !columnIsUndefined {
		return nil, false, err
	}
	if columnIsUndefined || dropped {
		if isNewTable {
			desc.AddColumn(shardCol)
		} else {
			desc.AddColumnMutation(shardCol, descpb.DescriptorMutation_ADD)
		}
		created = true
	}
	return shardCol, created, nil
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
	if n.n.PartitionByIndex == nil && n.tableDesc.GetPrimaryIndex().GetPartitioning().NumColumns > 0 {
		params.p.BufferClientNotice(
			params.ctx,
			errors.WithHint(
				pgnotice.Newf("creating non-partitioned index on partitioned table may not be performant"),
				"Consider modifying the index such that it is also partitioned.",
			),
		)
	}

	if n.n.Interleave != nil {
		params.p.BufferClientNotice(
			params.ctx,
			errors.WithIssueLink(
				pgnotice.Newf("interleaved tables and indexes are deprecated in 20.2 and will be removed in 21.2"),
				errors.IssueLink{IssueURL: build.MakeIssueURL(52009)},
			),
		)
	}

	indexDesc, err := MakeIndexDescriptor(params, n.n, n.tableDesc)
	if err != nil {
		return err
	}

	// Increment the counter if this index could be storing data across multiple column families.
	if len(indexDesc.StoreColumnNames) > 1 && len(n.tableDesc.Families) > 1 {
		telemetry.Inc(sqltelemetry.SecondaryIndexColumnFamiliesCounter)
	}

	encodingVersion := descpb.SecondaryIndexFamilyFormatVersion
	if params.p.EvalContext().Settings.Version.IsActive(params.ctx, clusterversion.EmptyArraysInInvertedIndexes) {
		encodingVersion = descpb.EmptyArraysInInvertedIndexesVersion
	}
	indexDesc.Version = encodingVersion

	*indexDesc, err = params.p.configureIndexDescForNewIndexPartitioning(
		params.ctx,
		n.tableDesc,
		*indexDesc,
		n.n.PartitionByIndex,
	)
	if err != nil {
		return err
	}

	mutationIdx := len(n.tableDesc.Mutations)
	if err := n.tableDesc.AddIndexMutation(indexDesc, descpb.DescriptorMutation_ADD); err != nil {
		return err
	}
	if err := n.tableDesc.AllocateIDs(params.ctx); err != nil {
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

	if n.n.Interleave != nil {
		if err := params.p.addInterleave(params.ctx, n.tableDesc, index, n.n.Interleave); err != nil {
			return err
		}
		if err := params.p.finalizeInterleave(params.ctx, n.tableDesc, index); err != nil {
			return err
		}
	}

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

		if partitionBy != nil {
			var numImplicitColumns int
			indexDesc, numImplicitColumns, err = detectImplicitPartitionColumns(
				p.EvalContext(),
				tableDesc,
				indexDesc,
				partitionBy,
			)
			if err != nil {
				return indexDesc, err
			}
			if indexDesc.Partitioning, err = CreatePartitioning(
				ctx,
				p.ExecCfg().Settings,
				p.EvalContext(),
				tableDesc,
				&indexDesc,
				numImplicitColumns,
				partitionBy,
			); err != nil {
				return indexDesc, err
			}
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
	// For REGIONAL BY ROW tables, correctly configure relevant zone configurations.
	if tableDesc.LocalityConfig != nil && tableDesc.LocalityConfig.GetRegionalByRow() != nil {
		dbDesc, err := p.Descriptors().GetImmutableDatabaseByID(
			ctx,
			p.txn,
			tableDesc.ParentID,
			tree.DatabaseLookupFlags{},
		)
		if err != nil {
			return err
		}
		if err := p.addNewZoneConfigSubzonesForIndex(
			ctx,
			tableDesc,
			indexDesc.ID,
			*dbDesc.RegionConfig,
		); err != nil {
			return err
		}
	}
	return nil
}
