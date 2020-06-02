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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

type createIndexNode struct {
	n         *tree.CreateIndex
	tableDesc *sqlbase.MutableTableDescriptor
}

// CreateIndex creates an index.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires INDEX on the table.
func (p *planner) CreateIndex(ctx context.Context, n *tree.CreateIndex) (planNode, error) {
	tableDesc, err := p.ResolveMutableTableDescriptor(
		ctx, &n.Table, true /*required*/, resolver.ResolveRequireTableDesc,
	)
	if err != nil {
		return nil, err
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
	tableDesc *MutableTableDescriptor,
	shardCol *sqlbase.ColumnDescriptor,
	idxColumns []string,
	buckets int32,
) error {
	family := sqlbase.GetColumnFamilyForShard(tableDesc, idxColumns)
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
	if err := tableDesc.AllocateIDs(); err != nil {
		return err
	}

	ckDef, err := makeShardCheckConstraintDef(tableDesc, int(buckets), shardCol)
	if err != nil {
		return err
	}
	info, err := tableDesc.GetConstraintInfo(ctx, nil, p.ExecCfg().Codec)
	if err != nil {
		return err
	}

	inuseNames := make(map[string]struct{}, len(info))
	for k := range info {
		inuseNames[k] = struct{}{}
	}

	ckBuilder := schemaexpr.NewCheckConstraintBuilder(ctx, p.tableName, tableDesc, &p.semaCtx)
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
		ck.Validity = sqlbase.ConstraintValidity_Validating
		tableDesc.AddCheckMutation(ck, sqlbase.DescriptorMutation_ADD)
	}
	return nil
}

// MakeIndexDescriptor creates an index descriptor from a CreateIndex node and optionally
// adds a hidden computed shard column (along with its check constraint) in case the index
// is hash sharded. Note that `tableDesc` will be modified when this method is called for
// a hash sharded index.
func MakeIndexDescriptor(
	params runParams, n *tree.CreateIndex, tableDesc *sqlbase.MutableTableDescriptor,
) (*sqlbase.IndexDescriptor, error) {
	// Ensure that the columns we want to index exist before trying to create the
	// index.
	if err := validateIndexColumnsExist(tableDesc, n.Columns); err != nil {
		return nil, err
	}

	// Ensure that the index name does not exist before trying to create the index.
	if err := tableDesc.ValidateIndexNameIsUnique(string(n.Name)); err != nil {
		return nil, err
	}
	indexDesc := sqlbase.IndexDescriptor{
		Name:              string(n.Name),
		Unique:            n.Unique,
		StoreColumnNames:  n.Storing.ToStrings(),
		CreatedExplicitly: true,
	}

	if n.Inverted {
		if n.Interleave != nil {
			return nil, pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes don't support interleaved tables")
		}

		if n.PartitionBy != nil {
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
		indexDesc.Type = sqlbase.IndexDescriptor_INVERTED
		columnDesc, _, err := tableDesc.FindColumnByName(n.Columns[0].Column)
		if err != nil {
			return nil, err
		}
		if columnDesc.Type.InternalType.Family == types.GeometryFamily {
			indexDesc.GeoConfig = *geoindex.DefaultGeometryIndexConfig()
			telemetry.Inc(sqltelemetry.GeometryInvertedIndexCounter)
		}
		if columnDesc.Type.InternalType.Family == types.GeographyFamily {
			indexDesc.GeoConfig = *geoindex.DefaultGeographyIndexConfig()
			telemetry.Inc(sqltelemetry.GeographyInvertedIndexCounter)
		}
		telemetry.Inc(sqltelemetry.InvertedIndexCounter)
	}

	if n.Sharded != nil {
		if n.PartitionBy != nil {
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
		// TODO(mgartner): remove this once partial indexes are fully supported.
		if !params.SessionData().PartialIndexes {
			return nil, unimplemented.NewWithIssue(9683, "partial indexes are not supported")
		}

		idxValidator := schemaexpr.NewIndexPredicateValidator(params.ctx, n.Table, tableDesc, &params.p.semaCtx)
		expr, err := idxValidator.Validate(n.Predicate)
		if err != nil {
			return nil, err
		}

		// Store the serialized predicate expression in the IndexDescriptor.
		indexDesc.Predicate = tree.Serialize(expr)
	}

	if err := indexDesc.FillColumns(n.Columns); err != nil {
		return nil, err
	}
	return &indexDesc, nil
}

// validateIndexColumnsExists validates that the columns for an index exist
// in the table and are not being dropped prior to attempting to add the index.
func validateIndexColumnsExist(
	desc *sqlbase.MutableTableDescriptor, columns tree.IndexElemList,
) error {
	for _, column := range columns {
		_, dropping, err := desc.FindColumnByName(column.Column)
		if err != nil {
			return err
		}
		if dropping {
			return sqlbase.NewUndefinedColumnError(string(column.Column))
		}
	}
	return nil
}

// ReadingOwnWrites implements the planNodeReadingOwnWrites interface.
// This is because CREATE INDEX performs multiple KV operations on descriptors
// and expects to see its own writes.
func (n *createIndexNode) ReadingOwnWrites() {}

var invalidClusterForShardedIndexError = pgerror.Newf(pgcode.FeatureNotSupported,
	"hash sharded indexes can only be created on a cluster that has fully migrated to version 20.1")

var hashShardedIndexesDisabledError = pgerror.Newf(pgcode.FeatureNotSupported,
	"hash sharded indexes require the experimental_enable_hash_sharded_indexes cluster setting")

func setupShardedIndex(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	semaCtx *tree.SemaContext,
	shardedIndexEnabled bool,
	columns *tree.IndexElemList,
	bucketsExpr tree.Expr,
	tableDesc *sqlbase.MutableTableDescriptor,
	indexDesc *sqlbase.IndexDescriptor,
	isNewTable bool,
) (shard *sqlbase.ColumnDescriptor, newColumn bool, err error) {
	st := evalCtx.Settings
	if !st.Version.IsActive(ctx, clusterversion.VersionHashShardedIndexes) {
		return nil, false, invalidClusterForShardedIndexError
	}
	if !shardedIndexEnabled {
		return nil, false, hashShardedIndexesDisabledError
	}

	colNames := make([]string, 0, len(*columns))
	for _, c := range *columns {
		colNames = append(colNames, string(c.Column))
	}
	buckets, err := sqlbase.EvalShardBucketCount(ctx, semaCtx, evalCtx, bucketsExpr)
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
	indexDesc.Sharded = sqlbase.ShardedDescriptor{
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
	shardBuckets int, desc *sqlbase.MutableTableDescriptor, colNames []string, isNewTable bool,
) (col *sqlbase.ColumnDescriptor, created bool, err error) {
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
	columnIsUndefined := sqlbase.IsUndefinedColumnError(err)
	if err != nil && !columnIsUndefined {
		return nil, false, err
	}
	if columnIsUndefined || dropped {
		if isNewTable {
			desc.AddColumn(shardCol)
		} else {
			desc.AddColumnMutation(shardCol, sqlbase.DescriptorMutation_ADD)
		}
		created = true
	}
	return shardCol, created, nil
}

func (n *createIndexNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeCreateCounter("index"))
	_, dropped, err := n.tableDesc.FindIndexByName(string(n.n.Name))
	if err == nil {
		if dropped {
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"index %q being dropped, try again later", string(n.n.Name))
		}
		if n.n.IfNotExists {
			return nil
		}
	}

	if n.n.Concurrently {
		params.p.SendClientNotice(
			params.ctx,
			pgnotice.Newf("CONCURRENTLY is not required as all indexes are created concurrently"),
		)
	}

	// Warn against creating a non-partitioned index on a partitioned table,
	// which is undesirable in most cases.
	if n.n.PartitionBy == nil && n.tableDesc.PrimaryIndex.Partitioning.NumColumns > 0 {
		params.p.SendClientNotice(
			params.ctx,
			errors.WithHint(
				pgnotice.Newf("creating non-partitioned index on partitioned table may not be performant"),
				"Consider modifying the index such that it is also partitioned.",
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

	// If all nodes in the cluster know how to handle secondary indexes with column families,
	// write the new version into the index descriptor.
	encodingVersion := sqlbase.BaseIndexFormatVersion
	if params.p.EvalContext().Settings.Version.IsActive(params.ctx, clusterversion.VersionSecondaryIndexColumnFamilies) {
		encodingVersion = sqlbase.SecondaryIndexFamilyFormatVersion
	}
	indexDesc.Version = encodingVersion

	if n.n.PartitionBy != nil {
		partitioning, err := CreatePartitioning(params.ctx, params.p.ExecCfg().Settings,
			params.EvalContext(), n.tableDesc, indexDesc, n.n.PartitionBy)
		if err != nil {
			return err
		}
		indexDesc.Partitioning = partitioning
	}

	mutationIdx := len(n.tableDesc.Mutations)
	if err := n.tableDesc.AddIndexMutation(indexDesc, sqlbase.DescriptorMutation_ADD); err != nil {
		return err
	}
	if err := n.tableDesc.AllocateIDs(); err != nil {
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

	// Record index creation in the event log. This is an auditable log
	// event and is recorded in the same transaction as the table descriptor
	// update.
	return MakeEventLogger(params.extendedEvalCtx.ExecCfg).InsertEventRecord(
		params.ctx,
		params.p.txn,
		EventLogCreateIndex,
		int32(n.tableDesc.ID),
		int32(params.extendedEvalCtx.NodeID.SQLInstanceID()),
		struct {
			TableName  string
			IndexName  string
			Statement  string
			User       string
			MutationID uint32
		}{
			n.n.Table.FQString(), indexName, n.n.String(),
			params.SessionData().User, uint32(mutationID),
		},
	)
}

func (*createIndexNode) Next(runParams) (bool, error) { return false, nil }
func (*createIndexNode) Values() tree.Datums          { return tree.Datums{} }
func (*createIndexNode) Close(context.Context)        {}
