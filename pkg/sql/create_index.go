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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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

type indexStorageParamObserver struct {
	indexDesc *descpb.IndexDescriptor
}

var _ storageParamObserver = (*indexStorageParamObserver)(nil)

func getS2ConfigFromIndex(indexDesc *descpb.IndexDescriptor) *geoindex.S2Config {
	var s2Config *geoindex.S2Config
	if indexDesc.GeoConfig.S2Geometry != nil {
		s2Config = indexDesc.GeoConfig.S2Geometry.S2Config
	}
	if indexDesc.GeoConfig.S2Geography != nil {
		s2Config = indexDesc.GeoConfig.S2Geography.S2Config
	}
	return s2Config
}

func (a *indexStorageParamObserver) applyS2ConfigSetting(
	evalCtx *tree.EvalContext, key string, expr tree.Datum, min int64, max int64,
) error {
	s2Config := getS2ConfigFromIndex(a.indexDesc)
	if s2Config == nil {
		return errors.Newf("index setting %q can only be set on GEOMETRY or GEOGRAPHY spatial indexes", key)
	}

	val, err := datumAsInt(evalCtx, key, expr)
	if err != nil {
		return errors.Wrapf(err, "error decoding %q", key)
	}
	if val < min || val > max {
		return errors.Newf("%q value must be between %d and %d inclusive", key, min, max)
	}
	switch key {
	case `s2_max_level`:
		s2Config.MaxLevel = int32(val)
	case `s2_level_mod`:
		s2Config.LevelMod = int32(val)
	case `s2_max_cells`:
		s2Config.MaxCells = int32(val)
	}

	return nil
}

func (a *indexStorageParamObserver) applyGeometryIndexSetting(
	evalCtx *tree.EvalContext, key string, expr tree.Datum,
) error {
	if a.indexDesc.GeoConfig.S2Geometry == nil {
		return errors.Newf("%q can only be applied to GEOMETRY spatial indexes", key)
	}
	val, err := datumAsFloat(evalCtx, key, expr)
	if err != nil {
		return errors.Wrapf(err, "error decoding %q", key)
	}
	switch key {
	case `geometry_min_x`:
		a.indexDesc.GeoConfig.S2Geometry.MinX = val
	case `geometry_max_x`:
		a.indexDesc.GeoConfig.S2Geometry.MaxX = val
	case `geometry_min_y`:
		a.indexDesc.GeoConfig.S2Geometry.MinY = val
	case `geometry_max_y`:
		a.indexDesc.GeoConfig.S2Geometry.MaxY = val
	default:
		return errors.Newf("unknown key: %q", key)
	}
	return nil
}

func (a *indexStorageParamObserver) apply(
	evalCtx *tree.EvalContext, key string, expr tree.Datum,
) error {
	switch key {
	case `fillfactor`:
		return applyFillFactorStorageParam(evalCtx, key, expr)
	case `s2_max_level`:
		return a.applyS2ConfigSetting(evalCtx, key, expr, 0, 30)
	case `s2_level_mod`:
		return a.applyS2ConfigSetting(evalCtx, key, expr, 1, 3)
	case `s2_max_cells`:
		return a.applyS2ConfigSetting(evalCtx, key, expr, 1, 32)
	case `geometry_min_x`, `geometry_max_x`, `geometry_min_y`, `geometry_max_y`:
		return a.applyGeometryIndexSetting(evalCtx, key, expr)
	case `vacuum_cleanup_index_scale_factor`,
		`buffering`,
		`fastupdate`,
		`gin_pending_list_limit`,
		`pages_per_range`,
		`autosummarize`:
		return unimplemented.NewWithIssuef(43299, "storage parameter %q", key)
	}
	return errors.Errorf("invalid storage parameter %q", key)
}

func (a *indexStorageParamObserver) runPostChecks() error {
	s2Config := getS2ConfigFromIndex(a.indexDesc)
	if s2Config != nil {
		if (s2Config.MaxLevel)%s2Config.LevelMod != 0 {
			return errors.Newf(
				"s2_max_level (%d) must be divisible by s2_level_mod (%d)",
				s2Config.MaxLevel,
				s2Config.LevelMod,
			)
		}
	}

	if cfg := a.indexDesc.GeoConfig.S2Geometry; cfg != nil {
		if cfg.MaxX <= cfg.MinX {
			return errors.Newf(
				"geometry_max_x (%f) must be greater than geometry_min_x (%f)",
				cfg.MaxX,
				cfg.MinX,
			)
		}
		if cfg.MaxY <= cfg.MinY {
			return errors.Newf(
				"geometry_max_y (%f) must be greater than geometry_min_y (%f)",
				cfg.MaxY,
				cfg.MinY,
			)
		}
	}
	return nil
}

// CreateIndex creates an index.
// Privileges: CREATE on table.
//   notes: postgres requires CREATE on the table.
//          mysql requires INDEX on the table.
func (p *planner) CreateIndex(ctx context.Context, n *tree.CreateIndex) (planNode, error) {
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
	tableDesc *MutableTableDescriptor,
	shardCol *descpb.ColumnDescriptor,
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
	params runParams, n *tree.CreateIndex, tableDesc *sqlbase.MutableTableDescriptor,
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
		indexDesc.Type = descpb.IndexDescriptor_INVERTED
		columnDesc, _, err := tableDesc.FindColumnByName(n.Columns[0].Column)
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
			return nil, pgerror.Newf(pgcode.FeatureNotSupported,
				"session variable experimental_partial_indexes is set to false, cannot create a partial index")
		}

		idxValidator := schemaexpr.MakeIndexPredicateValidator(params.ctx, n.Table, tableDesc, &params.p.semaCtx)
		expr, err := idxValidator.Validate(n.Predicate)
		if err != nil {
			return nil, err
		}
		indexDesc.Predicate = expr
	}

	if err := indexDesc.FillColumns(n.Columns); err != nil {
		return nil, err
	}

	if err := applyStorageParameters(
		params.ctx,
		params.p.SemaCtx(),
		params.EvalContext(),
		n.StorageParams,
		&indexStorageParamObserver{indexDesc: &indexDesc},
	); err != nil {
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
	indexDesc *descpb.IndexDescriptor,
	isNewTable bool,
) (shard *descpb.ColumnDescriptor, newColumn bool, err error) {
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
	shardBuckets int, desc *sqlbase.MutableTableDescriptor, colNames []string, isNewTable bool,
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
	columnIsUndefined := sqlbase.IsUndefinedColumnError(err)
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
	encodingVersion := descpb.BaseIndexFormatVersion
	if params.p.EvalContext().Settings.Version.IsActive(params.ctx, clusterversion.VersionSecondaryIndexColumnFamilies) {
		encodingVersion = descpb.SecondaryIndexFamilyFormatVersion
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
	if err := n.tableDesc.AddIndexMutation(indexDesc, descpb.DescriptorMutation_ADD); err != nil {
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

	// Add all newly created type back references.
	if err := params.p.addBackRefsFromAllTypesInTable(params.ctx, n.tableDesc); err != nil {
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
