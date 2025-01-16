// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tabledesc

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// ColumnDefDescs contains the non-error return values for MakeColumnDefDescs.
type ColumnDefDescs struct {
	// tree.ColumnTableDef is the column definition from which this struct is
	// derived.
	*tree.ColumnTableDef

	// descpb.ColumnDescriptor is the column descriptor built from the column
	// definition.
	*descpb.ColumnDescriptor

	// PrimaryKeyOrUniqueIndexDescriptor is the index descriptor for the index implied by a
	// PRIMARY KEY or UNIQUE column.
	PrimaryKeyOrUniqueIndexDescriptor *descpb.IndexDescriptor

	// DefaultExpr and OnUpdateExpr are the DEFAULT and ON UPDATE expressions,
	// returned in tree.TypedExpr form for analysis, e.g. recording sequence
	// dependencies.
	DefaultExpr, OnUpdateExpr tree.TypedExpr
}

// MaxBucketAllowed is the maximum number of buckets allowed when creating a
// hash-sharded index or primary key.
const MaxBucketAllowed = 2048

// ColExprKind is an enum type of possible expressions on a column
// (e.g. 'DEFAULT' expression or 'ON UPDATE' expression).
type ColExprKind string

const (
	// DefaultExpr means the expression is a DEFAULT expression.
	DefaultExpr ColExprKind = "DEFAULT"
	// OnUpdateExpr means the expression is a ON UPDATE expression.
	OnUpdateExpr ColExprKind = "ON UPDATE"
)

// ForEachTypedExpr iterates over each typed expression in this struct.
func (cdd *ColumnDefDescs) ForEachTypedExpr(
	fn func(expr tree.TypedExpr, colExprKind ColExprKind) error,
) error {
	if cdd.ColumnTableDef.HasDefaultExpr() {
		if err := fn(cdd.DefaultExpr, DefaultExpr); err != nil {
			return err
		}
	}
	if cdd.ColumnTableDef.HasOnUpdateExpr() {
		if err := fn(cdd.OnUpdateExpr, OnUpdateExpr); err != nil {
			return err
		}
	}
	return nil
}

// MakeColumnDefDescs creates the column descriptor for a column, as well as the
// index descriptor if the column is a primary key or unique.
//
// If the column type *may* be SERIAL (or SERIAL-like), it is the
// caller's responsibility to call sql.processSerialInColumnDef() and
// sql.doCreateSequence() before MakeColumnDefDescs() to remove the
// SERIAL type and replace it with a suitable integer type and default
// expression.
//
// semaCtx can be nil if no default expression is used for the
// column or during cluster bootstrapping.
//
// See the ColumnDefDescs definition for a description of the return values.
func MakeColumnDefDescs(
	ctx context.Context,
	d *tree.ColumnTableDef,
	semaCtx *tree.SemaContext,
	evalCtx *eval.Context,
	defaultExprCtx tree.SchemaExprContext,
) (*ColumnDefDescs, error) {
	if d.IsSerial {
		// To the reader of this code: if control arrives here, this means
		// the caller has not suitably called processSerialInColumnDef()
		// prior to calling MakeColumnDefDescs. The dependent sequences
		// must be created, and the SERIAL type eliminated, prior to this
		// point.
		return nil, pgerror.New(pgcode.FeatureNotSupported,
			"SERIAL cannot be used in this context")
	}

	if len(d.CheckExprs) > 0 {
		// Should never happen since `HoistConstraints` moves these to table level
		return nil, errors.New("unexpected column CHECK constraint")
	}
	if d.HasFKConstraint() {
		// Should never happen since `HoistConstraints` moves these to table level
		return nil, errors.New("unexpected column REFERENCED constraint")
	}

	col := &descpb.ColumnDescriptor{
		Name:     string(d.Name),
		Nullable: d.Nullable.Nullability != tree.NotNull && !d.PrimaryKey.IsPrimaryKey,
		Virtual:  d.IsVirtual(),
		Hidden:   d.Hidden,
	}
	ret := &ColumnDefDescs{
		ColumnTableDef:   d,
		ColumnDescriptor: col,
	}

	if d.GeneratedIdentity.IsGeneratedAsIdentity {
		switch d.GeneratedIdentity.GeneratedAsIdentityType {
		case tree.GeneratedAlways:
			col.GeneratedAsIdentityType = catpb.GeneratedAsIdentityType_GENERATED_ALWAYS
		case tree.GeneratedByDefault:
			col.GeneratedAsIdentityType = catpb.GeneratedAsIdentityType_GENERATED_BY_DEFAULT
		default:
			return nil, errors.AssertionFailedf(
				"column %s is of invalid generated as identity type (neither ALWAYS nor BY DEFAULT)", string(d.Name))
		}
		// GeneratedAsIdentitySequenceOption is used to populate the sequence options for the column information schema.
		// An empty string will populate default values and null will generate null values.
		s := ""
		if genSeqOpt := d.GeneratedIdentity.SeqOptions; genSeqOpt != nil {
			// Override GeneratedAsIdentitySequenceOption default values with specified SeqOptions.
			s = tree.Serialize(&d.GeneratedIdentity.SeqOptions)
		}
		col.GeneratedAsIdentitySequenceOption = &s
	}

	// Validate and assign column type.
	resType, err := tree.ResolveType(ctx, d.Type, semaCtx.GetTypeResolver())
	if err != nil {
		return nil, err
	}
	if err = colinfo.ValidateColumnDefType(ctx, evalCtx.Settings, resType); err != nil {
		return nil, err
	}
	col.Type = resType

	if d.HasDefaultExpr() {
		// Verify the default expression type is compatible with the column type
		// and does not contain invalid functions.
		ret.DefaultExpr, err = schemaexpr.SanitizeVarFreeExpr(
			ctx, d.DefaultExpr.Expr, resType, defaultExprCtx, semaCtx, volatility.Volatile, true, /*allowAssignmentCast*/
		)
		if err != nil {
			return nil, err
		}
		if err := funcdesc.MaybeFailOnUDFUsage(ret.DefaultExpr, defaultExprCtx, evalCtx.Settings.Version.ActiveVersion(ctx)); err != nil {
			return nil, err
		}

		ret.DefaultExpr, err = schemaexpr.MaybeReplaceUDFNameWithOIDReferenceInTypedExpr(ret.DefaultExpr)
		if err != nil {
			return nil, err
		}

		// Keep the type checked expression so that the type annotation gets
		// properly stored, only if the default expression is not NULL.
		// Otherwise we want to keep the default expression nil.
		if ret.DefaultExpr != tree.DNull {
			d.DefaultExpr.Expr = ret.DefaultExpr
			s := tree.Serialize(d.DefaultExpr.Expr)
			col.DefaultExpr = &s
		}
	}

	if d.HasOnUpdateExpr() {
		// Verify the on update expression type is compatible with the column type
		// and does not contain invalid functions.
		ret.OnUpdateExpr, err = schemaexpr.SanitizeVarFreeExpr(
			ctx, d.OnUpdateExpr.Expr, resType, tree.ColumnOnUpdateExpr, semaCtx, volatility.Volatile, true, /*allowAssignmentCast*/
		)
		if err != nil {
			return nil, err
		}
		if err := funcdesc.MaybeFailOnUDFUsage(ret.OnUpdateExpr, tree.ColumnOnUpdateExpr, evalCtx.Settings.Version.ActiveVersion(ctx)); err != nil {
			return nil, err
		}

		d.OnUpdateExpr.Expr = ret.OnUpdateExpr
		s := tree.Serialize(d.OnUpdateExpr.Expr)
		col.OnUpdateExpr = &s
	}

	if d.IsComputed() {
		// Note: We do not validate the computed column expression here because
		// it may reference columns that have not yet been added to a table
		// descriptor. Callers must validate the expression with
		// schemaexpr.ValidateComputedColumnExpression once all possible
		// reference columns are part of the table descriptor.
		s := tree.Serialize(d.Computed.Expr)
		col.ComputeExpr = &s
	}

	if d.PrimaryKey.IsPrimaryKey || (d.Unique.IsUnique && !d.Unique.WithoutIndex) {
		if !d.PrimaryKey.Sharded {
			ret.PrimaryKeyOrUniqueIndexDescriptor = &descpb.IndexDescriptor{
				Unique:              true,
				KeyColumnNames:      []string{string(d.Name)},
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC},
			}
		} else {
			buckets, err := EvalShardBucketCount(ctx, semaCtx, evalCtx, d.PrimaryKey.ShardBuckets, d.PrimaryKey.StorageParams)
			if err != nil {
				return nil, err
			}
			shardColName := GetShardColumnName([]string{string(d.Name)}, buckets)
			ret.PrimaryKeyOrUniqueIndexDescriptor = &descpb.IndexDescriptor{
				Unique:              true,
				KeyColumnNames:      []string{shardColName, string(d.Name)},
				KeyColumnDirections: []catenumpb.IndexColumn_Direction{catenumpb.IndexColumn_ASC, catenumpb.IndexColumn_ASC},
				Sharded: catpb.ShardedDescriptor{
					IsSharded:    true,
					Name:         shardColName,
					ShardBuckets: buckets,
					ColumnNames:  []string{string(d.Name)},
				},
			}
		}
		if d.Unique.ConstraintName != "" {
			ret.PrimaryKeyOrUniqueIndexDescriptor.Name = string(d.Unique.ConstraintName)
		}
	}

	return ret, nil
}

// EvalShardBucketCount evaluates and checks the integer argument to a `USING HASH WITH
// BUCKET_COUNT` index creation query.
func EvalShardBucketCount(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *eval.Context,
	shardBuckets tree.Expr,
	storageParams tree.StorageParams,
) (int32, error) {
	_, legacyBucketNotGiven := shardBuckets.(tree.DefaultVal)
	paramVal := storageParams.GetVal(`bucket_count`)

	// The legacy `BUCKET_COUNT` should not be set together with the new
	// `bucket_count` storage param.
	if !legacyBucketNotGiven && paramVal != nil {
		return 0, pgerror.New(
			pgcode.InvalidParameterValue,
			`"bucket_count" storage parameter and "BUCKET_COUNT" cannot be set at the same time`,
		)
	}

	var buckets int64
	const invalidBucketCountMsg = `hash sharded index bucket count must be in range [2, 2048], got %v`
	// If shardBuckets is not specified, use default bucket count from cluster setting.
	if legacyBucketNotGiven && paramVal == nil {
		buckets = DefaultHashShardedIndexBucketCount.Get(&evalCtx.Settings.SV)
	} else {
		if paramVal != nil {
			shardBuckets = paramVal
		}
		// Check if shardBuckets is NULL
		if shardBuckets == tree.DNull {
			return 0, pgerror.Newf(pgcode.InvalidParameterValue, invalidBucketCountMsg, "NULL")
		}
		typedExpr, err := schemaexpr.SanitizeVarFreeExpr(
			ctx, shardBuckets, types.Int, "BUCKET_COUNT", semaCtx, volatility.Volatile, false, /*allowAssignmentCast*/
		)
		if err != nil {
			return 0, err
		}
		d, err := eval.Expr(ctx, evalCtx, typedExpr)
		if err != nil {
			return 0, pgerror.Wrapf(err, pgcode.InvalidParameterValue, invalidBucketCountMsg, typedExpr)
		}
		buckets = int64(tree.MustBeDInt(d))
	}
	if buckets < 2 {
		return 0, pgerror.Newf(pgcode.InvalidParameterValue, invalidBucketCountMsg, buckets)
	}
	if buckets > MaxBucketAllowed {
		return 0, pgerror.Newf(pgcode.InvalidParameterValue, invalidBucketCountMsg, buckets)
	}
	return int32(buckets), nil
}

// DefaultHashShardedIndexBucketCount is the cluster setting of default bucket
// count for hash sharded index when bucket count is not specified in index
// definition.
var DefaultHashShardedIndexBucketCount = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.defaults.default_hash_sharded_index_bucket_count",
	"used as bucket count if bucket count is not specified in hash sharded index definition",
	16,
	settings.NonNegativeInt,
	settings.WithPublic)

// GetShardColumnName generates a name for the hidden shard column to be used to create a
// hash sharded index.
func GetShardColumnName(colNames []string, buckets int32) string {
	// We sort the `colNames` here because we want to avoid creating a duplicate shard
	// column if one already exists for the set of columns in `colNames`.
	sort.Strings(colNames)
	return strings.Join(
		append(append([]string{`crdb_internal`}, colNames...), fmt.Sprintf(`shard_%v`, buckets)), `_`,
	)
}

// getExistingOrNewConstraintCache should be the only place where the constraintCache
// field in wrapper is ever read.
func (desc *wrapper) getExistingOrNewConstraintCache() *constraintCache {
	if desc.constraintCache != nil {
		return desc.constraintCache
	}
	return newConstraintCache(desc.TableDesc(), desc.getExistingOrNewIndexCache(), desc.getExistingOrNewMutationCache())
}

// AllConstraints implements the catalog.TableDescriptor interface.
func (desc *wrapper) AllConstraints() []catalog.Constraint {
	return desc.getExistingOrNewConstraintCache().all
}

// EnforcedConstraints implements the catalog.TableDescriptor interface.
func (desc *wrapper) EnforcedConstraints() []catalog.Constraint {
	return desc.getExistingOrNewConstraintCache().allEnforced
}

// CheckConstraints implements the catalog.TableDescriptor interface.
func (desc *wrapper) CheckConstraints() []catalog.CheckConstraint {
	return desc.getExistingOrNewConstraintCache().checks
}

// EnforcedCheckConstraints implements the catalog.TableDescriptor interface.
func (desc *wrapper) EnforcedCheckConstraints() []catalog.CheckConstraint {
	return desc.getExistingOrNewConstraintCache().checksEnforced
}

// OutboundForeignKeys implements the catalog.TableDescriptor interface.
func (desc *wrapper) OutboundForeignKeys() []catalog.ForeignKeyConstraint {
	return desc.getExistingOrNewConstraintCache().fks
}

// EnforcedOutboundForeignKeys implements the catalog.TableDescriptor
// interface.
func (desc *wrapper) EnforcedOutboundForeignKeys() []catalog.ForeignKeyConstraint {
	return desc.getExistingOrNewConstraintCache().fksEnforced
}

// InboundForeignKeys implements the catalog.TableDescriptor interface.
func (desc *wrapper) InboundForeignKeys() []catalog.ForeignKeyConstraint {
	return desc.getExistingOrNewConstraintCache().fkBackRefs
}

// UniqueConstraintsWithIndex implements the catalog.TableDescriptor
// interface.
func (desc *wrapper) UniqueConstraintsWithIndex() []catalog.UniqueWithIndexConstraint {
	return desc.getExistingOrNewConstraintCache().uwis
}

// EnforcedUniqueConstraintsWithIndex implements the catalog.TableDescriptor
// interface.
func (desc *wrapper) EnforcedUniqueConstraintsWithIndex() []catalog.UniqueWithIndexConstraint {
	return desc.getExistingOrNewConstraintCache().uwisEnforced
}

// UniqueConstraintsWithoutIndex implements the catalog.TableDescriptor
// interface.
func (desc *wrapper) UniqueConstraintsWithoutIndex() []catalog.UniqueWithoutIndexConstraint {
	return desc.getExistingOrNewConstraintCache().uwois
}

// EnforcedUniqueConstraintsWithoutIndex implements the catalog.TableDescriptor
// interface.
func (desc *wrapper) EnforcedUniqueConstraintsWithoutIndex() []catalog.UniqueWithoutIndexConstraint {
	return desc.getExistingOrNewConstraintCache().uwoisEnforced
}

// InitTableDescriptor returns a blank TableDescriptor.
func InitTableDescriptor(
	id, parentID, parentSchemaID descpb.ID,
	name string,
	creationTime hlc.Timestamp,
	privileges *catpb.PrivilegeDescriptor,
	persistence tree.Persistence,
) Mutable {
	return Mutable{
		wrapper: wrapper{
			TableDescriptor: descpb.TableDescriptor{
				ID:                      id,
				Name:                    name,
				ParentID:                parentID,
				UnexposedParentSchemaID: parentSchemaID,
				FormatVersion:           descpb.InterleavedFormatVersion,
				Version:                 1,
				ModificationTime:        creationTime,
				Privileges:              privileges,
				CreateAsOfTime:          creationTime,
				Temporary:               persistence.IsTemporary(),
			},
		},
	}
}

// PrimaryKeyString returns the pretty-printed primary key declaration for a
// table descriptor.
func PrimaryKeyString(desc catalog.TableDescriptor) string {
	primaryIdx := desc.GetPrimaryIndex()
	f := tree.NewFmtCtx(tree.FmtSimple)
	f.WriteString("PRIMARY KEY (")
	startIdx := primaryIdx.ExplicitColumnStartIdx()
	for i, n := startIdx, primaryIdx.NumKeyColumns(); i < n; i++ {
		if i > startIdx {
			f.WriteString(", ")
		}
		// Primary key columns cannot be inaccessible computed columns, so it is
		// safe to always print the column name. For secondary indexes, we have
		// to print inaccessible computed column expressions. See
		// catformat.FormatIndexElements.
		name := primaryIdx.GetKeyColumnName(i)
		f.FormatNameP(&name)
		f.WriteByte(' ')
		f.WriteString(primaryIdx.GetKeyColumnDirection(i).String())
	}
	f.WriteByte(')')
	if primaryIdx.IsSharded() {
		f.WriteString(
			fmt.Sprintf(" USING HASH WITH (bucket_count=%v)", primaryIdx.GetSharded().ShardBuckets),
		)
	}
	return f.CloseAndGetString()
}

// ColumnNamePlaceholder constructs a placeholder name for a column based on its
// id.
func ColumnNamePlaceholder(id descpb.ColumnID) string {
	return fmt.Sprintf("crdb_internal_column_%d_name_placeholder", id)
}

// IndexNamePlaceholder constructs a placeholder name for an index based on its
// id.
func IndexNamePlaceholder(id descpb.IndexID) string {
	return fmt.Sprintf("crdb_internal_index_%d_name_placeholder", id)
}

// ConstraintNamePlaceholder constructs a placeholder name for a constraint based
// on its id.
func ConstraintNamePlaceholder(id descpb.ConstraintID) string {
	return fmt.Sprintf("crdb_internal_constraint_%d_name_placeholder", id)
}

// PolicyNamePlaceholder constructs a placeholder name for a policy based
// on its id.
func PolicyNamePlaceholder(id descpb.PolicyID) string {
	return fmt.Sprintf("crdb_internal_policy_%d_name_placeholder", id)
}

// RenameColumnInTable will rename the column in tableDesc from oldName to
// newName, including in expressions as well as shard columns.
// The function is recursive because of this, but there should only be one level
// of recursion.
func RenameColumnInTable(
	tableDesc *Mutable,
	col catalog.Column,
	newName tree.Name,
	isShardColumnRenameable func(shardCol catalog.Column, newShardColName tree.Name) (bool, error),
) error {
	renameInExpr := func(expr *string) error {
		newExpr, renameErr := schemaexpr.RenameColumn(*expr, col.ColName(), newName)
		if renameErr != nil {
			return renameErr
		}
		*expr = newExpr
		return nil
	}

	// Rename the column in CHECK constraints.
	for i := range tableDesc.Checks {
		if err := renameInExpr(&tableDesc.Checks[i].Expr); err != nil {
			return err
		}
	}

	// Rename the column in computed columns.
	for i := range tableDesc.Columns {
		if otherCol := &tableDesc.Columns[i]; otherCol.IsComputed() {
			if err := renameInExpr(otherCol.ComputeExpr); err != nil {
				return err
			}
		}
	}

	// Rename the column in partial idx predicates.
	for _, idx := range tableDesc.PublicNonPrimaryIndexes() {
		if idx.IsPartial() {
			if err := renameInExpr(&idx.IndexDesc().Predicate); err != nil {
				return err
			}
		}
	}

	// Rename the column in the TTL expiration expression.
	if tableDesc.HasRowLevelTTL() {
		if expirationExpr := tableDesc.GetRowLevelTTL().ExpirationExpr; expirationExpr != "" {
			expirationExprStr := string(expirationExpr)
			if err := renameInExpr(&expirationExprStr); err != nil {
				return err
			}
			tableDesc.GetRowLevelTTL().ExpirationExpr = catpb.Expression(expirationExprStr)
		}
	}

	// Do all of the above renames inside check constraints, computed expressions,
	// and idx predicates that are in mutations.
	for i := range tableDesc.Mutations {
		m := &tableDesc.Mutations[i]
		if constraint := m.GetConstraint(); constraint != nil {
			if constraint.ConstraintType == descpb.ConstraintToUpdate_CHECK ||
				constraint.ConstraintType == descpb.ConstraintToUpdate_NOT_NULL {
				if err := renameInExpr(&constraint.Check.Expr); err != nil {
					return err
				}
			}
		} else if otherCol := m.GetColumn(); otherCol != nil {
			if otherCol.IsComputed() {
				if err := renameInExpr(otherCol.ComputeExpr); err != nil {
					return err
				}
			}
		} else if idx := m.GetIndex(); idx != nil {
			if idx.IsPartial() {
				if err := renameInExpr(&idx.Predicate); err != nil {
					return err
				}
			}
		}
	}

	// Rename the column in hash-sharded idx descriptors. Potentially rename the
	// shard column too if we haven't already done it.
	shardColumnsToRename := make(map[tree.Name]tree.Name) // map[oldShardColName]newShardColName
	maybeUpdateShardedDesc := func(shardedDesc *catpb.ShardedDescriptor) {
		if !shardedDesc.IsSharded {
			return
		}
		// Simpler case: If the shard column is to be renamed, keep the
		// shard descriptor name in sync.
		if shardedDesc.Name == string(col.ColName()) {
			shardedDesc.Name = string(newName)
			return
		}
		// Harder case: If one of the columns that the shard column is based on is
		// to be renamed, then rename the base column in the descriptor as well as
		// the shard descriptor name. We also record this fact in `shardColumnsToRename`
		// so the next recursive call will rename the shard column.
		oldShardColName := tree.Name(GetShardColumnName(
			shardedDesc.ColumnNames, shardedDesc.ShardBuckets))
		var changed bool
		for i, c := range shardedDesc.ColumnNames {
			if c == string(col.ColName()) {
				changed = true
				shardedDesc.ColumnNames[i] = string(newName)
			}
		}
		if !changed {
			return
		}
		newShardColName, alreadyRenamed := shardColumnsToRename[oldShardColName]
		if !alreadyRenamed {
			newShardColName = tree.Name(GetShardColumnName(shardedDesc.ColumnNames, shardedDesc.ShardBuckets))
			shardColumnsToRename[oldShardColName] = newShardColName
		}
		// Keep the shardedDesc name in sync with the column name.
		shardedDesc.Name = string(newShardColName)
	}
	for _, idx := range tableDesc.NonDropIndexes() {
		maybeUpdateShardedDesc(&idx.IndexDesc().Sharded)
	}

	// Rename the REGIONAL BY ROW column reference.
	if tableDesc.IsLocalityRegionalByRow() {
		rbrColName, err := tableDesc.GetRegionalByRowTableRegionColumnName()
		if err != nil {
			return err
		}
		if rbrColName == col.ColName() {
			tableDesc.SetTableLocalityRegionalByRow(newName)
		}
	}

	// Rename the column name in the column, the column family, the indexes...
	tableDesc.RenameColumnDescriptor(col, string(newName))

	// Rename any shard columns which need to be renamed because their name was
	// based on this column.
	for oldShardColName, newShardColName := range shardColumnsToRename {
		shardCol, err := catalog.MustFindColumnByTreeName(tableDesc, oldShardColName)
		if err != nil {
			return err
		}
		var canBeRenamed bool
		if isShardColumnRenameable == nil {
			canBeRenamed = true
		} else if canBeRenamed, err = isShardColumnRenameable(shardCol, newShardColName); err != nil {
			return err
		}
		if !canBeRenamed {
			return nil
		}
		// Recursively rename the shard column.
		// We don't need to worry about deeper than one recursive call because
		// shard columns cannot refer to each other.
		return RenameColumnInTable(tableDesc, shardCol, newShardColName, nil /* isShardColumnRenameable */)
	}

	return nil
}
