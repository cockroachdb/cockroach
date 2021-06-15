// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tabledesc

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

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
// The DEFAULT expression is returned in TypedExpr form for analysis (e.g. recording
// sequence dependencies).
func MakeColumnDefDescs(
	ctx context.Context, d *tree.ColumnTableDef, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext,
) (*descpb.ColumnDescriptor, *descpb.IndexDescriptor, tree.TypedExpr, error) {
	if d.IsSerial {
		// To the reader of this code: if control arrives here, this means
		// the caller has not suitably called processSerialInColumnDef()
		// prior to calling MakeColumnDefDescs. The dependent sequences
		// must be created, and the SERIAL type eliminated, prior to this
		// point.
		return nil, nil, nil, pgerror.New(pgcode.FeatureNotSupported,
			"SERIAL cannot be used in this context")
	}

	if len(d.CheckExprs) > 0 {
		// Should never happen since `HoistConstraints` moves these to table level
		return nil, nil, nil, errors.New("unexpected column CHECK constraint")
	}
	if d.HasFKConstraint() {
		// Should never happen since `HoistConstraints` moves these to table level
		return nil, nil, nil, errors.New("unexpected column REFERENCED constraint")
	}

	col := &descpb.ColumnDescriptor{
		Name:     string(d.Name),
		Nullable: d.Nullable.Nullability != tree.NotNull && !d.PrimaryKey.IsPrimaryKey,
		Virtual:  d.IsVirtual(),
		Hidden:   d.Hidden,
	}

	// Validate and assign column type.
	resType, err := tree.ResolveType(ctx, d.Type, semaCtx.GetTypeResolver())
	if err != nil {
		return nil, nil, nil, err
	}
	if err := colinfo.ValidateColumnDefType(resType); err != nil {
		return nil, nil, nil, err
	}
	col.Type = resType

	var typedExpr tree.TypedExpr
	if d.HasDefaultExpr() {
		// Verify the default expression type is compatible with the column type
		// and does not contain invalid functions.
		var err error
		if typedExpr, err = schemaexpr.SanitizeVarFreeExpr(
			ctx, d.DefaultExpr.Expr, resType, "DEFAULT", semaCtx, tree.VolatilityVolatile,
		); err != nil {
			return nil, nil, nil, err
		}

		// Keep the type checked expression so that the type annotation gets
		// properly stored, only if the default expression is not NULL.
		// Otherwise we want to keep the default expression nil.
		if typedExpr != tree.DNull {
			d.DefaultExpr.Expr = typedExpr
			s := tree.Serialize(d.DefaultExpr.Expr)
			col.DefaultExpr = &s
		}
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

	var idx *descpb.IndexDescriptor
	if d.PrimaryKey.IsPrimaryKey || (d.Unique.IsUnique && !d.Unique.WithoutIndex) {
		if !d.PrimaryKey.Sharded {
			idx = &descpb.IndexDescriptor{
				Unique:              true,
				KeyColumnNames:      []string{string(d.Name)},
				KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
			}
		} else {
			buckets, err := EvalShardBucketCount(ctx, semaCtx, evalCtx, d.PrimaryKey.ShardBuckets)
			if err != nil {
				return nil, nil, nil, err
			}
			shardColName := GetShardColumnName([]string{string(d.Name)}, buckets)
			idx = &descpb.IndexDescriptor{
				Unique:              true,
				KeyColumnNames:      []string{shardColName, string(d.Name)},
				KeyColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
				Sharded: descpb.ShardedDescriptor{
					IsSharded:    true,
					Name:         shardColName,
					ShardBuckets: buckets,
					ColumnNames:  []string{string(d.Name)},
				},
			}
		}
		if d.Unique.ConstraintName != "" {
			idx.Name = string(d.Unique.ConstraintName)
		}
	}

	return col, idx, typedExpr, nil
}

// EvalShardBucketCount evaluates and checks the integer argument to a `USING HASH WITH
// BUCKET_COUNT` index creation query.
func EvalShardBucketCount(
	ctx context.Context, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, shardBuckets tree.Expr,
) (int32, error) {
	const invalidBucketCountMsg = `BUCKET_COUNT must be an integer greater than 1`
	typedExpr, err := schemaexpr.SanitizeVarFreeExpr(
		ctx, shardBuckets, types.Int, "BUCKET_COUNT", semaCtx, tree.VolatilityVolatile,
	)
	if err != nil {
		return 0, err
	}
	d, err := typedExpr.Eval(evalCtx)
	if err != nil {
		return 0, pgerror.Wrap(err, pgcode.InvalidParameterValue, invalidBucketCountMsg)
	}
	buckets := tree.MustBeDInt(d)
	if buckets < 2 {
		return 0, pgerror.New(pgcode.InvalidParameterValue, invalidBucketCountMsg)
	}
	return int32(buckets), nil
}

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

// GetConstraintInfo returns a summary of all constraints on the table.
func (desc *wrapper) GetConstraintInfo() (map[string]descpb.ConstraintDetail, error) {
	return desc.collectConstraintInfo(nil)
}

// GetConstraintInfoWithLookup returns a summary of all constraints on the
// table using the provided function to fetch a TableDescriptor from an ID.
func (desc *wrapper) GetConstraintInfoWithLookup(
	tableLookup catalog.TableLookupFn,
) (map[string]descpb.ConstraintDetail, error) {
	return desc.collectConstraintInfo(tableLookup)
}

// CheckUniqueConstraints returns a non-nil error if a descriptor contains two
// constraints with the same name.
func (desc *wrapper) CheckUniqueConstraints() error {
	_, err := desc.collectConstraintInfo(nil)
	return err
}

// if `tableLookup` is non-nil, provide a full summary of constraints, otherwise just
// check that constraints have unique names.
func (desc *wrapper) collectConstraintInfo(
	tableLookup catalog.TableLookupFn,
) (map[string]descpb.ConstraintDetail, error) {
	info := make(map[string]descpb.ConstraintDetail)

	// Indexes provide PK and Unique constraints that are enforced by an index.
	for _, indexI := range desc.NonDropIndexes() {
		index := indexI.IndexDesc()
		if index.ID == desc.PrimaryIndex.ID {
			if _, ok := info[index.Name]; ok {
				return nil, pgerror.Newf(pgcode.DuplicateObject,
					"duplicate constraint name: %q", index.Name)
			}
			colHiddenMap := make(map[descpb.ColumnID]bool, len(desc.Columns))
			for i := range desc.Columns {
				col := &desc.Columns[i]
				colHiddenMap[col.ID] = col.Hidden
			}
			// Don't include constraints against only hidden columns.
			// This prevents the auto-created rowid primary key index from showing up
			// in show constraints.
			hidden := true
			for _, id := range index.KeyColumnIDs {
				if !colHiddenMap[id] {
					hidden = false
					break
				}
			}
			if hidden {
				continue
			}
			detail := descpb.ConstraintDetail{Kind: descpb.ConstraintTypePK}
			detail.Columns = index.KeyColumnNames
			detail.Index = index
			info[index.Name] = detail
		} else if index.Unique {
			if _, ok := info[index.Name]; ok {
				return nil, pgerror.Newf(pgcode.DuplicateObject,
					"duplicate constraint name: %q", index.Name)
			}
			detail := descpb.ConstraintDetail{Kind: descpb.ConstraintTypeUnique}
			detail.Columns = index.KeyColumnNames
			detail.Index = index
			info[index.Name] = detail
		}
	}

	// Get the unique constraints that are not enforced by an index.
	ucs := desc.AllActiveAndInactiveUniqueWithoutIndexConstraints()
	for _, uc := range ucs {
		if _, ok := info[uc.Name]; ok {
			return nil, pgerror.Newf(pgcode.DuplicateObject,
				"duplicate constraint name: %q", uc.Name)
		}
		detail := descpb.ConstraintDetail{Kind: descpb.ConstraintTypeUnique}
		// Constraints in the Validating state are considered Unvalidated for this
		// purpose.
		detail.Unvalidated = uc.Validity != descpb.ConstraintValidity_Validated
		var err error
		detail.Columns, err = desc.NamesForColumnIDs(uc.ColumnIDs)
		if err != nil {
			return nil, err
		}
		detail.UniqueWithoutIndexConstraint = uc
		info[uc.Name] = detail
	}

	fks := desc.AllActiveAndInactiveForeignKeys()
	for _, fk := range fks {
		if _, ok := info[fk.Name]; ok {
			return nil, pgerror.Newf(pgcode.DuplicateObject,
				"duplicate constraint name: %q", fk.Name)
		}
		detail := descpb.ConstraintDetail{Kind: descpb.ConstraintTypeFK}
		// Constraints in the Validating state are considered Unvalidated for this
		// purpose.
		detail.Unvalidated = fk.Validity != descpb.ConstraintValidity_Validated
		var err error
		detail.Columns, err = desc.NamesForColumnIDs(fk.OriginColumnIDs)
		if err != nil {
			return nil, err
		}
		detail.FK = fk

		if tableLookup != nil {
			other, err := tableLookup(fk.ReferencedTableID)
			if err != nil {
				return nil, errors.NewAssertionErrorWithWrappedErrf(err,
					"error resolving table %d referenced in foreign key",
					log.Safe(fk.ReferencedTableID))
			}
			referencedColumnNames, err := other.NamesForColumnIDs(fk.ReferencedColumnIDs)
			if err != nil {
				return nil, err
			}
			detail.Details = fmt.Sprintf("%s.%v", other.GetName(), referencedColumnNames)
			detail.ReferencedTable = other.TableDesc()
		}
		info[fk.Name] = detail
	}

	for _, c := range desc.AllActiveAndInactiveChecks() {
		if _, ok := info[c.Name]; ok {
			return nil, pgerror.Newf(pgcode.DuplicateObject,
				"duplicate constraint name: %q", c.Name)
		}
		detail := descpb.ConstraintDetail{Kind: descpb.ConstraintTypeCheck}
		// Constraints in the Validating state are considered Unvalidated for this
		// purpose.
		detail.Unvalidated = c.Validity != descpb.ConstraintValidity_Validated
		detail.CheckConstraint = c
		detail.Details = c.Expr
		if tableLookup != nil {
			colsUsed, err := desc.ColumnsUsed(c)
			if err != nil {
				return nil, errors.NewAssertionErrorWithWrappedErrf(err,
					"error computing columns used in check constraint %q", c.Name)
			}
			for _, colID := range colsUsed {
				col, err := desc.FindColumnWithID(colID)
				if err != nil {
					return nil, errors.NewAssertionErrorWithWrappedErrf(err,
						"error finding column %d in table %s", log.Safe(colID), desc.Name)
				}
				detail.Columns = append(detail.Columns, col.GetName())
			}
		}
		info[c.Name] = detail
	}
	return info, nil
}

// FindFKReferencedUniqueConstraint finds the first index in the supplied
// referencedTable that can satisfy a foreign key of the supplied column ids.
// If no such index exists, attempts to find a unique constraint on the supplied
// column ids. If neither an index nor unique constraint is found, returns an
// error.
func FindFKReferencedUniqueConstraint(
	referencedTable catalog.TableDescriptor, referencedColIDs descpb.ColumnIDs,
) (descpb.UniqueConstraint, error) {
	// Search for a unique index on the referenced table that matches our foreign
	// key columns.
	primaryIndex := referencedTable.GetPrimaryIndex()
	if primaryIndex.IsValidReferencedUniqueConstraint(referencedColIDs) {
		return primaryIndex.IndexDesc(), nil
	}
	// If the PK doesn't match, find the index corresponding to the referenced column.
	for _, idx := range referencedTable.PublicNonPrimaryIndexes() {
		if idx.IsValidReferencedUniqueConstraint(referencedColIDs) {
			return idx.IndexDesc(), nil
		}
	}
	// As a last resort, try to find a unique constraint with matching columns.
	uniqueWithoutIndexConstraints := referencedTable.GetUniqueWithoutIndexConstraints()
	for i := range uniqueWithoutIndexConstraints {
		c := &uniqueWithoutIndexConstraints[i]

		// A partial unique constraint cannot be a reference constraint for a
		// FK.
		if c.IsPartial() {
			continue
		}

		if c.IsValidReferencedUniqueConstraint(referencedColIDs) {
			return c, nil
		}
	}
	return nil, pgerror.Newf(
		pgcode.ForeignKeyViolation,
		"there is no unique constraint matching given keys for referenced table %s",
		referencedTable.GetName(),
	)
}

// FindFKOriginIndexInTxn finds the first index in the supplied originTable
// that can satisfy an outgoing foreign key of the supplied column ids.
// It returns either an index that is active, or an index that was created
// in the same transaction that is currently running.
func FindFKOriginIndexInTxn(
	originTable *Mutable, originColIDs descpb.ColumnIDs,
) (*descpb.IndexDescriptor, error) {
	// Search for an index on the origin table that matches our foreign
	// key columns.
	if originTable.PrimaryIndex.IsValidOriginIndex(originColIDs) {
		return &originTable.PrimaryIndex, nil
	}
	// If the PK doesn't match, find the index corresponding to the origin column.
	for i := range originTable.Indexes {
		idx := &originTable.Indexes[i]
		if idx.IsValidOriginIndex(originColIDs) {
			return idx, nil
		}
	}
	currentMutationID := originTable.ClusterVersion.NextMutationID
	for i := range originTable.Mutations {
		mut := &originTable.Mutations[i]
		if idx := mut.GetIndex(); idx != nil &&
			mut.MutationID == currentMutationID &&
			mut.Direction == descpb.DescriptorMutation_ADD {
			if idx.IsValidOriginIndex(originColIDs) {
				return idx, nil
			}
		}
	}
	return nil, pgerror.Newf(
		pgcode.ForeignKeyViolation,
		"there is no index matching given keys for referenced table %s",
		originTable.Name,
	)
}

// InitTableDescriptor returns a blank TableDescriptor.
func InitTableDescriptor(
	id, parentID, parentSchemaID descpb.ID,
	name string,
	creationTime hlc.Timestamp,
	privileges *descpb.PrivilegeDescriptor,
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

// FindPublicColumnsWithNames is a convenience function which behaves exactly
// like FindPublicColumnWithName applied repeatedly to the names in the
// provided list, returning early at the first encountered error.
func FindPublicColumnsWithNames(
	desc catalog.TableDescriptor, names tree.NameList,
) ([]catalog.Column, error) {
	cols := make([]catalog.Column, len(names))
	for i, name := range names {
		c, err := FindPublicColumnWithName(desc, name)
		if err != nil {
			return nil, err
		}
		cols[i] = c
	}
	return cols, nil
}

// FindPublicColumnWithName is a convenience function which behaves exactly
// like desc.FindColumnWithName except it ignores column mutations.
func FindPublicColumnWithName(
	desc catalog.TableDescriptor, name tree.Name,
) (catalog.Column, error) {
	col, err := desc.FindColumnWithName(name)
	if err != nil {
		return nil, err
	}
	if !col.Public() {
		return nil, colinfo.NewUndefinedColumnError(string(name))
	}
	return col, nil
}

// FindPublicColumnWithID is a convenience function which behaves exactly
// like desc.FindColumnWithID except it ignores column mutations.
func FindPublicColumnWithID(
	desc catalog.TableDescriptor, id descpb.ColumnID,
) (catalog.Column, error) {
	col, err := desc.FindColumnWithID(id)
	if err != nil {
		return nil, err
	}
	if !col.Public() {
		return nil, fmt.Errorf("column-id \"%d\" does not exist", id)
	}
	return col, nil
}

// FindVirtualColumn returns a catalog.Column matching the virtual column
// descriptor in `spec` if not nil, nil otherwise.
func FindVirtualColumn(
	desc catalog.TableDescriptor, virtualColDesc *descpb.ColumnDescriptor,
) catalog.Column {
	if virtualColDesc == nil {
		return nil
	}
	found, err := desc.FindColumnWithID(virtualColDesc.ID)
	if err != nil {
		panic(errors.HandleAsAssertionFailure(err))
	}
	virtualColumn := found.DeepCopy()
	*virtualColumn.ColumnDesc() = *virtualColDesc
	return virtualColumn
}
