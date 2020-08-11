// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"golang.org/x/text/language"
)

// SanitizeVarFreeExpr verifies that an expression is valid, has the correct
// type and contains no variable expressions. It returns the type-checked and
// constant-folded expression.
func SanitizeVarFreeExpr(
	ctx context.Context,
	expr tree.Expr,
	expectedType *types.T,
	context string,
	semaCtx *tree.SemaContext,
	maxVolatility tree.Volatility,
) (tree.TypedExpr, error) {
	if tree.ContainsVars(expr) {
		return nil, pgerror.Newf(pgcode.Syntax,
			"variable sub-expressions are not allowed in %s", context)
	}

	// We need to save and restore the previous value of the field in
	// semaCtx in case we are recursively called from another context
	// which uses the properties field.
	defer semaCtx.Properties.Restore(semaCtx.Properties)

	// Ensure that the expression doesn't contain special functions.
	flags := tree.RejectSpecial

	switch maxVolatility {
	case tree.VolatilityImmutable:
		flags |= tree.RejectStableOperators
		fallthrough

	case tree.VolatilityStable:
		flags |= tree.RejectVolatileFunctions

	case tree.VolatilityVolatile:
		// Allow anything (no flags needed).

	default:
		panic(errors.AssertionFailedf("maxVolatility %s not supported", maxVolatility))
	}
	semaCtx.Properties.Require(context, flags)

	typedExpr, err := tree.TypeCheck(ctx, expr, semaCtx, expectedType)
	if err != nil {
		return nil, err
	}

	actualType := typedExpr.ResolvedType()
	if !expectedType.Equivalent(actualType) && typedExpr != tree.DNull {
		// The expression must match the column type exactly unless it is a constant
		// NULL value.
		return nil, fmt.Errorf("expected %s expression to have type %s, but '%s' has type %s",
			context, expectedType, expr, actualType)
	}
	return typedExpr, nil
}

// ValidateColumnDefType returns an error if the type of a column definition is
// not valid. It is checked when a column is created or altered.
func ValidateColumnDefType(t *types.T) error {
	switch t.Family() {
	case types.StringFamily, types.CollatedStringFamily:
		if t.Family() == types.CollatedStringFamily {
			if _, err := language.Parse(t.Locale()); err != nil {
				return pgerror.Newf(pgcode.Syntax, `invalid locale %s`, t.Locale())
			}
		}

	case types.DecimalFamily:
		switch {
		case t.Precision() == 0 && t.Scale() > 0:
			// TODO (seif): Find right range for error message.
			return errors.New("invalid NUMERIC precision 0")
		case t.Precision() < t.Scale():
			return fmt.Errorf("NUMERIC scale %d must be between 0 and precision %d",
				t.Scale(), t.Precision())
		}

	case types.ArrayFamily:
		if t.ArrayContents().Family() == types.ArrayFamily {
			// Nested arrays are not supported as a column type.
			return errors.Errorf("nested array unsupported as column type: %s", t.String())
		}
		if err := types.CheckArrayElementType(t.ArrayContents()); err != nil {
			return err
		}
		return ValidateColumnDefType(t.ArrayContents())

	case types.BitFamily, types.IntFamily, types.FloatFamily, types.BoolFamily, types.BytesFamily, types.DateFamily,
		types.INetFamily, types.IntervalFamily, types.JsonFamily, types.OidFamily, types.TimeFamily,
		types.TimestampFamily, types.TimestampTZFamily, types.UuidFamily, types.TimeTZFamily,
		types.GeographyFamily, types.GeometryFamily, types.EnumFamily:
		// These types are OK.

	default:
		return pgerror.Newf(pgcode.InvalidTableDefinition,
			"value type %s cannot be used for table columns", t.String())
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
	}

	// Validate and assign column type.
	resType, err := tree.ResolveType(ctx, d.Type, semaCtx.GetTypeResolver())
	if err != nil {
		return nil, nil, nil, err
	}
	if err := ValidateColumnDefType(resType); err != nil {
		return nil, nil, nil, err
	}
	col.Type = resType

	var typedExpr tree.TypedExpr
	if d.HasDefaultExpr() {
		// Verify the default expression type is compatible with the column type
		// and does not contain invalid functions.
		var err error
		if typedExpr, err = SanitizeVarFreeExpr(
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
		s := tree.Serialize(d.Computed.Expr)
		col.ComputeExpr = &s
	}

	var idx *descpb.IndexDescriptor
	if d.PrimaryKey.IsPrimaryKey || d.Unique {
		if !d.PrimaryKey.Sharded {
			idx = &descpb.IndexDescriptor{
				Unique:           true,
				ColumnNames:      []string{string(d.Name)},
				ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
			}
		} else {
			buckets, err := EvalShardBucketCount(ctx, semaCtx, evalCtx, d.PrimaryKey.ShardBuckets)
			if err != nil {
				return nil, nil, nil, err
			}
			shardColName := GetShardColumnName([]string{string(d.Name)}, buckets)
			idx = &descpb.IndexDescriptor{
				Unique:           true,
				ColumnNames:      []string{shardColName, string(d.Name)},
				ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC, descpb.IndexDescriptor_ASC},
				Sharded: descpb.ShardedDescriptor{
					IsSharded:    true,
					Name:         shardColName,
					ShardBuckets: buckets,
					ColumnNames:  []string{string(d.Name)},
				},
			}
		}
		if d.UniqueConstraintName != "" {
			idx.Name = string(d.UniqueConstraintName)
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
	typedExpr, err := SanitizeVarFreeExpr(
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

// EncodeColumns is a version of EncodePartialIndexKey that takes ColumnIDs and
// directions explicitly. WARNING: unlike EncodePartialIndexKey, EncodeColumns
// appends directly to keyPrefix.
func EncodeColumns(
	columnIDs []descpb.ColumnID,
	directions directions,
	colMap map[descpb.ColumnID]int,
	values []tree.Datum,
	keyPrefix []byte,
) (key []byte, containsNull bool, err error) {
	key = keyPrefix
	for colIdx, id := range columnIDs {
		val := findColumnValue(id, colMap, values)
		if val == tree.DNull {
			containsNull = true
		}

		dir, err := directions.get(colIdx)
		if err != nil {
			return nil, containsNull, err
		}

		if key, err = EncodeTableKey(key, val, dir); err != nil {
			return nil, containsNull, err
		}
	}
	return key, containsNull, nil
}

// GetColumnTypes returns the types of the columns with the given IDs.
func GetColumnTypes(
	desc *ImmutableTableDescriptor, columnIDs []descpb.ColumnID,
) ([]*types.T, error) {
	types := make([]*types.T, len(columnIDs))
	for i, id := range columnIDs {
		col, err := desc.FindActiveColumnByID(id)
		if err != nil {
			return nil, err
		}
		types[i] = col.Type
	}
	return types, nil
}

type tableLookupFn func(descpb.ID) (TableDescriptor, error)

// GetConstraintInfo returns a summary of all constraints on the table.
func (desc *ImmutableTableDescriptor) GetConstraintInfo(
	ctx context.Context, txn protoGetter, codec keys.SQLCodec,
) (map[string]descpb.ConstraintDetail, error) {
	var tableLookup tableLookupFn
	if txn != nil {
		tableLookup = func(id descpb.ID) (TableDescriptor, error) {
			raw, err := getTableDescFromID(ctx, txn, codec, id)
			if err != nil {
				return nil, err
			}
			return NewImmutableTableDescriptor(*raw), nil
		}
	}
	return desc.collectConstraintInfo(tableLookup)
}

// GetConstraintInfoWithLookup returns a summary of all constraints on the
// table using the provided function to fetch a TableDescriptor from an ID.
func (desc *ImmutableTableDescriptor) GetConstraintInfoWithLookup(
	tableLookup tableLookupFn,
) (map[string]descpb.ConstraintDetail, error) {
	return desc.collectConstraintInfo(tableLookup)
}

// CheckUniqueConstraints returns a non-nil error if a descriptor contains two
// constraints with the same name.
func (desc *ImmutableTableDescriptor) CheckUniqueConstraints() error {
	_, err := desc.collectConstraintInfo(nil)
	return err
}

// if `tableLookup` is non-nil, provide a full summary of constraints, otherwise just
// check that constraints have unique names.
func (desc *ImmutableTableDescriptor) collectConstraintInfo(
	tableLookup tableLookupFn,
) (map[string]descpb.ConstraintDetail, error) {
	info := make(map[string]descpb.ConstraintDetail)

	// Indexes provide PK and Unique constraints.
	indexes := desc.AllNonDropIndexes()
	for _, index := range indexes {
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
			for _, id := range index.ColumnIDs {
				if !colHiddenMap[id] {
					hidden = false
					break
				}
			}
			if hidden {
				continue
			}
			detail := descpb.ConstraintDetail{Kind: descpb.ConstraintTypePK}
			detail.Columns = index.ColumnNames
			detail.Index = index
			info[index.Name] = detail
		} else if index.Unique {
			if _, ok := info[index.Name]; ok {
				return nil, pgerror.Newf(pgcode.DuplicateObject,
					"duplicate constraint name: %q", index.Name)
			}
			detail := descpb.ConstraintDetail{Kind: descpb.ConstraintTypeUnique}
			detail.Columns = index.ColumnNames
			detail.Index = index
			info[index.Name] = detail
		}
	}

	fks := desc.AllActiveAndInactiveForeignKeys()
	for _, fk := range fks {
		if _, ok := info[fk.Name]; ok {
			return nil, pgerror.Newf(pgcode.DuplicateObject,
				"duplicate constraint name: %q", fk.Name)
		}
		detail := descpb.ConstraintDetail{Kind: descpb.ConstraintTypeFK}
		// Constraints in the Validating state are considered Unvalidated for this purpose
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
		// Constraints in the Validating state are considered Unvalidated for this purpose
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
				col, err := desc.FindColumnByID(colID)
				if err != nil {
					return nil, errors.NewAssertionErrorWithWrappedErrf(err,
						"error finding column %d in table %s", log.Safe(colID), desc.Name)
				}
				detail.Columns = append(detail.Columns, col.Name)
			}
		}
		info[c.Name] = detail
	}
	return info, nil
}

// FindFKReferencedIndex finds the first index in the supplied referencedTable
// that can satisfy a foreign key of the supplied column ids.
func FindFKReferencedIndex(
	referencedTable TableDescriptor, referencedColIDs descpb.ColumnIDs,
) (*descpb.IndexDescriptor, error) {
	// Search for a unique index on the referenced table that matches our foreign
	// key columns.
	primaryIndex := referencedTable.GetPrimaryIndex()
	if primaryIndex.IsValidReferencedIndex(referencedColIDs) {
		return primaryIndex, nil
	}
	// If the PK doesn't match, find the index corresponding to the referenced column.
	indexes := referencedTable.GetIndexes()
	for i := range indexes {
		idx := &indexes[i]
		if idx.IsValidReferencedIndex(referencedColIDs) {
			return idx, nil
		}
	}
	return nil, pgerror.Newf(
		pgcode.ForeignKeyViolation,
		"there is no unique constraint matching given keys for referenced table %s",
		referencedTable.GetName(),
	)
}

// FindFKOriginIndex finds the first index in the supplied originTable
// that can satisfy an outgoing foreign key of the supplied column ids.
func FindFKOriginIndex(
	originTable TableDescriptor, originColIDs descpb.ColumnIDs,
) (*descpb.IndexDescriptor, error) {
	// Search for an index on the origin table that matches our foreign
	// key columns.
	if primaryIndex := originTable.GetPrimaryIndex(); primaryIndex.IsValidOriginIndex(originColIDs) {
		return primaryIndex, nil
	}
	// If the PK doesn't match, find the index corresponding to the origin column.
	indexes := originTable.GetIndexes()
	for i := range indexes {
		idx := &indexes[i]
		if idx.IsValidOriginIndex(originColIDs) {
			return idx, nil
		}
	}
	return nil, pgerror.Newf(
		pgcode.ForeignKeyViolation,
		"there is no index matching given keys for referenced table %s",
		originTable.GetName(),
	)
}

// FindFKOriginIndexInTxn finds the first index in the supplied originTable
// that can satisfy an outgoing foreign key of the supplied column ids.
// It returns either an index that is active, or an index that was created
// in the same transaction that is currently running.
func FindFKOriginIndexInTxn(
	originTable *MutableTableDescriptor, originColIDs descpb.ColumnIDs,
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
) MutableTableDescriptor {
	return MutableTableDescriptor{
		ImmutableTableDescriptor: ImmutableTableDescriptor{
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

// NewMutableTableDescriptorAsReplacement creates a new MutableTableDescriptor
// as a replacement of an existing table. This is utilized with truncate.
//
// The passed readTimestamp is serialized into the descriptor's ReplacementOf
// field for debugging purposes. The passed id will be the ID of the newly
// returned replacement.
//
// TODO(ajwerner): Decide whether we need to clone here.
func NewMutableTableDescriptorAsReplacement(
	id descpb.ID, replacementOf *MutableTableDescriptor, readTimestamp hlc.Timestamp,
) *MutableTableDescriptor {
	replacement := &MutableTableDescriptor{ImmutableTableDescriptor: replacementOf.ImmutableTableDescriptor}
	replacement.ID = id
	replacement.Version = 1
	replacement.ReplacementOf = descpb.TableDescriptor_Replacement{
		ID:   replacementOf.ID,
		Time: readTimestamp,
	}
	return replacement
}
