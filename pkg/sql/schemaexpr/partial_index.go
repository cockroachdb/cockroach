// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemaexpr

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// IndexPredicateValidator validates that an expression is a valid partial index
// predicate. See Validate for more details.
type IndexPredicateValidator struct {
	ctx       context.Context
	tableName tree.TableName
	desc      sqlbase.TableDescriptor
	semaCtx   *tree.SemaContext
}

// MakeIndexPredicateValidator returns an IndexPredicateValidator struct that
// can be used to validate partial index predicates. See Validate for more
// details.
func MakeIndexPredicateValidator(
	ctx context.Context,
	tableName tree.TableName,
	desc sqlbase.TableDescriptor,
	semaCtx *tree.SemaContext,
) IndexPredicateValidator {
	return IndexPredicateValidator{
		ctx:       ctx,
		tableName: tableName,
		desc:      desc,
		semaCtx:   semaCtx,
	}
}

// Validate verifies that an expression is a valid partial index predicate. If
// the expression is valid, it returns the serialized expression with the
// columns dequalified.
//
// A predicate expression is valid if all of the following are true:
//
//   - It results in a boolean.
//   - It refers only to columns in the table.
//   - It does not include subqueries.
//   - It does not include non-immutable, aggregate, window, or set returning
//     functions.
//
func (v *IndexPredicateValidator) Validate(e tree.Expr) (string, error) {
	expr, _, err := DequalifyAndValidateExpr(
		v.ctx,
		v.desc,
		e,
		types.Bool,
		"index predicate",
		v.semaCtx,
		tree.VolatilityImmutable,
		&v.tableName,
	)
	if err != nil {
		return "", err
	}

	return expr, nil
}

// FormatIndexForDisplay formats a column descriptor as a SQL string. It
// converts user defined types in partial index predicate expressions to a
// human-readable form.
//
// If tableName is anonymous then no table name is included in the formatted
// string. For example:
//
//   INDEX i (a) WHERE b > 0
//
// If tableName is not anonymous, then "ON" and the name is included:
//
//   INDEX i ON t (a) WHERE b > 0
//
func FormatIndexForDisplay(
	ctx context.Context,
	table sqlbase.TableDescriptor,
	tableName *tree.TableName,
	index *descpb.IndexDescriptor,
	semaCtx *tree.SemaContext,
) (string, error) {
	f := tree.NewFmtCtx(tree.FmtSimple)
	if index.Unique {
		f.WriteString("UNIQUE ")
	}
	if index.Type == descpb.IndexDescriptor_INVERTED {
		f.WriteString("INVERTED ")
	}
	f.WriteString("INDEX ")
	f.FormatNameP(&index.Name)
	if *tableName != descpb.AnonymousTable {
		f.WriteString(" ON ")
		f.FormatNode(tableName)
	}
	f.WriteString(" (")
	index.ColNamesFormat(f)
	f.WriteByte(')')

	if index.IsSharded() {
		fmt.Fprintf(f, " USING HASH WITH BUCKET_COUNT = %v",
			index.Sharded.ShardBuckets)
	}

	if len(index.StoreColumnNames) > 0 {
		f.WriteString(" STORING (")
		for i := range index.StoreColumnNames {
			if i > 0 {
				f.WriteString(", ")
			}
			f.FormatNameP(&index.StoreColumnNames[i])
		}
		f.WriteByte(')')
	}

	if index.IsPartial() {
		f.WriteString(" WHERE ")
		pred, err := FormatExprForDisplay(ctx, table, index.Predicate, semaCtx)
		if err != nil {
			return "", err
		}
		f.WriteString(pred)
	}

	return f.CloseAndGetString(), nil
}

// MakePartialIndexExprs returns a map of predicate expressions for each
// partial index in the input list of indexes, or nil if none of the indexes
// are partial indexes. It also returns a set of all column IDs referenced in
// the expressions.
//
// If the expressions are being built within the context of an index add
// mutation in a transaction, the cols argument must include mutation columns
// that are added previously in the same transaction.
func MakePartialIndexExprs(
	ctx context.Context,
	indexes []descpb.IndexDescriptor,
	cols []descpb.ColumnDescriptor,
	tableDesc *sqlbase.ImmutableTableDescriptor,
	evalCtx *tree.EvalContext,
	semaCtx *tree.SemaContext,
) (_ map[descpb.IndexID]tree.TypedExpr, refColIDs sqlbase.TableColSet, _ error) {
	// If none of the indexes are partial indexes, return early.
	partialIndexCount := 0
	for i := range indexes {
		if indexes[i].IsPartial() {
			partialIndexCount++
		}
	}
	if partialIndexCount == 0 {
		return nil, refColIDs, nil
	}

	exprs := make(map[descpb.IndexID]tree.TypedExpr, partialIndexCount)

	tn := tree.NewUnqualifiedTableName(tree.Name(tableDesc.Name))
	nr := newNameResolver(evalCtx, tableDesc.ID, tn, columnDescriptorsToPtrs(cols))
	nr.addIVarContainerToSemaCtx(semaCtx)

	var txCtx transform.ExprTransformContext
	for i := range indexes {
		idx := &indexes[i]
		if idx.IsPartial() {
			expr, err := parser.ParseExpr(idx.Predicate)
			if err != nil {
				return nil, refColIDs, err
			}

			// Collect all column IDs that are referenced in the partial index
			// predicate expression.
			colIDs, err := ExtractColumnIDs(tableDesc, expr)
			if err != nil {
				return nil, refColIDs, err
			}
			refColIDs.UnionWith(colIDs)

			expr, err = nr.resolveNames(expr)
			if err != nil {
				return nil, refColIDs, err
			}

			typedExpr, err := tree.TypeCheck(ctx, expr, semaCtx, types.Bool)
			if err != nil {
				return nil, refColIDs, err
			}

			if typedExpr, err = txCtx.NormalizeExpr(evalCtx, typedExpr); err != nil {
				return nil, refColIDs, err
			}

			exprs[idx.ID] = typedExpr
		}
	}

	return exprs, refColIDs, nil
}
