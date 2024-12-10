// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemaexpr

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// ValidatePartialIndexPredicate verifies that an expression is a valid partial
// index predicate. If the expression is valid, it returns the serialized
// expression with the columns dequalified.
//
// A predicate expression is valid if all of the following are true:
//
//   - It results in a boolean.
//   - It refers only to columns in the table.
//   - It does not include subqueries.
//   - It does not include non-immutable, aggregate, window, or set returning
//     functions.
//   - It does not reference a column which is in the process of being added
//     or removed.
func ValidatePartialIndexPredicate(
	ctx context.Context,
	desc catalog.TableDescriptor,
	e tree.Expr,
	tn *tree.TableName,
	semaCtx *tree.SemaContext,
	version clusterversion.ClusterVersion,
) (string, error) {
	expr, _, cols, err := DequalifyAndValidateExpr(
		ctx,
		desc,
		e,
		types.Bool,
		tree.IndexPredicateExpr,
		semaCtx,
		volatility.Immutable,
		tn,
		version,
	)
	if err != nil {
		return "", err
	}
	if mutDesc, ok := desc.(catalog.MutableTableDescriptor); !ok || !mutDesc.IsNew() {
		if err := validatePartialIndexExprColsArePublic(desc, cols); err != nil {
			return "", err
		}
	}
	return expr, nil
}

func validatePartialIndexExprColsArePublic(
	desc catalog.TableDescriptor, cols catalog.TableColSet,
) (err error) {
	cols.ForEach(func(colID descpb.ColumnID) {
		if err != nil {
			return
		}
		var col catalog.Column
		col, err = catalog.MustFindColumnByID(desc, colID)
		if err != nil {
			return
		}
		if col.Public() {
			return
		}
		err = pgerror.WithCandidateCode(errors.Errorf(
			"cannot create partial index on column %q (%d) which is not public",
			col.GetName(), col.GetID()),
			pgcode.FeatureNotSupported,
		)
	})
	return err
}

// MakePartialIndexExpr is like MakePartialIndexExprs but for a single partial
// index using all public columns of the table. It returns an error if the
// passed index is not a partial index.
func MakePartialIndexExpr(
	ctx context.Context,
	table catalog.TableDescriptor,
	index catalog.Index,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
) (tree.TypedExpr, error) {
	if index == nil || !index.IsPartial() {
		return nil, errors.AssertionFailedf("%v of %v is not a partial index", index, table)
	}
	h := makePartialIndexHelper(table, table.PublicColumns(), evalCtx, semaCtx)
	expr, _, err := h.makePartialIndexExpr(ctx, index)
	if err != nil {
		return nil, err
	}
	return expr, nil
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
	indexes []catalog.Index,
	cols []catalog.Column,
	tableDesc catalog.TableDescriptor,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
) (_ map[descpb.IndexID]tree.TypedExpr, refColIDs catalog.TableColSet, _ error) {
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
	h := makePartialIndexHelper(tableDesc, cols, evalCtx, semaCtx)
	for _, idx := range indexes {
		if idx.IsPartial() {
			typedExpr, colIDs, err := h.makePartialIndexExpr(ctx, idx)
			if err != nil {
				return nil, catalog.TableColSet{}, err
			}
			refColIDs.UnionWith(colIDs)
			exprs[idx.GetID()] = typedExpr
		}
	}

	return exprs, refColIDs, nil
}

// partialIndexHelper is used to parse, type-check, and resolve partial
// index predicates for a table.
type partialIndexHelper struct {
	nr        *nameResolver
	evalCtx   *eval.Context
	semaCtx   *tree.SemaContext
	tableDesc catalog.TableDescriptor
}

func makePartialIndexHelper(
	table catalog.TableDescriptor,
	cols []catalog.Column,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
) partialIndexHelper {
	tn := tree.NewUnqualifiedTableName(tree.Name(table.GetName()))
	nr := newNameResolver(table.GetID(), tn, cols)
	nr.addIVarContainerToSemaCtx(semaCtx)
	return partialIndexHelper{
		nr:        nr,
		evalCtx:   evalCtx,
		semaCtx:   semaCtx,
		tableDesc: table,
	}
}

// makePartialIndexExpr turns an index's partial index predicate from a string to
// a TypedExpr.
func (pi partialIndexHelper) makePartialIndexExpr(
	ctx context.Context, idx catalog.Index,
) (tree.TypedExpr, catalog.TableColSet, error) {
	expr, err := parser.ParseExpr(idx.GetPredicate())
	if err != nil {
		return nil, catalog.TableColSet{}, err
	}

	// Collect all column IDs that are referenced in the partial index
	// predicate expression.
	colIDs, err := ExtractColumnIDs(pi.tableDesc, expr)
	if err != nil {
		return nil, catalog.TableColSet{}, err
	}

	expr, err = pi.nr.resolveNames(expr)
	if err != nil {
		return nil, catalog.TableColSet{}, err
	}

	typedExpr, err := tree.TypeCheck(ctx, expr, pi.semaCtx, types.Bool)
	if err != nil {
		return nil, catalog.TableColSet{}, err
	}
	var txCtx transform.ExprTransformContext
	if typedExpr, err = txCtx.NormalizeExpr(ctx, pi.evalCtx, typedExpr); err != nil {
		return nil, catalog.TableColSet{}, err
	}
	return typedExpr, colIDs, nil
}
