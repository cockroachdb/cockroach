// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlccl

import (
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

// selectPartitionExprs constructs an expression for selecting all rows in the
// given partitions.
func selectPartitionExprs(
	evalCtx *tree.EvalContext, tableDesc *sqlbase.TableDescriptor, partNames tree.NameList,
) (tree.Expr, error) {
	exprsByPartName := make(map[string]tree.TypedExpr)
	for _, partName := range partNames {
		exprsByPartName[string(partName)] = nil
	}

	a := &sqlbase.DatumAlloc{}
	var prefixDatums []tree.Datum
	if err := tableDesc.ForeachNonDropIndex(func(idxDesc *sqlbase.IndexDescriptor) error {
		genExpr := true
		return selectPartitionExprsByName(
			a, tableDesc, idxDesc, &idxDesc.Partitioning, prefixDatums, exprsByPartName, genExpr)
	}); err != nil {
		return nil, err
	}

	var expr tree.TypedExpr = tree.DBoolFalse
	for _, partName := range partNames {
		partExpr, ok := exprsByPartName[string(partName)]
		if !ok || partExpr == nil {
			return nil, errors.Errorf("unknown partition: %s", partName)
		}
		expr = tree.NewTypedOrExpr(expr, partExpr)
	}

	var err error
	expr, err = evalCtx.NormalizeExpr(expr)
	if err != nil {
		return nil, err
	}
	if e, equiv := sql.SimplifyExpr(evalCtx, expr); equiv {
		expr = e
	}
	// In order to typecheck during simplification and normalization, we used
	// dummy IndexVars. Swap them out for actual column references.
	finalExpr, err := tree.SimpleVisit(expr, func(e tree.Expr) (error, bool, tree.Expr) {
		if ivar, ok := e.(*tree.IndexedVar); ok {
			col, err := tableDesc.FindColumnByID(sqlbase.ColumnID(ivar.Idx))
			if err != nil {
				return err, false, nil
			}
			return nil, false, &tree.ColumnItem{ColumnName: tree.Name(col.Name)}
		}
		return nil, true, e
	})
	return finalExpr, err
}

// selectPartitionExprsByName constructs an expression for selecting all rows in
// each partition and subpartition in the given index. To make it easy to
// simplify and normalize the exprs, references to table columns are represented
// as TypedOrdinalReferences with an ordinal of the column ID.
//
// NB Subpartitions do not affect the expression for their parent partitions. So
// if a partition foo (a=3) is then subpartitiond by (b=5) and no DEFAULT, the
// expression for foo is still `a=3`, not `a=3 AND b=5`. This means that if some
// partition is requested, we can omit all of the subpartitions, because they'll
// also necessarily select subsets of the rows it will. "requested" here is
// indicated by the caller by setting the corresponding name in the
// `exprsByPartName` map to nil. In this case, `genExpr` is then set to false
// for subpartitions of this call, which causes each subpartition to only
// register itself in the map with a placeholder entry (so we can still verify
// that the requested partitions are all valid).
func selectPartitionExprsByName(
	a *sqlbase.DatumAlloc,
	tableDesc *sqlbase.TableDescriptor,
	idxDesc *sqlbase.IndexDescriptor,
	partDesc *sqlbase.PartitioningDescriptor,
	prefixDatums tree.Datums,
	exprsByPartName map[string]tree.TypedExpr,
	genExpr bool,
) error {
	if partDesc.NumColumns == 0 {
		return nil
	}

	// Setting genExpr to false skips the expression generation and only
	// registers each descendent partition in the map with a placeholder entry.
	if !genExpr {
		for _, l := range partDesc.List {
			exprsByPartName[l.Name] = tree.DBoolFalse
			var fakeDatums tree.Datums
			if err := selectPartitionExprsByName(
				a, tableDesc, idxDesc, &l.Subpartitioning, fakeDatums, exprsByPartName, genExpr,
			); err != nil {
				return err
			}
		}
		for _, r := range partDesc.Range {
			exprsByPartName[r.Name] = tree.DBoolFalse
		}
		return nil
	}

	var colVars tree.TypedExprs
	{
		// The recursive calls of selectPartitionExprsByName don't pass though
		// the column ordinal references, so reconstruct them here.
		colVars = make(tree.TypedExprs, len(prefixDatums)+int(partDesc.NumColumns))
		for i := range colVars {
			col, err := tableDesc.FindActiveColumnByID(idxDesc.ColumnIDs[i])
			if err != nil {
				return err
			}
			colVars[i] = tree.NewTypedOrdinalReference(int(col.ID), col.Type.ToDatumType())
		}
	}

	if len(partDesc.List) > 0 {
		type exprAndPartName struct {
			expr tree.TypedExpr
			name string
		}
		// Any partitions using DEFAULT must specifically exclude any relevant
		// higher specificity partitions (e.g for partitions `(1, DEFAULT)`,
		// `(1, 2)`, the expr for the former must exclude the latter. This is
		// done by bucketing the expression for each partition value by the
		// number of DEFAULTs it involves.
		partValueExprs := make([][]exprAndPartName, int(partDesc.NumColumns)+1)

		for _, l := range partDesc.List {
			for _, valueEncBuf := range l.Values {
				datums, _, err := sqlbase.TranslateValueEncodingToSpan(
					a, tableDesc, idxDesc, partDesc, valueEncBuf, prefixDatums)
				if err != nil {
					return err
				}
				allDatums := append(prefixDatums, datums...)

				// When len(allDatums) < len(colVars), the missing elements are DEFAULTs, so
				// we can simply exclude them from the expr.
				partValueExpr := tree.NewTypedComparisonExpr(tree.EQ,
					tree.NewTypedTuple(colVars[:len(allDatums)]), tree.NewDTuple(allDatums...))
				partValueExprs[len(datums)] = append(partValueExprs[len(datums)], exprAndPartName{
					expr: partValueExpr,
					name: l.Name,
				})

				genExpr := true
				if _, ok := exprsByPartName[l.Name]; ok {
					// Presence of a partition name in the exprsByPartName map
					// means the caller has expressed an interested in this
					// partition, which means any subpartitions can be skipped
					// (because they must by definition be a subset of this
					// partition). This saves us a little work and also helps
					// out the normalization & simplification of the resulting
					// expression, since it doesn't have to account for which
					// partitions overlap.
					genExpr = false
				}
				if err := selectPartitionExprsByName(
					a, tableDesc, idxDesc, &l.Subpartitioning, allDatums, exprsByPartName, genExpr,
				); err != nil {
					return err
				}
			}
		}

		// Walk backward through partValueExprs, so partition values with fewest
		// DEFAULTs to most. As we go, keep an expression to be AND NOT'd with
		// each partition value's expression in `excludeExpr`. This handles the
		// exclusion of `(1, 2)` from the expression for `(1, DEFAULT)` in the
		// example above.
		//
		// TODO(dan): The result of the way this currently works is correct but
		// too broad. In a two column partitioning with cases for `(a, b)` and
		// `(c, DEFAULT)`, the expression generated for `(c, DEFAULT)` will
		// needlessly exclude `(a, b)`. Concretely, we end up with expressions
		// like `(a) IN (1) AND ... (a, b) != (2, 3)`, where the `!= (2, 3)`
		// part is irrelevant. This only happens in fairly unrealistic
		// partitionings, so it's unclear if anything really needs to be done
		// here.
		excludeExpr := tree.TypedExpr(tree.DBoolFalse)
		for i := len(partValueExprs) - 1; i >= 0; i-- {
			nextExcludeExpr := tree.TypedExpr(tree.DBoolFalse)
			for _, v := range partValueExprs[i] {
				nextExcludeExpr = tree.NewTypedOrExpr(nextExcludeExpr, v.expr)
				partValueExpr := tree.NewTypedAndExpr(v.expr, tree.NewTypedNotExpr(excludeExpr))
				// We can get multiple expressions for the same partition in
				// a single-col `PARTITION foo VALUES IN ((1), (2))`.
				if e, ok := exprsByPartName[v.name]; !ok || e == nil {
					exprsByPartName[v.name] = partValueExpr
				} else {
					exprsByPartName[v.name] = tree.NewTypedOrExpr(e, partValueExpr)
				}
			}
			excludeExpr = tree.NewTypedOrExpr(excludeExpr, nextExcludeExpr)
		}
	}

	for range partDesc.Range {
		return errors.New("TODO(dan): unsupported for range partitionings")
	}

	return nil
}
