// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package partitionccl

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/pkg/errors"
)

// valueEncodePartitionTuple typechecks the datums in maybeTuple. It returns the
// concatenation of these datums, each encoded using the table "value" encoding.
// The special values of DEFAULT (for list) and MAXVALUE (for range) are encoded
// as NOT NULL.
//
// TODO(dan): The typechecking here should be run during plan construction, so
// we can support placeholders.
func valueEncodePartitionTuple(
	typ tree.PartitionByType,
	evalCtx *tree.EvalContext,
	maybeTuple tree.Expr,
	cols []sqlbase.ColumnDescriptor,
) ([]byte, error) {
	tuple, ok := maybeTuple.(*tree.Tuple)
	if !ok {
		// If we don't already have a tuple, promote whatever we have to a 1-tuple.
		tuple = &tree.Tuple{Exprs: []tree.Expr{maybeTuple}}
	}

	if len(tuple.Exprs) != len(cols) {
		return nil, errors.Errorf("partition has %d columns but %d values were supplied",
			len(cols), len(tuple.Exprs))
	}

	var value, scratch []byte
	for i, expr := range tuple.Exprs {
		expr = tree.StripParens(expr)
		switch expr.(type) {
		case tree.DefaultVal:
			if typ != tree.PartitionByList {
				return nil, errors.Errorf("%s cannot be used with PARTITION BY %s", expr, typ)
			}
			// NOT NULL is used to signal that a PartitionSpecialValCode follows.
			value = encoding.EncodeNotNullValue(value, encoding.NoColumnID)
			value = encoding.EncodeNonsortingUvarint(value, uint64(sqlbase.PartitionDefaultVal))
			continue
		case tree.MinVal:
			if typ != tree.PartitionByRange {
				return nil, errors.Errorf("%s cannot be used with PARTITION BY %s", expr, typ)
			}
			// NOT NULL is used to signal that a PartitionSpecialValCode follows.
			value = encoding.EncodeNotNullValue(value, encoding.NoColumnID)
			value = encoding.EncodeNonsortingUvarint(value, uint64(sqlbase.PartitionMinVal))
			continue
		case tree.MaxVal:
			if typ != tree.PartitionByRange {
				return nil, errors.Errorf("%s cannot be used with PARTITION BY %s", expr, typ)
			}
			// NOT NULL is used to signal that a PartitionSpecialValCode follows.
			value = encoding.EncodeNotNullValue(value, encoding.NoColumnID)
			value = encoding.EncodeNonsortingUvarint(value, uint64(sqlbase.PartitionMaxVal))
			continue
		case *tree.Placeholder:
			return nil, pgerror.UnimplementedWithIssueErrorf(
				19464, "placeholders are not supported in PARTITION BY")
		default:
			// Fall-through.
		}

		typedExpr, err := sqlbase.SanitizeVarFreeExpr(expr, cols[i].Type.ToDatumType(), "partition",
			nil /* semaCtx */, evalCtx)
		if err != nil {
			return nil, err
		}
		if !tree.IsConst(evalCtx, typedExpr) {
			return nil, fmt.Errorf("%s: partition values must be constant", typedExpr)
		}
		datum, err := typedExpr.Eval(evalCtx)
		if err != nil {
			return nil, errors.Wrap(err, typedExpr.String())
		}
		if err := sqlbase.CheckColumnType(cols[i], datum.ResolvedType(), nil); err != nil {
			return nil, err
		}
		value, err = sqlbase.EncodeTableValue(
			value, sqlbase.ColumnID(encoding.NoColumnID), datum, scratch,
		)
		if err != nil {
			return nil, err
		}
	}
	return value, nil
}

func createPartitioningImpl(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	tableDesc *sqlbase.TableDescriptor,
	indexDesc *sqlbase.IndexDescriptor,
	partBy *tree.PartitionBy,
	colOffset int,
) (sqlbase.PartitioningDescriptor, error) {
	partDesc := sqlbase.PartitioningDescriptor{}
	if partBy == nil {
		return partDesc, nil
	}
	partDesc.NumColumns = uint32(len(partBy.Fields))

	partitioningString := func() string {
		// We don't have the fields for our parent partitions handy, but we can use
		// the names from the index we're partitioning. They must have matched or we
		// would have already returned an error.
		partCols := append([]string(nil), indexDesc.ColumnNames[:colOffset]...)
		for _, p := range partBy.Fields {
			partCols = append(partCols, string(p))
		}
		return strings.Join(partCols, ", ")
	}

	var cols []sqlbase.ColumnDescriptor
	for i := 0; i < len(partBy.Fields); i++ {
		if colOffset+i >= len(indexDesc.ColumnNames) {
			return partDesc, fmt.Errorf(
				"declared partition columns (%s) exceed the number of columns in index being partitioned (%s)",
				partitioningString(), strings.Join(indexDesc.ColumnNames, ", "))
		}
		// Search by name because some callsites of this method have not
		// allocated ids yet (so they are still all the 0 value).
		col, err := tableDesc.FindActiveColumnByName(indexDesc.ColumnNames[colOffset+i])
		if err != nil {
			return partDesc, err
		}
		cols = append(cols, col)
		if string(partBy.Fields[i]) != col.Name {
			n := colOffset + len(partBy.Fields)
			return partDesc, fmt.Errorf(
				"declared partition columns (%s) do not match first %d columns in index being partitioned (%s)",
				partitioningString(), n, strings.Join(indexDesc.ColumnNames[:n], ", "))
		}
	}

	for _, l := range partBy.List {
		p := sqlbase.PartitioningDescriptor_List{
			Name: string(l.Name),
		}
		for _, expr := range l.Exprs {
			encodedTuple, err := valueEncodePartitionTuple(
				tree.PartitionByList, evalCtx, expr, cols)
			if err != nil {
				return partDesc, errors.Wrapf(err, "PARTITION %s", p.Name)
			}
			p.Values = append(p.Values, encodedTuple)
		}
		if l.Subpartition != nil {
			newColOffset := colOffset + int(partDesc.NumColumns)
			subpartitioning, err := createPartitioningImpl(
				ctx, evalCtx, tableDesc, indexDesc, l.Subpartition, newColOffset)
			if err != nil {
				return partDesc, err
			}
			p.Subpartitioning = subpartitioning
		}
		partDesc.List = append(partDesc.List, p)
	}

	for _, r := range partBy.Range {
		p := sqlbase.PartitioningDescriptor_Range{
			Name: string(r.Name),
		}
		var err error
		p.FromInclusive, err = valueEncodePartitionTuple(
			tree.PartitionByRange, evalCtx, r.From, cols)
		if err != nil {
			return partDesc, errors.Wrapf(err, "PARTITION %s", p.Name)
		}
		p.ToExclusive, err = valueEncodePartitionTuple(
			tree.PartitionByRange, evalCtx, r.To, cols)
		if err != nil {
			return partDesc, errors.Wrapf(err, "PARTITION %s", p.Name)
		}
		if r.Subpartition != nil {
			return partDesc, errors.Errorf("PARTITION %s: cannot subpartition a range partition", p.Name)
		}
		partDesc.Range = append(partDesc.Range, p)
	}

	return partDesc, nil
}

// createPartitioning constructs the partitioning descriptor for an index that
// is partitioned into ranges, each addressable by zone configs.
func createPartitioning(
	ctx context.Context,
	st *cluster.Settings,
	evalCtx *tree.EvalContext,
	tableDesc *sqlbase.TableDescriptor,
	indexDesc *sqlbase.IndexDescriptor,
	partBy *tree.PartitionBy,
) (sqlbase.PartitioningDescriptor, error) {
	if !st.Version.IsMinSupported(cluster.VersionPartitioning) {
		return sqlbase.PartitioningDescriptor{},
			errors.New("cluster version does not support partitioning")
	}

	org := sql.ClusterOrganization.Get(&st.SV)
	if err := utilccl.CheckEnterpriseEnabled(st, evalCtx.ClusterID, org, "partitions"); err != nil {
		return sqlbase.PartitioningDescriptor{}, err
	}

	return createPartitioningImpl(
		ctx, evalCtx, tableDesc, indexDesc, partBy, 0 /* colOffset */)
}

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
				t, _, err := sqlbase.DecodePartitionTuple(
					a, tableDesc, idxDesc, partDesc, valueEncBuf, prefixDatums)
				if err != nil {
					return err
				}
				allDatums := append(prefixDatums, t.Datums...)

				// When len(allDatums) < len(colVars), the missing elements are DEFAULTs, so
				// we can simply exclude them from the expr.
				partValueExpr := tree.NewTypedComparisonExpr(tree.EQ,
					tree.NewTypedTuple(colVars[:len(allDatums)]), tree.NewDTuple(allDatums...))
				partValueExprs[len(t.Datums)] = append(partValueExprs[len(t.Datums)], exprAndPartName{
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

func init() {
	sql.CreatePartitioningCCL = createPartitioning
}
