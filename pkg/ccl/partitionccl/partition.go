// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package partitionccl

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scdeps"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// valueEncodePartitionTuple typechecks the datums in maybeTuple. It returns the
// concatenation of these datums, each encoded using the table "value" encoding.
// The special values of DEFAULT (for list) and MAXVALUE (for range) are encoded
// as NOT NULL.
//
// TODO(dan): The typechecking here should be run during plan construction, so
// we can support placeholders.
func valueEncodePartitionTuple(
	ctx context.Context,
	typ tree.PartitionByType,
	evalCtx *eval.Context,
	maybeTuple tree.Expr,
	cols []catalog.Column,
) ([]byte, error) {
	// Replace any occurrences of the MINVALUE/MAXVALUE pseudo-names
	// into MinVal and MaxVal, to be recognized below.
	// We are operating in a context where the expressions cannot
	// refer to table columns, so these two names are unambiguously
	// referring to the desired partition boundaries.
	maybeTuple, _ = tree.WalkExpr(zonepb.ReplaceMinMaxValVisitor{}, maybeTuple)

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
			value = encoding.EncodeNonsortingUvarint(value, uint64(rowenc.PartitionDefaultVal))
			continue
		case tree.PartitionMinVal:
			if typ != tree.PartitionByRange {
				return nil, errors.Errorf("%s cannot be used with PARTITION BY %s", expr, typ)
			}
			// NOT NULL is used to signal that a PartitionSpecialValCode follows.
			value = encoding.EncodeNotNullValue(value, encoding.NoColumnID)
			value = encoding.EncodeNonsortingUvarint(value, uint64(rowenc.PartitionMinVal))
			continue
		case tree.PartitionMaxVal:
			if typ != tree.PartitionByRange {
				return nil, errors.Errorf("%s cannot be used with PARTITION BY %s", expr, typ)
			}
			// NOT NULL is used to signal that a PartitionSpecialValCode follows.
			value = encoding.EncodeNotNullValue(value, encoding.NoColumnID)
			value = encoding.EncodeNonsortingUvarint(value, uint64(rowenc.PartitionMaxVal))
			continue
		case *tree.Placeholder:
			return nil, unimplemented.NewWithIssuef(
				19464, "placeholders are not supported in PARTITION BY")
		default:
			// Fall-through.
		}

		var semaCtx tree.SemaContext
		colTyp := cols[i].GetType()
		typedExpr, err := schemaexpr.SanitizeVarFreeExpr(ctx, expr, colTyp, "partition",
			&semaCtx,
			volatility.Immutable,
			false, /*allowAssignmentCast*/
		)
		if err != nil {
			return nil, err
		}
		if !eval.IsConst(evalCtx, typedExpr) {
			return nil, pgerror.Newf(pgcode.Syntax,
				"%s: partition values must be constant", typedExpr)
		}
		datum, err := eval.Expr(ctx, evalCtx, typedExpr)
		if err != nil {
			return nil, errors.Wrapf(err, "evaluating %s", typedExpr)
		}
		err = colinfo.CheckDatumTypeFitsColumnType(cols[i].GetName(), colTyp, datum.ResolvedType())
		if err != nil {
			return nil, err
		}
		value, scratch, err = valueside.EncodeWithScratch(value, valueside.NoColumnID, datum, scratch[:0])
		if err != nil {
			return nil, err
		}
	}
	return value, nil
}

func createPartitioningImpl(
	ctx context.Context,
	evalCtx *eval.Context,
	columnLookupFn func(tree.Name) (catalog.Column, error),
	newIdxColumnNames []string,
	partBy *tree.PartitionBy,
	allowedNewColumnNames []tree.Name,
	numImplicitColumns int,
	colOffset int,
) (catpb.PartitioningDescriptor, error) {
	partDesc := catpb.PartitioningDescriptor{}
	if partBy == nil {
		return partDesc, nil
	}
	partDesc.NumColumns = uint32(len(partBy.Fields))
	partDesc.NumImplicitColumns = uint32(numImplicitColumns)

	partitioningString := func() string {
		// We don't have the fields for our parent partitions handy, but we can use
		// the names from the index we're partitioning. They must have matched or we
		// would have already returned an error.
		partCols := append([]string(nil), newIdxColumnNames[:colOffset]...)
		for _, p := range partBy.Fields {
			partCols = append(partCols, string(p))
		}
		return strings.Join(partCols, ", ")
	}

	var cols []catalog.Column
	for i := 0; i < len(partBy.Fields); i++ {
		if colOffset+i >= len(newIdxColumnNames) {
			return partDesc, pgerror.Newf(pgcode.Syntax,
				"declared partition columns (%s) exceed the number of columns in index being partitioned (%s)",
				partitioningString(), strings.Join(newIdxColumnNames, ", "))
		}
		// Search by name because some callsites of this method have not
		// allocated ids yet (so they are still all the 0 value).
		col, err := findColumnByNameOnTable(
			columnLookupFn,
			tree.Name(newIdxColumnNames[colOffset+i]),
			allowedNewColumnNames,
		)
		if err != nil {
			return partDesc, err
		}
		cols = append(cols, col)
		if string(partBy.Fields[i]) != col.GetName() {
			// This used to print the first `colOffset + len(partBy.Fields)` fields
			// but there might not be this many columns in the index. See #37682.
			n := colOffset + i + 1
			return partDesc, pgerror.Newf(pgcode.Syntax,
				"declared partition columns (%s) do not match first %d columns in index being partitioned (%s)",
				partitioningString(), n, strings.Join(newIdxColumnNames[:n], ", "))
		}
		switch col.GetType().Family() {
		case types.ArrayFamily:
			return partDesc, unimplemented.NewWithIssuef(91766,
				"partitioning by array column (%s) not supported", col.GetName())

		case types.PGVectorFamily:
			// Can't partition by a column that does not have linear ordering.
			return partDesc, pgerror.Newf(pgcode.FeatureNotSupported,
				"partitioning by vector column (%s) not supported", col.GetName())
		}
	}

	for _, l := range partBy.List {
		p := catpb.PartitioningDescriptor_List{
			Name: string(l.Name),
		}
		for _, expr := range l.Exprs {
			encodedTuple, err := valueEncodePartitionTuple(
				ctx, tree.PartitionByList, evalCtx, expr, cols,
			)
			if err != nil {
				return partDesc, errors.Wrapf(err, "PARTITION %s", p.Name)
			}
			p.Values = append(p.Values, encodedTuple)
		}
		if l.Subpartition != nil {
			newColOffset := colOffset + int(partDesc.NumColumns)
			if numImplicitColumns > 0 {
				return catpb.PartitioningDescriptor{}, unimplemented.New(
					"PARTITION BY SUBPARTITION",
					"implicit column partitioning on a subpartition is not yet supported",
				)
			}
			subpartitioning, err := createPartitioningImpl(
				ctx,
				evalCtx,
				columnLookupFn,
				newIdxColumnNames,
				l.Subpartition,
				allowedNewColumnNames,
				0, /* implicitColumnNames */
				newColOffset,
			)
			if err != nil {
				return partDesc, err
			}
			p.Subpartitioning = subpartitioning
		}
		partDesc.List = append(partDesc.List, p)
	}

	for _, r := range partBy.Range {
		p := catpb.PartitioningDescriptor_Range{
			Name: string(r.Name),
		}
		var err error
		p.FromInclusive, err = valueEncodePartitionTuple(
			ctx, tree.PartitionByRange, evalCtx, &tree.Tuple{Exprs: r.From}, cols,
		)
		if err != nil {
			return partDesc, errors.Wrapf(err, "PARTITION %s", p.Name)
		}
		p.ToExclusive, err = valueEncodePartitionTuple(
			ctx, tree.PartitionByRange, evalCtx, &tree.Tuple{Exprs: r.To}, cols,
		)
		if err != nil {
			return partDesc, errors.Wrapf(err, "PARTITION %s", p.Name)
		}
		if r.Subpartition != nil {
			return partDesc, errors.Newf("PARTITION %s: cannot subpartition a range partition", p.Name)
		}
		partDesc.Range = append(partDesc.Range, p)
	}

	return partDesc, nil
}

// collectImplicitPartitionColumns collects implicit partitioning columns.
func collectImplicitPartitionColumns(
	columnLookupFn func(tree.Name) (catalog.Column, error),
	indexFirstColumnName string,
	partBy *tree.PartitionBy,
	allowedNewColumnNames []tree.Name,
) (implicitCols []catalog.Column, _ error) {
	seenImplicitColumnNames := map[string]struct{}{}
	// Iterate over each field in the PARTITION BY until it matches the start
	// of the actual explicitly indexed columns.
	for _, field := range partBy.Fields {
		// As soon as the fields match, we have no implicit columns to add.
		if string(field) == indexFirstColumnName {
			break
		}

		col, err := findColumnByNameOnTable(
			columnLookupFn,
			field,
			allowedNewColumnNames,
		)
		if err != nil {
			return nil, err
		}
		if _, ok := seenImplicitColumnNames[col.GetName()]; ok {
			return nil, pgerror.Newf(
				pgcode.InvalidObjectDefinition,
				`found multiple definitions in partition using column "%s"`,
				col.GetName(),
			)
		}
		seenImplicitColumnNames[col.GetName()] = struct{}{}
		implicitCols = append(implicitCols, col)
	}

	return implicitCols, nil
}

// findColumnByNameOnTable finds the given column from the table.
// By default we only allow public columns on PARTITION BY clauses.
// However, any columns appearing as allowedNewColumnNames is also
// permitted provided the caller will ensure this column is backfilled
// before the partitioning is active.
func findColumnByNameOnTable(
	columnLookupFn func(tree.Name) (catalog.Column, error),
	col tree.Name,
	allowedNewColumnNames []tree.Name,
) (catalog.Column, error) {
	ret, err := columnLookupFn(col)
	if err != nil {
		return nil, err
	}
	if ret.Public() {
		return ret, nil
	}
	for _, allowedNewColName := range allowedNewColumnNames {
		if allowedNewColName == col {
			return ret, nil
		}
	}
	return nil, colinfo.NewUndefinedColumnError(string(col))
}

// createPartitioning constructs the partitioning descriptor for an index that
// is partitioned into ranges, each addressable by zone configs.
func createPartitioning(
	ctx context.Context,
	st *cluster.Settings,
	evalCtx *eval.Context,
	columnLookupFn func(tree.Name) (catalog.Column, error),
	oldNumImplicitColumns int,
	oldKeyColumnNames []string,
	partBy *tree.PartitionBy,
	allowedNewColumnNames []tree.Name,
	allowImplicitPartitioning bool,
) (newImplicitCols []catalog.Column, newPartitioning catpb.PartitioningDescriptor, err error) {
	if err := utilccl.CheckEnterpriseEnabled(st, "partitions"); err != nil {
		return nil, newPartitioning, err
	}

	// Truncate existing implicitly partitioned column names.
	newIdxColumnNames := oldKeyColumnNames[oldNumImplicitColumns:]

	if allowImplicitPartitioning {
		newImplicitCols, err = collectImplicitPartitionColumns(
			columnLookupFn,
			newIdxColumnNames[0],
			partBy,
			allowedNewColumnNames,
		)
		if err != nil {
			return nil, newPartitioning, err
		}
	}
	if len(newImplicitCols) > 0 {
		// Prepend with new implicit column names.
		newIdxColumnNames = make([]string, len(newImplicitCols), len(newImplicitCols)+len(newIdxColumnNames))
		for i, col := range newImplicitCols {
			newIdxColumnNames[i] = col.GetName()
		}
		newIdxColumnNames = append(newIdxColumnNames, oldKeyColumnNames[oldNumImplicitColumns:]...)
	}

	// If we had implicit column partitioning beforehand, check we have the
	// same implicitly partitioned columns.
	// Having different implicitly partitioned columns requires rewrites,
	// which is outside the scope of createPartitioning.
	if oldNumImplicitColumns > 0 {
		if len(newImplicitCols) != oldNumImplicitColumns {
			return nil, newPartitioning, errors.AssertionFailedf(
				"mismatching number of implicit columns: old %d vs new %d",
				oldNumImplicitColumns,
				len(newImplicitCols),
			)
		}
		for i, col := range newImplicitCols {
			if oldKeyColumnNames[i] != col.GetName() {
				return nil, newPartitioning, errors.AssertionFailedf("found new implicit partitioning at column ordinal %d", i)
			}
		}
	}

	newPartitioning, err = createPartitioningImpl(
		ctx,
		evalCtx,
		columnLookupFn,
		newIdxColumnNames,
		partBy,
		allowedNewColumnNames,
		len(newImplicitCols),
		0, /* colOffset */
	)
	if err != nil {
		return nil, catpb.PartitioningDescriptor{}, err
	}
	return newImplicitCols, newPartitioning, err
}

func init() {
	sql.CreatePartitioningCCL = createPartitioning
	scdeps.CreatePartitioningCCL = createPartitioning
}
