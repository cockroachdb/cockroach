// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// TODO(annie): This is unused for now.
var _ = createPartitioningImpl

func createPartitioningImpl(
	b BuildCtx,
	tableID catid.DescID,
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

	var cols []*scpb.ColumnName
	for i := 0; i < len(partBy.Fields); i++ {
		if colOffset+i >= len(newIdxColumnNames) {
			return partDesc, pgerror.Newf(pgcode.Syntax,
				"declared partition columns (%s) exceed the number of columns in index being partitioned (%s)",
				partitioningString(), strings.Join(newIdxColumnNames, ", "))
		}
		// Search by name because some callsites of this method have not
		// allocated ids yet (so they are still all the 0 value).
		col, err := findColumnByNameOnTable(
			b,
			tableID,
			tree.Name(newIdxColumnNames[colOffset+i]),
			allowedNewColumnNames,
		)
		if err != nil {
			return partDesc, err
		}
		cols = append(cols, col)
		if string(partBy.Fields[i]) != col.Name {
			// This used to print the first `colOffset + len(partBy.Fields)` fields
			// but there might not be this many columns in the index. See #37682.
			n := colOffset + i + 1
			return partDesc, pgerror.Newf(pgcode.Syntax,
				"declared partition columns (%s) do not match first %d columns in index being partitioned (%s)",
				partitioningString(), n, strings.Join(newIdxColumnNames[:n], ", "))
		}
		colTypFamily := b.QueryByID(tableID).FilterColumnType().
			Filter(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.ColumnType) bool {
				return e.ColumnID == col.ColumnID
			}).MustGetOneElement().Type.Family()
		if colTypFamily == types.ArrayFamily {
			return partDesc, unimplemented.NewWithIssuef(91766, "partitioning by array column (%s) not supported",
				col.Name)
		}
	}

	for _, l := range partBy.List {
		p := catpb.PartitioningDescriptor_List{
			Name: string(l.Name),
		}
		for _, expr := range l.Exprs {
			encodedTuple, err := valueEncodePartitionTuple(
				b, tableID, tree.PartitionByList, expr, cols,
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
				b,
				tableID,
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
			b, tableID, tree.PartitionByRange, &tree.Tuple{Exprs: r.From}, cols,
		)
		if err != nil {
			return partDesc, errors.Wrapf(err, "PARTITION %s", p.Name)
		}
		p.ToExclusive, err = valueEncodePartitionTuple(
			b, tableID, tree.PartitionByRange, &tree.Tuple{Exprs: r.To}, cols,
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

// findColumnByNameOnTable finds the given column from the table.
// By default we only allow public columns on PARTITION BY clauses.
// However, any columns appearing as allowedNewColumnNames is also
// permitted provided the caller will ensure this column is backfilled
// before the partitioning is active.
func findColumnByNameOnTable(
	b BuildCtx, tableID catid.DescID, colName tree.Name, allowedNewColumnNames []tree.Name,
) (*scpb.ColumnName, error) {
	isPublic := false
	col := b.QueryByID(tableID).FilterColumnName().
		Filter(func(curr scpb.Status, _ scpb.TargetStatus, e *scpb.ColumnName) bool {
			isColMatch := e.Name == string(colName)
			if isColMatch {
				isPublic = curr == scpb.Status_PUBLIC
			}
			return isColMatch
		}).MustGetOneElement()
	if isPublic {
		return col, nil
	}
	for _, allowedNewColName := range allowedNewColumnNames {
		if allowedNewColName == colName {
			return col, nil
		}
	}
	return nil, colinfo.NewUndefinedColumnError(string(colName))
}

// valueEncodePartitionTuple typechecks the datums in maybeTuple. It returns the
// concatenation of these datums, each encoded using the table "value" encoding.
// The special values of DEFAULT (for list) and MAXVALUE (for range) are encoded
// as NOT NULL.
//
// TODO(dan): The typechecking here should be run during plan construction, so
// we can support placeholders.
func valueEncodePartitionTuple(
	b BuildCtx,
	tableID catid.DescID,
	typ tree.PartitionByType,
	maybeTuple tree.Expr,
	cols []*scpb.ColumnName,
) ([]byte, error) {
	// Replace any occurrences of the MINVALUE/MAXVALUE pseudo-names
	// into MinVal and MaxVal, to be recognized below.
	// We are operating in a context where the expressions cannot
	// refer to table columns, so these two names are unambiguously
	// referring to the desired partition boundaries.
	maybeTuple, _ = tree.WalkExpr(replaceMinMaxValVisitor{}, maybeTuple)

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
		colType := b.QueryByID(tableID).FilterColumnType().
			Filter(func(current scpb.Status, target scpb.TargetStatus, e *scpb.ColumnType) bool {
				return e.ColumnID == cols[i].ColumnID
			}).MustGetOneElement().Type
		typedExpr, err := schemaexpr.SanitizeVarFreeExpr(b, expr, colType, "partition",
			&semaCtx,
			volatility.Immutable,
			false, /*allowAssignmentCast*/
		)
		if err != nil {
			return nil, err
		}
		if !eval.IsConst(b.EvalCtx(), typedExpr) {
			return nil, pgerror.Newf(pgcode.Syntax,
				"%s: partition values must be constant", typedExpr)
		}
		datum, err := eval.Expr(b, b.EvalCtx(), typedExpr)
		if err != nil {
			return nil, errors.Wrapf(err, "evaluating %s", typedExpr)
		}
		err = colinfo.CheckDatumTypeFitsColumnType(cols[i].Name, colType, datum.ResolvedType())
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

type replaceMinMaxValVisitor = zonepb.ReplaceMinMaxValVisitor
