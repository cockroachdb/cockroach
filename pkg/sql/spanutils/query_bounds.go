// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanutils

import (
	"bytes"
	"context"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// QueryBounds defines a logical span with primary key bounds for SQL filtering.
type QueryBounds struct {
	// Start represent the lower bounds in the SELECT statement. After each
	// SelectQueryBuilder.Run, the start bounds increase to exclude the rows
	// selected in the previous SelectQueryBuilder.Run.
	//
	// For the first SELECT in a span, the start bounds are inclusive because the
	// start bounds are based on the first row >= Span.Key. That row must be
	// included in the first SELECT. For subsequent SELECTS, the start bounds
	// are exclusive to avoid re-selecting the last row from the previous SELECT.
	Start tree.Datums
	// End represents the upper bounds in the SELECT statement. The end bounds
	// never change between each SelectQueryBuilder.Run.
	//
	// For all SELECTS in a span, the end bounds are inclusive even though a
	// span's end key is exclusive because the end bounds are based on the first
	// row < Span.EndKey.
	End tree.Datums
}

var (
	startKeyCompareOps = map[catenumpb.IndexColumn_Direction]string{
		catenumpb.IndexColumn_ASC:  ">",
		catenumpb.IndexColumn_DESC: "<",
	}
	endKeyCompareOps = map[catenumpb.IndexColumn_Direction]string{
		catenumpb.IndexColumn_ASC:  "<",
		catenumpb.IndexColumn_DESC: ">",
	}
)

// SpanToQueryBounds converts a DistSQL span (roachpb.Span) into SQL-layer
// primary key bounds (QueryBounds) that can be used to generate WHERE clauses
// for row-level queries.
//
// This function performs a best-effort mapping of the key span to logical
// row-level start and end conditions by scanning the span to find:
//   - The first row (using a forward scan), which defines the lower bound.
//   - The last row (using a reverse scan), which defines the upper bound.
//
// The KV keys for those rows are decoded into primary key datums using the
// provided codec, column descriptors, and directions. The resulting QueryBounds
// represents a closed interval [Start, End], inclusive on the start and end,
// matching SQL query semantics.
//
// If no rows are found in the span, the function returns hasRows = false.
//
// The caller is expected to use these bounds to construct a SQL WHERE clause that
// limits a query to just the rows in this span.
func SpanToQueryBounds(
	ctx context.Context,
	kvDB *kv.DB,
	codec keys.SQLCodec,
	pkColIDs catalog.TableColMap,
	pkColTypes []*types.T,
	pkColDirs []catenumpb.IndexColumn_Direction,
	numFamilies int,
	span roachpb.Span,
	alloc *tree.DatumAlloc,
) (bounds QueryBounds, hasRows bool, _ error) {
	partialStartKey := span.Key
	partialEndKey := span.EndKey
	startKeyValues, err := kvDB.Scan(ctx, partialStartKey, partialEndKey, int64(numFamilies))
	if err != nil {
		return bounds, false, errors.Wrapf(err, "scan error startKey=%x endKey=%x", []byte(partialStartKey), []byte(partialEndKey))
	}
	// If span has 0 rows then return early - it will not be processed.
	if len(startKeyValues) == 0 {
		return bounds, false, nil
	}
	endKeyValues, err := kvDB.ReverseScan(ctx, partialStartKey, partialEndKey, int64(numFamilies))
	if err != nil {
		return bounds, false, errors.Wrapf(err, "reverse scan error startKey=%x endKey=%x", []byte(partialStartKey), []byte(partialEndKey))
	}
	// If span has 0 rows then return early - it will not be processed. This is
	// checked again here because the calls to Scan and ReverseScan are
	// non-transactional so the row could have been deleted between the calls.
	if len(endKeyValues) == 0 {
		return bounds, false, nil
	}
	bounds.Start, err = rowenc.DecodeIndexKeyToDatums(codec, pkColIDs, pkColTypes, pkColDirs, startKeyValues, alloc)
	if err != nil {
		return bounds, false, errors.Wrapf(err, "decode startKeyValues error on %+v", startKeyValues)
	}
	bounds.End, err = rowenc.DecodeIndexKeyToDatums(codec, pkColIDs, pkColTypes, pkColDirs, endKeyValues, alloc)
	if err != nil {
		return bounds, false, errors.Wrapf(err, "decode endKeyValues error on %+v", endKeyValues)
	}
	return bounds, true, nil
}

// GetPKColumnTypes returns tableDesc's primary key column types.
func GetPKColumnTypes(
	tableDesc catalog.TableDescriptor, indexDesc *descpb.IndexDescriptor,
) ([]*types.T, error) {
	pkColTypes := make([]*types.T, 0, len(indexDesc.KeyColumnIDs))
	for i, id := range indexDesc.KeyColumnIDs {
		col, err := catalog.MustFindColumnByID(tableDesc, id)
		if err != nil {
			return nil, errors.Wrapf(err, "column index=%d", i)
		}
		pkColTypes = append(pkColTypes, col.GetType())
	}
	return pkColTypes, nil
}

// RenderQueryBounds generates SQL predicates over the primary key columns that
// restrict rows to fall within a given QueryBounds span. It returns a string
// fragment suitable for use inside a WHERE clause.
//
// The span is expressed as a series of OR-ed row-level comparisons that implement
// lexicographic filtering, e.g.:
//
//	(pk1, pk2, pk3) >= ($N, $N+1, $N+2)
//	(pk1, pk2, pk3) <=  ($M, $M+1, $M+2)
//
// This logic does not use the actual bound values. Instead, it assumes the values
// will be provided later as SQL placeholders ($N), and constructs the comparison
// expressions accordingly. Column names are SQL-escaped, and each placeholder is
// annotated with a fully qualified type cast (e.g., ::INT8).
//
// The caller is responsible for assigning the placeholder numbering via
// endPlaceholderOffset and for providing the actual parameter values in a
// consistent order (typically: cutoff, end bound datums, start bound datums).
func RenderQueryBounds(
	pkColNames []string,
	pkColDirs []catenumpb.IndexColumn_Direction,
	pkColTypes []*types.T,
	numStartQueryBounds, numEndQueryBounds int,
	startIncl bool,
	endPlaceholderOffset int,
) (string, error) {
	if len(pkColNames) != len(pkColDirs) || len(pkColNames) != len(pkColTypes) {
		return "", errors.AssertionFailedf(
			"inconsistent PK metadata: %d names, %d dirs, %d types",
			len(pkColNames), len(pkColDirs), len(pkColTypes))
	}
	if numStartQueryBounds > len(pkColNames) {
		return "", errors.AssertionFailedf(
			"start bound uses %d columns, but only %d PK columns available",
			numStartQueryBounds, len(pkColNames))
	}
	if numEndQueryBounds > len(pkColNames) {
		return "", errors.AssertionFailedf(
			"end bound uses %d columns, but only %d PK columns available",
			numEndQueryBounds, len(pkColNames))
	}

	var buf bytes.Buffer
	appendPKRangeORChain(
		&buf,
		pkColNames,
		pkColDirs,
		pkColTypes,
		numStartQueryBounds,
		endPlaceholderOffset+numEndQueryBounds,
		startKeyCompareOps,
		startIncl,
		false, // no AND for the first clause
	)
	appendPKRangeORChain(
		&buf,
		pkColNames,
		pkColDirs,
		pkColTypes,
		numEndQueryBounds,
		endPlaceholderOffset,
		endKeyCompareOps,
		true,                    // ending is always inclusive
		numStartQueryBounds > 0, // add AND only if start clause was emitted
	)
	return buf.String(), nil
}

// appendPKRangeORChain appends SQL expressions to the buffer that restrict rows
// using a lexicographic comparison over a prefix of primary key columns.
// It generates a disjunction of ANDed equality and inequality comparisons,
// using parameter placeholders and explicit type casts.
func appendPKRangeORChain(
	buf *bytes.Buffer,
	pkColNames []string,
	pkColDirs []catenumpb.IndexColumn_Direction,
	pkColTypes []*types.T,
	numQueryBounds int,
	placeholderOffset int,
	compareOps map[catenumpb.IndexColumn_Direction]string,
	inclusive bool,
	withAnd bool,
) {
	if numQueryBounds > 0 {
		if withAnd {
			buf.WriteString("\nAND ")
		}
		buf.WriteString("(")
		for i := 0; i < numQueryBounds; i++ {
			isLast := i == numQueryBounds-1
			buf.WriteString("\n  (")
			for j := 0; j < i; j++ {
				buf.WriteString(pkColNames[j])
				buf.WriteString(" = $")
				buf.WriteString(strconv.Itoa(j + placeholderOffset))
				buf.WriteString("::")
				buf.WriteString(pkColTypes[j].SQLStringFullyQualified())
				buf.WriteString(" AND ")
			}
			buf.WriteString(pkColNames[i])
			buf.WriteString(" ")
			buf.WriteString(compareOps[pkColDirs[i]])
			if isLast && inclusive {
				buf.WriteString("=")
			}
			buf.WriteString(" $")
			buf.WriteString(strconv.Itoa(i + placeholderOffset))
			buf.WriteString("::")
			buf.WriteString(pkColTypes[i].SQLStringFullyQualified())
			buf.WriteString(")")
			if !isLast {
				buf.WriteString(" OR")
			}
		}
		buf.WriteString("\n)")
	}
}
